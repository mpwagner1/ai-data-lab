"""Every reader and `read_bronze` use the path they are given. All data is synthetic."""

import io
import os
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from test_azure_stage import FakeContainer

from ingestion import azure_stage, bronze
from ingestion.detect import FileFormat
from ingestion.metadata import hash_file
from ingestion.source_config import SourceConfig

Build = Callable[[str], bytes]

_NAMES = ["d[1]", "d?", "*"]
_SIBLING = "d1"


def _parquet_bytes(value: str) -> bytes:
    buffer = io.BytesIO()
    pl.DataFrame({"id": [value]}).write_parquet(buffer)
    return buffer.getvalue()


_FORMATS: dict[str, tuple[str, FileFormat, str, Build]] = {
    "csv": (".csv", FileFormat.DELIMITED, "utf-8", lambda v: f"id\n{v}\n".encode()),
    "csv-cp1252": (".csv", FileFormat.DELIMITED, "cp1252", lambda v: f"id\n{v}\n".encode()),
    "tsv": (".tsv", FileFormat.DELIMITED, "utf-8", lambda v: f"id\n{v}\n".encode()),
    "psv": (".psv", FileFormat.DELIMITED, "utf-8", lambda v: f"id\n{v}\n".encode()),
    "json": (".json", FileFormat.JSON, "utf-8", lambda v: f'[{{"id": "{v}"}}]'.encode()),
    "ndjson": (".ndjson", FileFormat.NDJSON, "utf-8", lambda v: f'{{"id": "{v}"}}\n'.encode()),
    "parquet": (".parquet", FileFormat.PARQUET, "utf-8", _parquet_bytes),
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def home(project: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A temporary HOME, plus a local folder that is really named `~`."""
    folder = project / "home"
    folder.mkdir()
    (project / "~").mkdir()
    monkeypatch.setenv("HOME", str(folder))
    return folder


def _bronze_rows(lake: Path, file_name: str) -> list[tuple[str, str]]:
    return [
        (row["id"], row["_file_hash"])
        for path in (lake / "bronze").rglob("*.parquet")
        for row in pl.read_parquet(path, glob=False).to_dicts()
        if row["_file_name"] == file_name
    ]


def _read_ids(path: Path, file_format: FileFormat, config: SourceConfig) -> list[str]:
    [part] = bronze.READERS[file_format](path, config, None)
    return part.frame.collect()["id"].to_list()


def _assert_loads_own_rows(project: Path, name: str, sibling: str, build: Build) -> None:
    folder = project / "data" / "landing" / "demo"
    (folder / name).write_bytes(build("own"))
    (folder / sibling).write_bytes(build("sibling"))

    assert bronze.main(["demo"]) == 0

    assert _bronze_rows(project / "lake", name) == [("own", hash_file(folder / name))]
    assert _bronze_rows(project / "lake", sibling) == [("sibling", hash_file(folder / sibling))]


@pytest.mark.parametrize("stem", _NAMES)
@pytest.mark.parametrize("kind", _FORMATS)
def test_a_landing_file_loads_its_own_rows(project: Path, kind: str, stem: str) -> None:
    extension, _, encoding, build = _FORMATS[kind]
    config = project / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text(f"encoding: {encoding}\n")

    _assert_loads_own_rows(project, stem + extension, _SIBLING + extension, build)


@pytest.mark.parametrize("stem", _NAMES)
def test_a_landing_workbook_loads_its_own_rows(
    project: Path, ods_bytes: Callable[..., bytes], stem: str
) -> None:
    def build(value: str) -> bytes:
        return ods_bytes([["id"], [value]])

    _assert_loads_own_rows(project, f"{stem}.ods", f"{_SIBLING}.ods", build)


@pytest.mark.parametrize("kind", _FORMATS)
def test_a_reader_takes_a_leading_tilde_as_written(project: Path, home: Path, kind: str) -> None:
    extension, file_format, encoding, build = _FORMATS[kind]
    (project / "~" / f"a{extension}").write_bytes(build("own"))
    (home / f"a{extension}").write_bytes(build("sibling"))

    ids = _read_ids(Path("~") / f"a{extension}", file_format, SourceConfig(encoding=encoding))

    assert ids == ["own"]


def test_the_workbook_reader_takes_a_leading_tilde_as_written(
    project: Path, home: Path, ods_bytes: Callable[..., bytes]
) -> None:
    (project / "~" / "a.ods").write_bytes(ods_bytes([["id"], ["own"]]))
    (home / "a.ods").write_bytes(ods_bytes([["id"], ["sibling"]]))

    assert _read_ids(Path("~/a.ods"), FileFormat.ODS, SourceConfig()) == ["own"]


def _load_into(lake: Path, source_file: Path, value: str) -> bronze.IngestResult:
    source_file.write_bytes(f"id\n{value}\n".encode())
    return bronze.ingest_file(source_file, "demo", lake)


def _ids(lake: Path) -> list[str]:
    return [row[0] for row in bronze.read_bronze("demo", lake).project("id").fetchall()]


@pytest.mark.parametrize("root", ["work [1]", "work?", "work*", "work{1,2}", "**"])
def test_a_lake_root_is_used_as_written(project: Path, root: str) -> None:
    for sibling in ("work 1", "work1", "work2"):
        _load_into(project / sibling / "lake", project / "other.csv", f"sibling {sibling}")
    lake = project / root / "lake"
    _load_into(lake, project / "own.csv", "own")

    assert bronze.ingest_file(project / "own.csv", "demo", lake).skipped
    assert _ids(lake) == ["own"]


def test_a_relative_tilde_lake_root_is_used_as_written(project: Path, home: Path) -> None:
    _load_into(home / "lake", project / "other.csv", "sibling")
    lake = Path("~/lake")
    _load_into(lake, project / "own.csv", "own")

    assert _ids(lake) == ["own"]
    assert len(list((project / "~" / "lake").rglob("*.parquet"))) == 1
    assert len(list((home / "lake").rglob("*.parquet"))) == 1


@pytest.mark.skipif(os.sep == "\\", reason="a backslash is the path separator here")
def test_read_bronze_rejects_a_lake_root_with_a_backslash(project: Path) -> None:
    _load_into(project / "a" / "b" / "lake", project / "other.csv", "sibling")
    lake = project / "a\\b" / "lake"
    _load_into(lake, project / "own.csv", "own")

    with pytest.raises(ValueError, match="backslash"):
        bronze.read_bronze("demo", lake)


@pytest.mark.parametrize("stem", _NAMES)
def test_staging_writes_each_blob_under_its_own_name(tmp_path: Path, stem: str) -> None:
    folder = tmp_path / "orders"
    folder.mkdir()
    (folder / f"{_SIBLING}.csv").write_bytes(b"id\nsibling\n")
    container = FakeContainer({f"orders/{stem}.csv": b"id\nown\n"})

    summary = azure_stage.stage(container, "orders", "orders/", tmp_path)

    assert summary.staged == 1
    assert (folder / f"{stem}.csv").read_bytes() == b"id\nown\n"
    assert (folder / f"{_SIBLING}.csv").read_bytes() == b"id\nsibling\n"
