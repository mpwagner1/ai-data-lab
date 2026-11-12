from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion.bronze import ingest_file, main
from ingestion.source_config import SourceConfig, load_source_config, parse_source_config

RUN_AT = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)


def _write(path: Path, sep: str, encoding: str = "utf-8") -> Path:
    text = f"id{sep}code\n001{sep}A1\n002{sep}B2\n"
    path.write_bytes(text.encode(encoding))
    return path


def _bronze(lake: Path, source: str = "demo") -> pl.DataFrame:
    return pl.read_parquet(sorted(lake.rglob("*.parquet")))


def _ids(lake: Path, source: str = "demo") -> list[str]:
    frame = _bronze(lake, source)
    assert {frame.schema[c] for c in ("id", "code")} == {pl.String}
    return sorted(frame["id"].to_list())


@pytest.mark.parametrize(("name", "sep"), [("a.psv", "|"), ("a.tsv", "\t")])
def test_extension_default_delimiters(tmp_path: Path, name: str, sep: str) -> None:
    ingest_file(_write(tmp_path / name, sep), "demo", tmp_path / "lake", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_with_config_delimiter(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    ingest_file(path, "demo", tmp_path / "lake", config=SourceConfig(delimiter="|"), run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_with_flag_delimiter(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    ingest_file(path, "demo", tmp_path / "lake", delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_without_delimiter_fails(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    with pytest.raises(ValueError, match="no delimiter"):
        ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)


def test_flag_beats_config_for_txt(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    config = SourceConfig(delimiter=";")
    ingest_file(path, "demo", tmp_path / "lake", config=config, delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_flag_does_not_change_csv(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.csv", ",")
    ingest_file(path, "demo", tmp_path / "lake", delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_cp1252_file_keeps_accents(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes("id,name\n001,caf\xe9\n".encode("cp1252"))
    ingest_file(
        path, "demo", tmp_path / "lake", config=SourceConfig(encoding="cp1252"), run_at=RUN_AT
    )
    assert _bronze(tmp_path / "lake")["name"].to_list() == ["café"]


def test_unknown_config_key_raises(tmp_path: Path) -> None:
    (tmp_path / "demo.yaml").write_text("delimeter: '|'\n")
    with pytest.raises(ValueError, match="unknown source config keys"):
        load_source_config("demo", tmp_path)


def test_config_loads_and_missing_file_is_default(tmp_path: Path) -> None:
    assert load_source_config("demo", tmp_path) == SourceConfig()
    (tmp_path / "demo.yaml").write_text("delimiter: '|'\nencoding: cp1252\nsheets: [A]\n")
    assert load_source_config("demo", tmp_path) == SourceConfig("|", "cp1252", None, ("A",))


@pytest.mark.parametrize("raw", [{"delimiter": "||"}, {"encoding": "nope"}, ["x"]])
def test_bad_config_values_raise(raw: object) -> None:
    with pytest.raises(ValueError):
        parse_source_config(raw)


@pytest.mark.parametrize("flag", ["||", ""])
def test_multi_character_flag_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["demo", "--delimiter", flag]) == 2


def test_cli_loads_txt_with_flag_and_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "data" / "landing" / "demo"
    landing.mkdir(parents=True)
    _write(landing / "a.txt", "|")
    _write(landing / "b.psv", "|").write_text("id|code\n003|C3\n")
    monkeypatch.chdir(tmp_path)

    assert main(["demo", "--delimiter", "|"]) == 0

    assert len(list((tmp_path / "lake").rglob("*.parquet"))) == 2
