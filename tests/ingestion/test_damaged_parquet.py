"""A damaged Parquet input fails only itself. Synthetic data only."""

import io
import logging
import zipfile
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest

from damaged_parquet import panicking_parquet
from ingestion import bronze, parquet_reader
from ingestion.parquet_reader import ParquetReadError, read_parquet_as_strings
from ingestion.source_config import SourceConfig

GOOD_CSV = b"id\n3\n"
CONFIG = SourceConfig()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / bronze.LANDING_ROOT / "demo").mkdir(parents=True)
    return tmp_path


def _loose_files(project: Path) -> None:
    folder = project / bronze.LANDING_ROOT / "demo"
    (folder / "a.parquet").write_bytes(panicking_parquet())
    (folder / "b.csv").write_bytes(GOOD_CSV)


def _archive(project: Path) -> None:
    with zipfile.ZipFile(project / bronze.LANDING_ROOT / "demo" / "a.zip", "w") as archive:
        archive.writestr("a.parquet", panicking_parquet())
        archive.writestr("b.csv", GOOD_CSV)


def _bronze_files(project: Path) -> list[Path]:
    return sorted((project / bronze.LAKE_ROOT / "bronze" / "demo").glob("ingest_date=*/*"))


def _lines(caplog: pytest.LogCaptureFixture, event: str) -> list[str]:
    return [m for m in caplog.messages if m.startswith(f"event={event} ")]


def test_the_damaged_fixture_panics_a_full_read(tmp_path: Path) -> None:
    # Guards the tests below: if a Polars upgrade changes the layout, find a new offset.
    with pytest.raises(pl.exceptions.PanicException):
        pl.read_parquet(io.BytesIO(panicking_parquet()))


def test_reader_raises_its_own_error_without_context(tmp_path: Path) -> None:
    path = tmp_path / "a.parquet"
    path.write_bytes(panicking_parquet())

    with pytest.raises(ParquetReadError) as info:
        read_parquet_as_strings(path, CONFIG, None)

    assert str(info.value) == "cannot read Parquet a.parquet: PanicException"
    assert info.value.__context__ is None
    assert info.value.__cause__ is None


def test_conversion_step_is_covered_too(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "a.parquet"
    pl.DataFrame({"id": ["1"]}).write_parquet(path)

    def fail(series: pl.Series) -> pl.Series:
        raise pl.exceptions.PanicException("synthetic")

    monkeypatch.setattr(parquet_reader, "_as_string", fail)

    with pytest.raises(ParquetReadError, match="PanicException"):
        read_parquet_as_strings(path, CONFIG, None)


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_reader_lets_interrupts_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupt: type[BaseException]
) -> None:
    def stop(source: object) -> None:
        raise interrupt

    monkeypatch.setattr(parquet_reader.pl, "read_parquet", stop)

    with pytest.raises(interrupt):
        read_parquet_as_strings(tmp_path / "a.parquet", CONFIG, None)


def test_run_fails_that_file_and_loads_the_rest(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _loose_files(project)
    caplog.set_level(logging.INFO)

    assert bronze.main(["demo"]) == 1

    assert len(_bronze_files(project)) == 1
    assert _lines(caplog, "bronze_error") == [
        'event=bronze_error source="demo" file_name="a.parquet" error="ParquetReadError"'
    ]
    assert "written=1" in caplog.text
    assert "failed=1" in caplog.text


def test_run_fails_that_member_and_loads_the_rest(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _archive(project)
    caplog.set_level(logging.INFO)

    assert bronze.main(["demo"]) == 1

    assert len(_bronze_files(project)) == 1
    assert _lines(caplog, "bronze_error") == [
        'event=bronze_error source="demo" file_name="a.zip!#1" error="ParquetReadError"'
    ]
    assert "written=1" in caplog.text
    assert "failed=1" in caplog.text


@pytest.mark.parametrize("setup", [_loose_files, _archive], ids=["files", "archive"])
def test_pending_reports_it_and_lists_the_rest(
    project: Path, caplog: pytest.LogCaptureFixture, setup: Callable[[Path], None]
) -> None:
    setup(project)
    caplog.set_level(logging.INFO)

    assert bronze.main(["demo", "--pending"]) == bronze.EXIT_HELD

    failed, ready = _lines(caplog, "bronze_pending")
    assert 'reason="failed" error="ParquetReadError"' in failed
    assert 'reason="ready"' in ready
    assert _bronze_files(project) == []
