import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from ingestion import bronze
from ingestion.bronze import IngestResult, ingest_csv, main, read_bronze
from ingestion.metadata import BRONZE_META_COLUMNS, hash_file

FIXTURES = Path(__file__).parent.parent / "fixtures"
ORDERS = FIXTURES / "orders_leading_zeros.csv"
RUN_AT = datetime(2026, 11, 11, 16, 33, 0, tzinfo=UTC)


def _parquet_files(lake: Path) -> list[Path]:
    return sorted(lake.rglob("*.parquet"))


def test_writes_metadata_columns_and_uncast_values(tmp_path: Path) -> None:
    result = ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    frame = pl.read_parquet(_parquet_files(tmp_path)[0])
    assert result == IngestResult(ORDERS.name, hash_file(ORDERS), 3, skipped=False)
    assert all(column in frame.columns for column in BRONZE_META_COLUMNS)
    assert frame["id"].to_list() == ["001", "002", "003"]
    assert frame["amount"].to_list() == ["1e3", "007.50", "0"]
    assert frame["code"].to_list() == ["A1", "B2", None]
    assert set(frame["_file_hash"]) == {result.file_hash}


def test_path_and_name_follow_layout(tmp_path: Path) -> None:
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    expected = (
        tmp_path
        / "bronze"
        / "orders"
        / "ingest_date=2026-11-11"
        / f"{hash_file(ORDERS)}20261111T163300Z.parquet"
    )
    assert _parquet_files(tmp_path) == [expected]


def test_rerun_skips_and_leaves_files_unchanged(tmp_path: Path) -> None:
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    before = _parquet_files(tmp_path)

    result = ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT + timedelta(days=1))

    assert result.skipped
    assert _parquet_files(tmp_path) == before
    assert read_bronze("orders", tmp_path).count("*").fetchall() == [(3,)]


def test_force_adds_new_file_and_keeps_old(tmp_path: Path) -> None:
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    first = _parquet_files(tmp_path)

    result = ingest_csv(
        ORDERS, "orders", tmp_path, force=True, run_at=RUN_AT + timedelta(seconds=5)
    )

    files = _parquet_files(tmp_path)
    assert not result.skipped
    assert len(files) == 2
    assert set(first) < set(files)
    assert len({re.search(r"(\d{8}T\d{6}Z)", f.name).group(1) for f in files}) == 2


def test_name_collision_fails_without_overwriting(tmp_path: Path) -> None:
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    target = _parquet_files(tmp_path)[0]
    original = target.read_bytes()

    with pytest.raises(FileExistsError):
        ingest_csv(ORDERS, "orders", tmp_path, force=True, run_at=RUN_AT)

    assert target.read_bytes() == original
    assert len(_parquet_files(tmp_path)) == 1


def test_files_with_different_columns_both_load(tmp_path: Path) -> None:
    other = tmp_path / "landing" / "orders_v2.csv"
    other.parent.mkdir()
    other.write_text("id,region\n9,north\n")
    lake = tmp_path / "lake"

    ingest_csv(ORDERS, "orders", lake, run_at=RUN_AT)
    ingest_csv(other, "orders", lake, run_at=RUN_AT)

    relation = read_bronze("orders", lake)
    assert {"code", "region", "ingest_date"} <= set(relation.columns)
    assert relation.count("*").fetchall() == [(4,)]
    assert relation.filter("id = '9'").project("code, region").fetchall() == [(None, "north")]


def test_empty_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="empty file"):
        ingest_csv(FIXTURES / "empty.csv", "orders", tmp_path, run_at=RUN_AT)
    assert _parquet_files(tmp_path) == []


def test_header_only_file_writes_nothing(tmp_path: Path) -> None:
    result = ingest_csv(FIXTURES / "orders_header_only.csv", "orders", tmp_path, run_at=RUN_AT)

    assert (result.rows, result.skipped) == (0, False)
    assert _parquet_files(tmp_path) == []


def test_file_changing_during_read_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "in.csv"
    source.write_text("a\n1\n")
    real_read = bronze.read_csv_as_strings

    def read_then_modify(path: Path) -> pl.LazyFrame:
        frame = real_read(path)
        path.write_text("a\n2\n")
        return frame

    monkeypatch.setattr(bronze, "read_csv_as_strings", read_then_modify)

    with pytest.raises(RuntimeError, match="changed during ingestion"):
        ingest_csv(source, "orders", tmp_path / "lake", run_at=RUN_AT)
    assert _parquet_files(tmp_path / "lake") == []


@pytest.mark.parametrize("source", ["../x", "a/b", "", ".hidden"])
def test_invalid_source_rejected(tmp_path: Path, source: str) -> None:
    with pytest.raises(ValueError, match="invalid source"):
        ingest_csv(ORDERS, source, tmp_path, run_at=RUN_AT)


def test_logs_contain_no_cell_values(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    text = caplog.text
    assert "rows_ingested=3" in text and "event=bronze_skip" in text
    for value in ("007.50", "B2", "1e3"):
        assert value not in text


def test_cli_ingests_landing_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    landing = tmp_path / "data" / "landing" / "orders"
    landing.mkdir(parents=True)
    (landing / "orders.csv").write_bytes(ORDERS.read_bytes())
    monkeypatch.chdir(tmp_path)

    assert main(["orders"]) == 0

    assert len(_parquet_files(tmp_path / "lake")) == 1
    assert main(["orders"]) == 0
    assert len(_parquet_files(tmp_path / "lake")) == 1
