import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from ingestion import bronze, raw_zone
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
        / f"{hash_file(ORDERS)}20261111T163300Z_"
    )
    [actual] = _parquet_files(tmp_path)
    assert actual.parent == expected.parent
    assert re.fullmatch(re.escape(expected.name) + r"[0-9a-f]{8}\.parquet", actual.name)


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


def test_name_collision_fails_without_overwriting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bronze.secrets, "token_hex", lambda n: "ab" * n)
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


HEADER_ONLY = FIXTURES / "orders_header_only.csv"


def test_header_only_file_writes_zero_row_parquet(tmp_path: Path) -> None:
    result = ingest_csv(HEADER_ONLY, "orders", tmp_path, run_at=RUN_AT)

    assert (result.rows, result.skipped) == (0, False)
    [file] = _parquet_files(tmp_path)
    assert file.name.startswith(result.file_hash)
    frame = pl.read_parquet(file)
    assert frame.height == 0
    assert set(BRONZE_META_COLUMNS) <= set(frame.columns)
    assert frame.schema["_file_hash"] == pl.String


def test_header_only_rerun_is_skipped_without_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingest_csv(HEADER_ONLY, "orders", tmp_path, run_at=RUN_AT)
    before = _parquet_files(tmp_path)

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("data was read on a skipped file")

    monkeypatch.setattr(bronze, "_load_with_metadata", fail)
    result = ingest_csv(HEADER_ONLY, "orders", tmp_path, run_at=RUN_AT + timedelta(seconds=5))

    assert (result.rows, result.skipped) == (0, True)
    assert _parquet_files(tmp_path) == before


def test_header_only_force_adds_another_zero_row_file(tmp_path: Path) -> None:
    ingest_csv(HEADER_ONLY, "orders", tmp_path, run_at=RUN_AT)
    first = _parquet_files(tmp_path)
    result = ingest_csv(
        HEADER_ONLY, "orders", tmp_path, force=True, run_at=RUN_AT + timedelta(seconds=5)
    )

    assert (result.rows, result.skipped) == (0, False)
    files = _parquet_files(tmp_path)
    assert len(files) == 2
    assert set(first) <= set(files)


def test_read_bronze_mixes_header_only_and_data_files(tmp_path: Path) -> None:
    header_columns = pl.read_csv(HEADER_ONLY, infer_schema=False).columns
    ingest_csv(HEADER_ONLY, "orders", tmp_path, run_at=RUN_AT)
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    relation = read_bronze("orders", tmp_path)
    assert relation.count("*").fetchall() == [(3,)]
    assert set(header_columns) <= set(relation.columns)
    assert all(
        str(t) == "VARCHAR"
        for name, t in zip(relation.columns, relation.types, strict=True)
        if name not in {"ingest_date", "_ingested_at", "_row_number"}
    )


def test_file_changing_during_read_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "in.csv"
    source.write_text("a\n1\n")
    real_read = bronze.read_csv_as_strings

    def read_then_modify(path: Path, *args: object, **kwargs: object) -> pl.LazyFrame:
        frame = real_read(path, *args, **kwargs)
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


def test_same_second_force_runs_both_succeed(tmp_path: Path) -> None:
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    ingest_csv(ORDERS, "orders", tmp_path, force=True, run_at=RUN_AT)

    assert len(_parquet_files(tmp_path)) == 2


def test_leftover_temp_file_is_ignored_by_skip_check_and_reader(tmp_path: Path) -> None:
    file_hash = hash_file(ORDERS)
    partition = tmp_path / "bronze" / "orders" / "ingest_date=2026-11-11"
    partition.mkdir(parents=True)
    (partition / f"{file_hash}20261111T163300Z_deadbeef.parquet.tmp").write_bytes(b"PAR1 junk")

    result = ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    assert not result.skipped
    assert read_bronze("orders", tmp_path).count("*").fetchall() == [(3,)]


def test_failed_write_removes_temp_and_publishes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(self: pl.DataFrame, file: Path) -> None:
        Path(file).write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_write)

    with pytest.raises(OSError, match="disk full"):
        ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    assert [p for p in tmp_path.rglob("*") if p.is_file()] == []


def test_log_values_are_quoted(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    odd = tmp_path / "a b\nc.csv"
    odd.write_bytes(ORDERS.read_bytes())

    ingest_csv(odd, "orders", tmp_path / "lake", run_at=RUN_AT)

    assert 'file_name="a b\\nc.csv"' in caplog.text
    assert len(caplog.text.strip().splitlines()) == 1
    assert re.search(r"duration_s=\d+\.?\d*(\s|$)", caplog.text)
    assert 'duration_s="' not in caplog.text


def test_cli_continues_after_a_failed_file_and_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    landing = tmp_path / "data" / "landing" / "orders"
    landing.mkdir(parents=True)
    (landing / "a_empty.csv").write_bytes(b"")
    (landing / "b_ok.csv").write_bytes(ORDERS.read_bytes())
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)

    assert main(["orders"]) == 1

    assert len(_parquet_files(tmp_path / "lake")) == 1
    assert 'error="ValueError"' in caplog.text
    assert "empty file" not in caplog.text


def test_cli_missing_landing_folder_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.chdir(tmp_path)

    assert main(["typo"]) == 2
    assert "event=bronze_error" in caplog.text
    assert 'error=FileNotFoundError source="typo"' in caplog.text
    assert "landing" not in caplog.text  # the path is not logged


def test_cli_invalid_source_logs_error_and_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.chdir(tmp_path)

    assert main(["../x"]) == 2

    assert 'event=bronze_error error=ValueError source="../x"' in caplog.text
    assert "Traceback" not in caplog.text


def test_cli_warns_about_stale_temp_files_and_keeps_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    landing = tmp_path / "data" / "landing" / "orders"
    landing.mkdir(parents=True)
    (landing / "orders.csv").write_bytes(ORDERS.read_bytes())
    partition = tmp_path / "lake" / "bronze" / "orders" / "ingest_date=2026-11-10"
    partition.mkdir(parents=True)
    stale = partition / "abc20261110T000000Z_deadbeef.parquet.tmp"
    stale.write_bytes(b"junk")
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.INFO)

    assert main(["orders"]) == 0

    assert "event=bronze_stale_temp" in caplog.text
    assert "count=1" in caplog.text
    assert stale.name in caplog.text
    assert stale.exists()


def test_no_stale_temp_warning_when_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "data" / "landing" / "orders").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    assert main(["orders"]) == 0
    assert "bronze_stale_temp" not in caplog.text


def test_link_collision_removes_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_link(src: Path, dst: Path) -> None:
        raise FileExistsError(dst)

    monkeypatch.setattr(raw_zone.os, "link", fail_link)

    with pytest.raises(FileExistsError):
        ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)

    assert [p for p in tmp_path.rglob("*") if p.is_file()] == []


def test_write_and_skip_logs_have_numeric_duration(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    ingest_csv(ORDERS, "orders", tmp_path, run_at=RUN_AT)
    ingest_csv(FIXTURES / "orders_header_only.csv", "orders", tmp_path, run_at=RUN_AT)

    lines = [m for m in caplog.messages if m.startswith("event=bronze_")]
    assert len(lines) == 3
    for line in lines:
        assert re.search(r" duration_s=\d+(\.\d+)?$", line)


def test_cli_no_csvs_warns_and_exits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "data" / "landing" / "orders").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)

    assert main(["orders"]) == 0
    assert "bronze_no_files" in caplog.text


def test_cli_force_writes_new_file_and_keeps_old(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "data" / "landing" / "orders"
    landing.mkdir(parents=True)
    (landing / "orders.csv").write_bytes(ORDERS.read_bytes())
    monkeypatch.chdir(tmp_path)
    assert main(["orders"]) == 0
    first = _parquet_files(tmp_path / "lake")

    assert main(["orders", "--force"]) == 0

    files = _parquet_files(tmp_path / "lake")
    assert len(files) == 2
    assert set(first) < set(files)
    assert read_bronze("orders", tmp_path / "lake").count("*").fetchall() == [(6,)]


def test_file_changing_before_read_fails_rehash_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "in.csv"
    source.write_text("a\n1\n")
    real_read = bronze.read_csv_as_strings

    def modify_then_read(path: Path, *args: object, **kwargs: object) -> pl.LazyFrame:
        path.write_text("a\n2\n")  # changes after the first hash, before the load
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(bronze, "read_csv_as_strings", modify_then_read)

    with pytest.raises(RuntimeError, match="changed during ingestion"):
        ingest_csv(source, "orders", tmp_path / "lake", run_at=RUN_AT)
    assert _parquet_files(tmp_path / "lake") == []


def _ingested(name: str, lake: Path) -> pl.DataFrame:
    ingest_csv(FIXTURES / name, "orders", lake, run_at=RUN_AT)
    return pl.read_parquet(_parquet_files(lake)[0])


def test_bom_is_not_part_of_first_column_name(tmp_path: Path) -> None:
    frame = _ingested("orders_bom.csv", tmp_path)

    assert frame.columns[:2] == ["id", "name"]
    assert frame["id"].to_list() == ["1", "2"]


def test_quoted_fields_keep_embedded_newlines_commas_and_quotes(tmp_path: Path) -> None:
    frame = _ingested("orders_quoted_newlines.csv", tmp_path)

    assert frame.height == 2
    assert frame["note"].to_list() == ["line one\nline two", 'has, comma and "quotes"']
    assert frame["_row_number"].to_list() == [1, 2]


def test_duplicate_rows_are_kept_as_is(tmp_path: Path) -> None:
    frame = _ingested("orders_duplicate_rows.csv", tmp_path)

    assert frame.select("id", "name").rows() == [("1", "alice"), ("1", "alice"), ("2", "bob")]
    assert frame["_row_number"].to_list() == [1, 2, 3]


def test_unquoted_empty_is_null_quoted_empty_is_empty_string_and_spaces_kept(
    tmp_path: Path,
) -> None:
    frame = _ingested("orders_empty_values.csv", tmp_path)

    assert frame["name"].to_list() == [None, "", " "]
    assert frame["note"].to_list() == ["x", None, "z"]
    assert frame.schema["name"] == pl.String
