import hashlib
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from ingestion import metadata
from ingestion.csv_reader import read_csv_as_strings
from ingestion.metadata import BRONZE_META_COLUMNS, add_bronze_metadata, hash_file

FIXTURES = Path(__file__).parent.parent / "fixtures"
INGESTED_AT = datetime(2026, 11, 11, 16, 33, tzinfo=UTC)


def _load() -> pl.DataFrame:
    source = FIXTURES / "orders_leading_zeros.csv"
    lazy = add_bronze_metadata(read_csv_as_strings(source), source.name, "abc", INGESTED_AT)
    return lazy.collect()


def test_hash_matches_hashlib() -> None:
    source = FIXTURES / "orders_leading_zeros.csv"
    assert hash_file(source) == hashlib.sha256(source.read_bytes()).hexdigest()


def test_hash_reads_in_multiple_chunks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metadata, "_HASH_CHUNK_BYTES", 4)
    source = tmp_path / "multi.csv"
    source.write_bytes(b"a\n" + b"xyz\n" * 10)
    assert hash_file(source) == hashlib.sha256(source.read_bytes()).hexdigest()


def test_hash_of_empty_file_is_sha256_of_empty_bytes(tmp_path: Path) -> None:
    source = tmp_path / "empty.csv"
    source.write_bytes(b"")
    assert hash_file(source) == hashlib.sha256(b"").hexdigest()


def test_adds_five_metadata_columns_after_source_columns() -> None:
    assert tuple(_load().columns[-5:]) == BRONZE_META_COLUMNS


def test_row_number_is_one_based_and_contiguous() -> None:
    assert _load()["_row_number"].to_list() == [1, 2, 3]


def test_ingested_at_is_utc_and_identical_within_one_load() -> None:
    column = _load()["_ingested_at"]
    assert column.dtype == pl.Datetime("us", "UTC")
    assert column.unique().to_list() == [INGESTED_AT]


def test_file_name_and_hash_set_on_every_row() -> None:
    frame = _load()
    assert frame["_file_name"].unique().to_list() == ["orders_leading_zeros.csv"]
    assert frame["_file_hash"].unique().to_list() == ["abc"]


def test_header_only_file_has_zero_rows_with_metadata_columns() -> None:
    source = FIXTURES / "orders_header_only.csv"
    frame = add_bronze_metadata(read_csv_as_strings(source), source.name, "h", INGESTED_AT)
    result = frame.collect()
    assert result.height == 0
    assert tuple(result.columns[-5:]) == BRONZE_META_COLUMNS


@pytest.mark.parametrize(
    "bad_time",
    [datetime(2026, 11, 11), datetime(2026, 11, 11, tzinfo=timezone(timedelta(hours=2)))],
)
def test_rejects_non_utc_ingested_at(bad_time: datetime) -> None:
    lazy = pl.LazyFrame({"a": ["1"]})
    with pytest.raises(ValueError, match="UTC"):
        add_bronze_metadata(lazy, "f.csv", "h", bad_time)
