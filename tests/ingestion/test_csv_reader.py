from pathlib import Path

import polars as pl
import pytest

from ingestion.csv_reader import read_csv_as_strings

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_all_columns_are_strings() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert set(frame.dtypes) == {pl.String}


def test_leading_zeros_and_scientific_notation_survive() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert frame["id"].to_list() == ["001", "002", "003"]
    assert frame["amount"].to_list() == ["1e3", "007.50", "0"]


def test_empty_field_reads_as_null() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert frame["code"].to_list() == ["A1", "B2", None]


def test_empty_file_raises() -> None:
    with pytest.raises(ValueError, match="empty file"):
        read_csv_as_strings(FIXTURES / "empty.csv")


def test_whitespace_only_file_raises_value_error() -> None:
    with pytest.raises(ValueError, match="empty file"):
        read_csv_as_strings(FIXTURES / "blank_lines_only.csv")


def test_header_only_file_gives_zero_rows() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_header_only.csv").collect()
    assert frame.height == 0
    assert frame.columns == ["id", "code", "amount"]


def test_underscore_prefixed_column_is_rejected_by_name() -> None:
    with pytest.raises(ValueError, match="_secret"):
        read_csv_as_strings(FIXTURES / "orders_underscore_column.csv")
