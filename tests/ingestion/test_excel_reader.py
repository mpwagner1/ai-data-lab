import logging
import traceback
import zipfile
from collections.abc import Callable
from datetime import UTC, date, datetime, time
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from ingestion.bronze import ingest_file, main, read_bronze
from ingestion.excel_reader import ExcelReadError, read_workbook
from ingestion.source_config import SourceConfig, parse_source_config

RUN_AT = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)
SECRET = "SECRETCELL"
SHEET_SECRET = "SecretSheetName"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _landing(project: Path) -> Path:
    return project / "data" / "landing" / "demo"


def _workbook(path: Path, sheets: dict[str, list[tuple[int, int, object]]]) -> Path:
    """Write a workbook from {sheet: [(row, column, value)]}; rows and columns are 1-based."""
    book = openpyxl.Workbook()
    book.remove(book.active)
    for name, cells in sheets.items():
        sheet = book.create_sheet(name)
        for row, column, value in cells:
            sheet.cell(row, column, value)
    book.save(path)
    return path


def _table(rows: list[tuple[str, ...]], first_row: int = 1) -> list[tuple[int, int, object]]:
    return [(first_row + i, j, v) for i, row in enumerate(rows) for j, v in enumerate(row, 1)]


def _bronze(lake: Path) -> pl.DataFrame:
    files = sorted(lake.rglob("*.parquet"))
    assert len(files) == 1
    return pl.read_parquet(files[0])


def _ingest(path: Path, tmp_path: Path, **config: object) -> pl.DataFrame:
    config_obj = parse_source_config(config)
    ingest_file(path, "demo", tmp_path / "lake", config=config_obj, run_at=RUN_AT)
    return _bronze(tmp_path / "lake")


def _two_sheets(path: Path) -> Path:
    return _workbook(
        path,
        {
            "Jan": _table([("id", "name"), ("a", "x"), ("b", "y")]),
            "Feb": _table([("id", "extra"), ("c", "z")]),
        },
    )


def test_two_sheets_give_one_parquet_with_sheet_names_and_row_numbers(tmp_path: Path) -> None:
    frame = _ingest(_two_sheets(tmp_path / "w.xlsx"), tmp_path)
    assert frame.columns == [
        "id",
        "name",
        "extra",
        "_file_name",
        "_ingested_at",
        "_file_hash",
        "_row_number",
        "_sheet_name",
    ]
    rows = frame.select("_sheet_name", "id", "_row_number").rows()
    assert rows == [("Jan", "a", 2), ("Jan", "b", 3), ("Feb", "c", 2)]
    assert frame["name"].to_list() == ["x", "y", None]
    assert set(frame.schema.values()) - {pl.Datetime("us", "UTC"), pl.Int64} == {pl.String}


def test_sheets_filter(tmp_path: Path) -> None:
    frame = _ingest(_two_sheets(tmp_path / "w.xlsx"), tmp_path, sheets=["Feb"])
    assert frame["_sheet_name"].unique().to_list() == ["Feb"]


def test_unknown_sheet_in_filter_fails_without_echoing_it(tmp_path: Path) -> None:
    path = _two_sheets(tmp_path / "w.xlsx")
    with pytest.raises(ExcelReadError) as error:
        read_workbook(path, SourceConfig(sheets=("Nope",)), None)
    assert "Nope" not in str(error.value)
    assert "w.xlsx" in str(error.value)


def test_header_row_skips_blank_rows_and_numbers_by_sheet_row(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    cells = _table([("id", "name"), ("001", SECRET), ("002", "b")], first_row=6)
    path = _workbook(tmp_path / "w.xlsx", {"S": cells})
    caplog.set_level(logging.INFO)
    frame = _ingest(path, tmp_path, header_row=6)
    assert frame.columns[:2] == ["id", "name"]
    assert frame["_row_number"].to_list() == [7, 8]
    assert frame["id"].to_list() == ["001", "002"]
    assert "header_row=6 skipped_rows=5 rows=2" in caplog.text
    assert SECRET not in caplog.text


def test_without_header_row_the_headers_are_unnamed(tmp_path: Path) -> None:
    cells = _table([("id", "name"), ("1", "a")], first_row=6)
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame.columns[:2] == ["unnamed_1", "unnamed_2"]
    assert frame["_row_number"].to_list() == [2, 3, 4, 5, 6, 7]  # five blank rows, header, data
    assert frame["unnamed_1"].to_list()[-2:] == ["id", "1"]


def test_blank_header_becomes_unnamed_by_position(tmp_path: Path) -> None:
    cells = [(1, 1, "id"), (1, 3, "n"), (2, 1, "a"), (2, 2, "b"), (2, 3, "c")]
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame.columns[:3] == ["id", "unnamed_2", "n"]


def test_blank_rows_inside_data_keep_their_row_numbers(tmp_path: Path) -> None:
    cells = [(1, 1, "id"), (2, 1, "a"), (5, 1, "b")]  # rows 3 and 4 are blank
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame["_row_number"].to_list() == [2, 3, 4, 5]
    assert frame["id"].to_list() == ["a", None, None, "b"]


def test_header_row_past_end_of_sheet_fails_the_file(tmp_path: Path) -> None:
    path = _workbook(tmp_path / "w.xlsx", {"S": _table([("id",), ("a",)])})
    with pytest.raises(ExcelReadError, match="header_row"):
        ingest_file(
            path, "demo", tmp_path / "lake", config=SourceConfig(header_row=9), run_at=RUN_AT
        )
    assert not list((tmp_path / "lake").rglob("*.parquet"))


def test_numbers_stay_as_the_reader_yields_them(tmp_path: Path) -> None:
    cells = [(1, 1, "n"), (2, 1, 1), (3, 1, 2.5), (4, 1, "001")]
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame["n"].to_list() == ["1", "2.5", "001"]


def test_reserved_and_duplicate_headers_fail_without_echoing_them(tmp_path: Path) -> None:
    reserved = _workbook(tmp_path / "r.xlsx", {"S": [(1, 1, "_secret"), (2, 1, "x")]})
    duplicate = _workbook(tmp_path / "d.xlsx", {"S": [(1, 1, "a"), (1, 2, "a"), (2, 1, "x")]})
    for path in (reserved, duplicate):
        with pytest.raises(ExcelReadError) as error:
            read_workbook(path, SourceConfig(), None)
        assert "_secret" not in str(error.value)


def test_empty_sheet_is_skipped_but_all_empty_fails(tmp_path: Path) -> None:
    mixed = _workbook(tmp_path / "m.xlsx", {"Empty": [], "S": _table([("id",), ("a",)])})
    assert [p.sheet_name for p in read_workbook(mixed, SourceConfig(), None)] == ["S"]
    blank = _workbook(tmp_path / "b.xlsx", {"Empty": []})
    with pytest.raises(ExcelReadError):
        read_workbook(blank, SourceConfig(), None)


@pytest.mark.parametrize("value", [0, -1, "6", True, 1.5, None])
def test_header_row_must_be_a_positive_int(value: object) -> None:
    with pytest.raises(ValueError, match="header_row") as error:
        SourceConfig(header_row=value)  # type: ignore[arg-type]
    assert "6" not in str(error.value)


def test_header_row_from_yaml_mapping() -> None:
    assert parse_source_config({"header_row": 6}).header_row == 6
    assert parse_source_config({}).header_row == 1
    with pytest.raises(ValueError, match="header_row"):
        parse_source_config({"header_row": "6"})


def test_xlsx_saved_as_csv_is_read_as_xlsx(tmp_path: Path) -> None:
    path = _workbook(tmp_path / "fake.csv", {"S": _table([("id",), ("a",)])})
    frame = _ingest(path, tmp_path)
    assert frame["id"].to_list() == ["a"]
    assert frame["_sheet_name"].to_list() == ["S"]


def test_delimited_files_keep_null_sheet_name_and_old_numbering(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text("id\n1\n2\n")
    frame = _ingest(path, tmp_path, header_row=6)  # header_row does not apply to delimited
    assert frame["_row_number"].to_list() == [1, 2]
    assert frame["_sheet_name"].to_list() == [None, None]


def test_old_schema_file_and_new_file_read_back_together(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    old = lake / "bronze" / "demo" / "ingest_date=2026-01-01"
    old.mkdir(parents=True)
    pl.DataFrame({"id": ["old"], "_row_number": [1]}).write_parquet(old / "aaaa_old.parquet")
    ingest_file(_two_sheets(tmp_path / "w.xlsx"), "demo", lake, run_at=RUN_AT)
    rows = sorted(read_bronze("demo", lake).project("id, _sheet_name").fetchall())
    assert rows == [("a", "Jan"), ("b", "Jan"), ("c", "Feb"), ("old", None)]


def test_corrupt_workbook_fails_only_that_file_and_keeps_raw(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    # Valid zip signature and workbook marker, but the content is not a workbook.
    with zipfile.ZipFile(landing / "bad.xlsx", "w") as archive:
        archive.writestr("xl/workbook.xml", SECRET)
    _two_sheets(landing / "good.xlsx")
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 1
    assert "event=bronze_error" in caplog.text
    assert 'file_name="bad.xlsx"' in caplog.text
    assert SECRET not in caplog.text
    assert len(list((project / "lake" / "raw" / "demo").glob("receive_date=*/*.xlsx"))) == 2
    assert len(list((project / "lake" / "bronze" / "demo").rglob("*.parquet"))) == 1


def test_truncated_and_protected_workbooks_fail_not_held(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    whole = _two_sheets(project / "w.xlsx").read_bytes()
    (landing / "cut.xlsx").write_bytes(whole[: len(whole) // 2])
    # Password-protected files are an OLE container, not a zip.
    (landing / "locked.xlsx").write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 600)
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 1
    assert caplog.text.count("event=bronze_error") == 2
    assert "bronze_unparsed" not in caplog.text
    assert len(list((project / "lake" / "raw" / "demo").glob("receive_date=*/*"))) == 2


def test_logs_have_no_sheet_names_headers_or_cell_values(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    sheets = {
        "First": _table([("id",), ("a",)]),
        SHEET_SECRET: _table([("SecretHeader",), (SECRET,)]),
    }
    _workbook(_landing(project) / "w.xlsx", sheets)
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 0
    assert 'event=bronze_sheet file_name="w.xlsx" sheet_index=2' in caplog.text
    for text in (SHEET_SECRET, "SecretHeader", SECRET):
        assert text not in caplog.text


def test_failure_messages_have_no_sheet_names(tmp_path: Path) -> None:
    path = _workbook(tmp_path / "w.xlsx", {SHEET_SECRET: _table([("id",)])})
    with pytest.raises(ExcelReadError) as error:
        read_workbook(path, SourceConfig(header_row=9), None)
    assert SHEET_SECRET not in str(error.value)


def test_large_float_is_written_out_in_full_library_behaviour(tmp_path: Path) -> None:
    # Documents calamine/fastexcel behaviour, not a rule of ours: we must not reformat it.
    path = _workbook(tmp_path / "w.xlsx", {"S": [(1, 1, "n"), (2, 1, 1.234567890123457e19)]})
    assert _ingest(path, tmp_path)["n"].to_list() == ["12345678901234569216"]


def test_leading_blank_columns_are_dropped_by_the_library(tmp_path: Path) -> None:
    # Pins current behaviour: columns start at the first used one, so unnamed_<n> counts from it.
    cells = [(1, 3, "id"), (1, 5, None), (2, 3, "a"), (2, 4, "b"), (2, 5, "c")]
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame.columns[:3] == ["id", "unnamed_2", "unnamed_3"]
    assert frame["id"].to_list() == ["a"]


def test_hidden_sheet_is_read(tmp_path: Path) -> None:
    path = _two_sheets(tmp_path / "w.xlsx")
    book = openpyxl.load_workbook(path)
    book["Feb"].sheet_state = "hidden"
    book.save(path)
    assert set(_ingest(path, tmp_path)["_sheet_name"].to_list()) == {"Jan", "Feb"}


def test_merged_range_fills_only_its_top_left_cell(tmp_path: Path) -> None:
    path = _workbook(tmp_path / "w.xlsx", {"S": _table([("id", "n"), ("a", "m"), ("b", "k")])})
    book = openpyxl.load_workbook(path)
    book["S"].merge_cells("A2:A3")  # keeps only A2 ("a"); A3 is emptied by the merge
    book.save(path)
    assert _ingest(path, tmp_path)["id"].to_list() == ["a", None]


# Issue #33 items 7 and 9. These document what calamine hands back; our code must not cast.
def test_date_time_bool_and_percent_cells_arrive_in_calamines_string_form(tmp_path: Path) -> None:
    cells = [
        (1, 1, "d"),
        (1, 2, "dt"),
        (1, 3, "t"),
        (1, 4, "b"),
        (1, 5, "pct"),
        (2, 1, date(2026, 1, 2)),
        (2, 2, datetime(2026, 1, 2, 3, 4, 5)),
        (2, 3, time(3, 4, 5)),
        (2, 4, True),
        (2, 5, 0.256),
    ]
    path = _workbook(tmp_path / "w.xlsx", {"S": cells})
    book = openpyxl.load_workbook(path)
    book["S"]["E2"].number_format = "0.0%"  # shown as 25.6%, stored as 0.256
    book.save(path)

    frame = _ingest(path, tmp_path)

    assert frame.select("d", "dt", "t", "b", "pct").row(0) == (
        "2026-01-02 00:00:00",  # a date is shown as a datetime (date versus datetime is lost)
        "2026-01-02 03:04:05",
        "1899-12-31 03:04:05",  # a time of day gets calamine's epoch date
        "true",
        "0.256",  # the stored value, not the displayed "25.6%"
    )
    assert {frame.schema[c] for c in ("d", "dt", "t", "b", "pct")} == {pl.String}


def test_formula_without_a_cached_value_is_null(tmp_path: Path) -> None:
    cells = [(1, 1, "f"), (1, 2, "k"), (2, 1, "=1+2"), (2, 2, "x")]  # openpyxl stores no cache
    frame = _ingest(_workbook(tmp_path / "w.xlsx", {"S": cells}), tmp_path)
    assert frame.select("f", "k").rows() == [(None, "x")]


def _xlsm(path: Path) -> Path:
    _workbook(path.with_suffix(".xlsx"), {"S": _table([("id", "n"), ("007", "x")])}).rename(path)
    return path


@pytest.mark.parametrize("extension", [".xlsm", ".ods"])
def test_xlsm_and_ods_files_load_through_the_cli(
    project: Path, ods_bytes: Callable[..., bytes], extension: str
) -> None:
    path = _landing(project) / f"w{extension}"
    if extension == ".ods":
        path.write_bytes(ods_bytes([["id", "n"], ["007", "x"]]))
    else:
        _xlsm(path)

    assert main(["demo"]) == 0

    frame = _bronze(project / "lake")
    assert frame.select("id", "n").rows() == [("007", "x")]
    assert frame["_sheet_name"].to_list() == ["S"]
    assert frame["_file_name"].to_list() == [path.name]
    assert [p.suffix for p in (project / "lake" / "raw" / "demo").rglob("*.*")] == [extension]


def test_ods_date_and_number_cells_arrive_as_stored_not_as_shown(
    tmp_path: Path, ods_bytes: Callable[..., bytes]
) -> None:
    # Pins calamine's ODS forms (the cell's displayed text is "shown"); our code does not cast.
    path = tmp_path / "w.ods"
    path.write_bytes(ods_bytes([["d", "n"], [("date", "2026-01-02"), ("float", "2.50")]]))

    frame = _ingest(path, tmp_path)

    assert frame.select("d", "n").rows() == [("2026-01-02", "2.5")]


def _corrupt(path: Path) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("xl/workbook.xml", SECRET)
    return path


def _truncated(path: Path) -> Path:
    whole = _two_sheets(path).read_bytes()
    path.write_bytes(whole[: len(whole) // 2])
    return path


def _protected(path: Path) -> Path:
    path.write_bytes(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 600)
    return path


def _plain(path: Path) -> Path:
    return _workbook(path, {"S": _table([("id",), ("a",)])})


def _duplicate(path: Path) -> Path:
    return _workbook(path, {"S": [(1, 1, SECRET), (1, 2, SECRET), (2, 1, "x")]})


@pytest.mark.parametrize(
    ("make", "config"),
    [
        (_corrupt, SourceConfig()),
        (_truncated, SourceConfig()),
        (_protected, SourceConfig()),
        (_plain, SourceConfig(sheets=(SECRET,))),
        (_plain, SourceConfig(header_row=9)),
        (_duplicate, SourceConfig()),
    ],
    ids=["corrupt", "truncated", "protected", "missing_sheet", "header_past_end", "duplicate"],
)
def test_excel_errors_carry_no_context_or_secret(
    tmp_path: Path, make: Callable[[Path], Path], config: SourceConfig
) -> None:
    path = make(tmp_path / "w.xlsx")
    with pytest.raises(ExcelReadError) as caught:
        read_workbook(path, config, None)
    exc = caught.value
    assert exc.__context__ is None
    assert exc.__cause__ is None
    assert SECRET not in "".join(traceback.format_exception(exc))
