"""Errors raised while handling a library error must not keep it as `__context__`."""

import traceback
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import fastexcel
import openpyxl
import pytest

from ingestion import bronze
from ingestion.detect import validate_format
from ingestion.excel_reader import ExcelReadError, read_workbook
from ingestion.source_config import SourceConfig, load_source_config, validate_encoding

SECRET = "SECRETCONFIGTEXT"


def _bad_yaml(tmp_path: Path) -> None:
    (tmp_path / "demo.yaml").write_text(f"a: [{SECRET}\n")
    load_source_config("demo", tmp_path)


def _undecodable(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes(b"id\n\xff\xfe\xfa\n")
    bronze._load_with_metadata(
        path,
        "0" * 64,
        datetime.now(UTC),
        SourceConfig(encoding="utf-8"),
        None,
        bronze.FileFormat.DELIMITED,
        path.name,
    )


@pytest.mark.parametrize(
    "run",
    [
        _bad_yaml,
        lambda _: validate_encoding(SECRET),
        lambda _: validate_format(SECRET),
        _undecodable,
    ],
    ids=["yaml", "encoding", "format", "undecodable_file"],
)
def test_no_context_or_cause_and_no_secret(tmp_path: Path, run: Callable[[Path], None]) -> None:
    with pytest.raises(Exception) as caught:  # noqa: B017 - the type varies by site
        run(tmp_path)
    exc = caught.value
    assert exc.__context__ is None
    assert exc.__cause__ is None
    assert SECRET not in "".join(traceback.format_exception(exc))


def _damaged_sheet_xlsx(path: Path) -> None:
    """A real workbook whose `sheet1.xml` is cut short; the rest of the zip stays valid."""
    book = openpyxl.Workbook()
    book.active.append(["id"])
    book.active.append([SECRET])
    book.save(path)
    with zipfile.ZipFile(path) as source:
        members = {name: source.read(name) for name in source.namelist()}
    sheet = members["xl/worksheets/sheet1.xml"]
    members["xl/worksheets/sheet1.xml"] = sheet[: sheet.index(b"<sheetData>") + 20]
    with zipfile.ZipFile(path, "w") as target:
        for name, data in members.items():
            target.writestr(name, data)


def test_damaged_sheet_has_no_context_or_cause_and_no_secret(tmp_path: Path) -> None:
    path = tmp_path / "damaged.xlsx"
    _damaged_sheet_xlsx(path)
    book = fastexcel.read_excel(path)  # the workbook opens; only the sheet is damaged
    assert book.sheet_names

    with pytest.raises(ExcelReadError) as caught:
        read_workbook(path, SourceConfig(), None)

    exc = caught.value
    assert exc.__context__ is None
    assert exc.__cause__ is None
    assert SECRET not in "".join(traceback.format_exception(exc))
