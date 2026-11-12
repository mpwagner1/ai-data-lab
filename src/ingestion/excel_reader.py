"""Read Excel and OpenDocument workbooks for bronze with `fastexcel` (calamine).

Every cell is read as a string, exactly as the reader yields it. Excel stores numbers as numbers, so
they come back in calamine's string form (for example `1.0`, and `001` is already `1` in the file);
dates and booleans likewise. Nothing is cast, rounded, trimmed or reformatted here, and merged cells
are not filled: a merged range yields whatever calamine gives (the value in its top-left cell only).

Rows are never dropped: blank rows inside the data stay, so `_row_number` (the sheet row, as Excel
shows it) is exact. The sheet is read from its first row, so rows above `header_row` are skipped
by position, not guessed. A blank header cell becomes `unnamed_<n>` (n is the 1-based position of
the column among the columns read). That is the only rename.

Errors name the error type and the file, never cell text, sheet content or parser messages.
"""

import json
import logging
from pathlib import Path

import fastexcel
import polars as pl

from ingestion.parsed import ParsedPart
from ingestion.source_config import SourceConfig

logger = logging.getLogger(__name__)


class ExcelReadError(Exception):
    """A workbook cannot be read (corrupt, truncated, password-protected or misconfigured)."""


def _fail(path: Path, problem: str) -> ExcelReadError:
    return ExcelReadError(f"cannot read workbook {path.name}: {problem}")


def _open(path: Path) -> fastexcel.ExcelReader:
    try:
        return fastexcel.read_excel(path)
    except Exception as error:
        # The parser message can quote file content, so only its type is kept.
        raise _fail(path, type(error).__name__) from None


def _select_sheets(path: Path, names: list[str], wanted: tuple[str, ...] | None) -> list[str]:
    if wanted is None:
        return names
    if not set(wanted) <= set(names):
        raise _fail(path, "a sheet listed in `sheets` is not in the workbook")
    return [name for name in names if name in wanted]


def _load_rows(path: Path, book: fastexcel.ExcelReader, sheet: str) -> pl.DataFrame:
    """All rows of the sheet from sheet row 1, no header applied, every cell a string."""
    try:
        # header_row=None with skip_rows=0 keeps leading blank rows, so row i is sheet row i + 1.
        loaded = book.load_sheet(sheet, header_row=None, skip_rows=0, dtypes="string")
        return loaded.to_polars()
    except Exception as error:
        raise _fail(path, type(error).__name__) from None


def _column_names(header: tuple[object, ...]) -> list[str]:
    return [
        str(cell) if cell not in (None, "") else f"unnamed_{position}"
        for position, cell in enumerate(header, start=1)
    ]


def _check_names(path: Path, names: list[str]) -> None:
    if any(name.startswith("_") for name in names):
        raise _fail(path, "a column name starts with '_' (reserved for bronze metadata)")
    if len(set(names)) != len(names):
        raise _fail(path, "duplicate column names")


def _log_sheet(path: Path, sheet: str, header_row: int, skipped: int, rows: int) -> None:
    logger.info(
        "event=bronze_sheet file_name=%s sheet=%s header_row=%d skipped_rows=%d rows=%d",
        json.dumps(path.name),
        json.dumps(sheet),
        header_row,
        skipped,
        rows,
    )


def _parse_sheet(path: Path, sheet: str, rows: pl.DataFrame, header_row: int) -> ParsedPart:
    if header_row > rows.height:
        raise _fail(path, "`header_row` is past the end of a sheet")
    names = _column_names(rows.row(header_row - 1))
    _check_names(path, names)
    data = rows.slice(header_row)
    data.columns = names
    _log_sheet(path, sheet, header_row, header_row - 1, data.height)
    return ParsedPart(sheet, data.lazy(), first_row_number=header_row + 1)


def read_workbook(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    """Read every sheet (or those in `config.sheets`) as one all-string part per sheet.

    A sheet with no cells at all has nothing to load and is skipped. Raises ExcelReadError for a
    workbook that cannot be opened, a configured sheet that is missing, `header_row` past the end
    of a sheet, a reserved or duplicate column name, or a workbook with no sheet with cells.
    `flag` (the --delimiter value) does not apply to workbooks.
    """
    book = _open(path)
    parts: list[ParsedPart] = []
    for sheet in _select_sheets(path, book.sheet_names, config.sheets):
        rows = _load_rows(path, book, sheet)
        if rows.width == 0:
            continue
        parts.append(_parse_sheet(path, sheet, rows, config.header_row))
    if not parts:
        raise _fail(path, "no sheet with cells")
    return parts
