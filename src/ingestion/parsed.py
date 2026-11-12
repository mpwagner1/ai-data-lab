"""The shared result type of every reader (ADR 0002)."""

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class ParsedPart:
    """One unit a reader found in a file, ready for metadata.

    `sheet_name` is None for files that are not workbooks. `frame` has only source columns, all
    strings. `first_row_number` is the `_row_number` of the frame's first row: 1 for delimited
    files (the data-row index), the sheet row for workbooks (`header_row + 1`).
    """

    sheet_name: str | None
    frame: pl.LazyFrame
    first_row_number: int = 1
