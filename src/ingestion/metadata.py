"""Bronze metadata columns and file hashing."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

BRONZE_META_COLUMNS: tuple[str, ...] = (
    "_file_name",
    "_ingested_at",
    "_file_hash",
    "_row_number",
    "_sheet_name",
)

_HASH_CHUNK_BYTES = 1024 * 1024


def hash_file(path: Path) -> str:
    """Return the SHA-256 hex digest of the file bytes, read in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def add_bronze_metadata(
    frame: pl.LazyFrame,
    file_name: str,
    file_hash: str,
    ingested_at: datetime,
    first_row_number: int = 1,
    sheet_name: str | None = None,
) -> pl.LazyFrame:
    """Append the five bronze metadata columns to a frame of source rows.

    `ingested_at` is passed in (not read here) so every row of one load shares it.
    `_row_number` counts from `first_row_number`: 1 for delimited files (position among data
    rows), `header_row + 1` for a workbook sheet (the row as shown in the sheet). `sheet_name`
    fills `_sheet_name` (null when the file is not a workbook).
    """
    if ingested_at.tzinfo is None or ingested_at.utcoffset() != UTC.utcoffset(None):
        raise ValueError("ingested_at must be a timezone-aware UTC datetime")
    return frame.with_columns(
        pl.lit(file_name, dtype=pl.String).alias("_file_name"),
        pl.lit(ingested_at, dtype=pl.Datetime("us", "UTC")).alias("_ingested_at"),
        pl.lit(file_hash, dtype=pl.String).alias("_file_hash"),
        pl.int_range(first_row_number, first_row_number + pl.len(), dtype=pl.Int64).alias(
            "_row_number"
        ),
        pl.lit(sheet_name, dtype=pl.String).alias("_sheet_name"),
    )
