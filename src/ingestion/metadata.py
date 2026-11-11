"""Bronze metadata columns and file hashing."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

BRONZE_META_COLUMNS: tuple[str, ...] = ("_file_name", "_ingested_at", "_file_hash", "_row_number")

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
) -> pl.LazyFrame:
    """Append the four bronze metadata columns to a frame of source rows.

    `ingested_at` is passed in (not read here) so every row of one load shares it.
    """
    if ingested_at.tzinfo is None or ingested_at.utcoffset() != UTC.utcoffset(None):
        raise ValueError("ingested_at must be a timezone-aware UTC datetime")
    return frame.with_columns(
        pl.lit(file_name, dtype=pl.String).alias("_file_name"),
        pl.lit(ingested_at, dtype=pl.Datetime("us", "UTC")).alias("_ingested_at"),
        pl.lit(file_hash, dtype=pl.String).alias("_file_hash"),
        pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias("_row_number"),
    )
