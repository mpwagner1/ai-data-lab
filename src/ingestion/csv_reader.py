"""Read CSV files for bronze: every value stays a string, exactly as read."""

from pathlib import Path

import polars as pl


def read_csv_as_strings(path: Path) -> pl.LazyFrame:
    """Lazily read a CSV with all columns as strings (no inference, trimming or renaming).

    Raises ValueError for an empty file or a source column starting with `_`, which would
    collide with bronze metadata columns. Messages name columns, never values.
    """
    if path.stat().st_size == 0:
        raise ValueError(f"empty file: {path.name}")
    frame = pl.scan_csv(path, infer_schema=False)
    reserved = [name for name in frame.collect_schema().names() if name.startswith("_")]
    if reserved:
        raise ValueError(f"source columns start with '_': {reserved}")
    return frame
