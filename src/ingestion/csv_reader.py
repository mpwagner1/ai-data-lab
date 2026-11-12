"""Read CSV files for bronze: every value stays a string, exactly as read."""

import codecs
import io
from pathlib import Path

import polars as pl

DEFAULT_SEPARATOR = ","
DEFAULT_ENCODING = "utf-8"
_CHUNK_BYTES = 1024 * 1024


def _has_no_content(path: Path) -> bool:
    """True when the file is zero bytes or only whitespace (so it has no header)."""
    return not path.read_bytes().strip()


def _check_utf8(path: Path) -> None:
    """Stream the file through a strict UTF-8 decoder; raises UnicodeDecodeError if invalid.

    Polars would fail later with a generic error, so this gives bronze a clear `encoding` signal.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_BYTES):
            decoder.decode(chunk)
    decoder.decode(b"", final=True)


def _scan(path: Path, separator: str, encoding: str) -> pl.LazyFrame:
    if encoding.lower().replace("_", "-") in {"utf-8", "utf8"}:
        _check_utf8(path)
        return pl.scan_csv(path, separator=separator, infer_schema=False)
    # Polars only reads UTF-8, so decode other encodings first (in memory, not lazy).
    text = path.read_bytes().decode(encoding)
    frame = pl.read_csv(io.BytesIO(text.encode("utf-8")), separator=separator, infer_schema=False)
    return frame.lazy()


def read_csv_as_strings(
    path: Path, separator: str = DEFAULT_SEPARATOR, encoding: str = DEFAULT_ENCODING
) -> pl.LazyFrame:
    """Read a delimited file with all columns as strings (no inference or renaming).

    UTF-8 files are scanned lazily. Other encodings are decoded fully in memory first.

    Raises ValueError for an empty file or a source column starting with `_`, which would
    collide with bronze metadata columns. Messages name columns, never values.
    """
    if _has_no_content(path):
        raise ValueError(f"empty file: {path.name}")
    frame = _scan(path, separator, encoding)
    reserved = [name for name in frame.collect_schema().names() if name.startswith("_")]
    if reserved:
        raise ValueError(f"source columns start with '_': {reserved}")
    return frame
