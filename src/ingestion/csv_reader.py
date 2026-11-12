"""Read CSV files for bronze: every value stays a string, exactly as read."""

import codecs
import io
import unicodedata
from pathlib import Path

import polars as pl

DEFAULT_SEPARATOR = ","
DEFAULT_ENCODING = "utf-8"
_CHUNK_BYTES = 1024 * 1024


def looks_utf16(head: bytes) -> bool:
    """UTF-16/32 text: a BOM, or NUL in every second byte of the start (ASCII-range text)."""
    if head.startswith((b"\xff\xfe", b"\xfe\xff")):
        return True
    start = head[:64]
    return len(start) >= 4 and (
        start[1::2].count(0) == len(start[1::2]) or start[::2].count(0) == len(start[::2])
    )


def _has_no_content(path: Path) -> bool:
    """True when the file is zero bytes or only whitespace (so it has no header)."""
    return not path.read_bytes().strip()


def _check_utf8(path: Path) -> None:
    """Stream the file through a strict UTF-8 decoder; raises UnicodeDecodeError if invalid.

    Polars would fail later with a generic error, so this gives bronze a clear `encoding` signal.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    with path.open("rb") as handle:
        first = handle.read(_CHUNK_BYTES)
        # UTF-16/32 text mislabelled as UTF-8 is refused rather than misread. A stray NUL in
        # otherwise valid UTF-8 is data and loads unchanged, at any offset.
        if looks_utf16(first):
            raise UnicodeDecodeError("utf-8", b"", 0, 1, "UTF-16/32 pattern")
        chunk = first
        while chunk:
            decoder.decode(chunk)
            chunk = handle.read(_CHUNK_BYTES)
    decoder.decode(b"", final=True)


_HEADER_CHARS = 64 * 1024
_BOM_ENCODINGS = {
    "utf-16": (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE),
    "utf-32": (codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE),
}


def _is_garbage_char(char: str) -> bool:
    """Control (but not tab), unassigned, private-use, surrogate or replacement characters."""
    if char == "\t":
        return False
    return char == "\ufffd" or unicodedata.category(char) in {"Cc", "Cn", "Co", "Cs"}


def _check_decoded(raw: bytes, text: str, encoding: str) -> None:
    """Raise UnicodeError if a non-UTF-8 decode looks wrong (wrong codec or byte order).

    A wrong UTF-16/32 byte order or codec can decode "successfully" into nonsense. Plain
    `utf-16`/`utf-32` must start with a BOM (otherwise the byte order is a guess). The start of
    the text must have no replacement, control, unassigned or private-use characters. Values are
    only inspected, never changed. Messages carry no file content.
    """
    boms = _BOM_ENCODINGS.get(codecs.lookup(encoding).name)
    if boms is not None and not raw.startswith(boms):
        raise UnicodeDecodeError(encoding, b"", 0, 1, "no BOM")
    header = text[:_HEADER_CHARS].split("\n", 1)[0].removeprefix("\ufeff").rstrip("\r")
    if any(_is_garbage_char(char) for char in header):
        raise UnicodeDecodeError(encoding, b"", 0, 1, "implausible header")


def _scan(path: Path, separator: str, encoding: str) -> pl.LazyFrame:
    if encoding.lower().replace("_", "-") in {"utf-8", "utf8"}:
        _check_utf8(path)
        return pl.scan_csv(path, separator=separator, infer_schema=False)
    # Polars only reads UTF-8, so decode other encodings first (in memory, not lazy).
    raw = path.read_bytes()
    text = raw.decode(encoding)
    _check_decoded(raw, text, encoding)
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
