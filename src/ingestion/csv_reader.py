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


def _wrong_byte_order(sample: bytes, codec: str) -> bool:
    """True if an explicit-endian UTF-16/32 sample has its NUL bytes where the low byte should be.

    ASCII-range text has NUL in the high-order bytes of each code unit. If the NULs sit in the
    low-order byte instead, the bytes were written in the opposite order.
    """
    size = 2 if codec.startswith("utf-16") else 4
    sample = sample[: len(sample) - len(sample) % size]
    if not sample:
        return False
    low_at = size - 1 if codec.endswith("-be") else 0
    units = len(sample) // size
    low_nuls = sample[low_at::size].count(0)
    high_nuls = sample.count(0) - low_nuls
    return low_nuls / units > high_nuls / (units * (size - 1))


def _check_decoded(raw: bytes, text: str, encoding: str) -> None:
    """Raise UnicodeError if a non-UTF-8 decode looks wrong (wrong codec or byte order).

    A wrong codec can decode "successfully" into nonsense, so the decode is checked:

    - UTF-16/32 (plain or explicit-endian): real text of any realistic file has NUL bytes
      (a newline code unit contains one), so bytes with no NUL are UTF-8 or other single-byte
      text read with the wrong codec. Plain `utf-16`/`utf-32` must also start with a BOM.
    - Explicit-endian UTF-16/32: NUL bytes must sit in the high-order byte (wrong byte order
      is held, including a header-only file with no newline).
    - A UTF-8 BOM read with any codec other than `utf-8-sig` is held (it would otherwise leak
      into the first column name).
    - The first line must have no replacement, control, unassigned or private-use characters.
      This uses Python's `unicodedata` (Python 3.12 is Unicode 15.0), so a very new character in
      a UTF-16 header can cause a visible false hold; that is accepted.

    Cannot be detected: general mojibake (for example `cafÃ©`), and genuine UTF-16/32 text
    with no NUL byte at all (for example CJK only), which is held. Values are only inspected,
    never changed. Messages carry no file content.
    """
    codec = codecs.lookup(encoding).name
    if raw.startswith(codecs.BOM_UTF8) and codec != "utf-8-sig":
        raise UnicodeDecodeError(encoding, b"", 0, 1, "UTF-8 BOM")
    if codec.startswith(("utf-16", "utf-32")):
        if b"\x00" not in raw:
            raise UnicodeDecodeError(encoding, b"", 0, 1, "no NUL bytes")
        if codec in _BOM_ENCODINGS and not raw.startswith(_BOM_ENCODINGS[codec]):
            raise UnicodeDecodeError(encoding, b"", 0, 1, "no BOM")
        if codec.endswith(("-le", "-be")) and _wrong_byte_order(raw[:_HEADER_CHARS], codec):
            raise UnicodeDecodeError(encoding, b"", 0, 1, "byte order")
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
