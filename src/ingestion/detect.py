"""Work out what format a file is: config, then file signature, then text heuristics.

Only the first bytes of a file (and, for zip containers, the member names) are read. Nothing
here logs or returns file content.
"""

import json
import zipfile
from enum import Enum
from pathlib import Path

from ingestion.source_config import SourceConfig

_HEAD_BYTES = 64 * 1024
_UTF8_BOM = b"\xef\xbb\xbf"
_DELIMITED_EXTENSIONS = frozenset({".csv", ".tsv", ".psv", ".txt", ".dat"})
# These always mean "delimited text"; JSON heuristics are skipped so a header like `[id]` is safe.
_EXTENSIONS_WITH_DELIMITER = frozenset({".csv", ".tsv", ".psv"})


class FileFormat(Enum):
    """File formats bronze knows about. Only those in `READERS` can be parsed so far."""

    DELIMITED = "delimited"
    XLSX = "xlsx"
    XLS = "xls"
    ODS = "ods"
    PARQUET = "parquet"
    GZIP = "gzip"
    ZIP = "zip"
    JSON = "json"
    NDJSON = "ndjson"


def _from_config(name: str) -> FileFormat:
    try:
        return FileFormat(name)
    except ValueError:
        raise ValueError("unknown `format` in source config") from None


def _zip_container(path: Path) -> FileFormat:
    try:
        with zipfile.ZipFile(path) as archive:
            names = set(archive.namelist())
    except zipfile.BadZipFile:
        return FileFormat.ZIP
    if "xl/workbook.xml" in names:
        return FileFormat.XLSX
    if "content.xml" in names and "mimetype" in names:
        return FileFormat.ODS
    return FileFormat.ZIP


def _from_signature(path: Path, head: bytes) -> FileFormat | None:
    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06"):
        return _zip_container(path)
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        return FileFormat.XLS
    if head.startswith(b"PAR1"):
        return FileFormat.PARQUET
    if head.startswith(b"\x1f\x8b"):
        return FileFormat.GZIP
    return None


def _json_kind(head: bytes) -> FileFormat | None:
    text = head.removeprefix(_UTF8_BOM).lstrip()
    if text.startswith(b"["):
        return FileFormat.JSON
    if not text.startswith(b"{"):
        return None
    first_line = text.split(b"\n", 1)[0]
    try:
        json.loads(first_line)
    except ValueError:
        return FileFormat.JSON  # a pretty-printed object, not one object per line
    return FileFormat.NDJSON


def _from_text(extension: str, head: bytes) -> FileFormat | None:
    if b"\x00" in head:
        return None
    if extension not in _EXTENSIONS_WITH_DELIMITER:
        kind = _json_kind(head)
        if kind is not None:
            return kind
    if extension in _DELIMITED_EXTENSIONS:
        return FileFormat.DELIMITED
    return None


def detect_format(path: Path, config: SourceConfig) -> FileFormat | None:
    """Return the file's format, or None when it is not recognised (it is then held).

    Order: `format:` in config, file signature (so an xlsx saved as `.csv` is still xlsx), then
    text heuristics. Text with an unknown extension is not guessed to be delimited.
    Raises ValueError if config names a format that does not exist.
    """
    if config.format is not None:
        return _from_config(config.format)
    with path.open("rb") as handle:
        head = handle.read(_HEAD_BYTES)
    return _from_signature(path, head) or _from_text(path.suffix.lower(), head)
