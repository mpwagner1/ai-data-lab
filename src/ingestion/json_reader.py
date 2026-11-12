"""Read JSON (a top-level array of objects) and NDJSON (one object per line) for bronze.

Every source column is `pl.String` and scalars keep their source text. The parser is Python's
`json` with `parse_int`, `parse_float` and `parse_constant` set to keep the token as text, so
`1.50` stays `"1.50"`, `1e3` stays `"1e3"`, a 20-digit integer is not rounded and `NaN` stays
`"NaN"`. Polars inference is never used (it would turn numbers into floats). Mapping:

- string: kept exactly (escapes decoded, nothing trimmed).
- number: its source text. `true` / `false`: `"true"` / `"false"`.
- `null`, or a key missing from a record: null.
- object or array: compact JSON text (no spaces, `ensure_ascii=False`, key order preserved),
  with numbers inside it written as their source text too (`{"a":1.50}`).

Columns are the union of keys over all records in first-seen order; a record without a key gets
null. A duplicate key inside one object keeps the last value (Python `json` behaviour); this is
not detected or fixed. `_row_number` is the 1-based position among records (blank NDJSON lines
are skipped and not counted).

A UTF-8 byte order mark is ignored. The file is decoded with `config.encoding`; a file that does
not decode raises UnicodeError, which bronze holds as `encoding` (same as CSV).

Memory: a JSON array is loaded in full (text plus parsed records), so it is unbounded for large
files. NDJSON is read line by line and only the column lists are kept. Caps come with archives.

Errors name the error type and the file only, never record text, key names, values or parser
messages. Raises are chained with `from None` so library text is not carried.
"""

import json
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from ingestion.parsed import ParsedPart
from ingestion.source_config import SourceConfig


class JsonReadError(ValueError):
    """A JSON or NDJSON file cannot be read (invalid, wrong shape, empty or reserved key)."""


class _Number(str):
    """A number token kept as source text; nested encoding writes it without quotes."""

    __slots__ = ()


def _fail(path: Path, problem: str) -> JsonReadError:
    return JsonReadError(f"cannot read JSON {path.name}: {problem}")


def _encode(value: object) -> str:
    """Compact JSON text of a parsed value, numbers as their source text."""
    if isinstance(value, _Number):
        return str.__str__(value)
    if isinstance(value, dict):
        return "{" + ",".join(f"{_encode_text(k)}:{_encode(v)}" for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ",".join(_encode(item) for item in value) + "]"
    if isinstance(value, str):
        return _encode_text(value)
    return json.dumps(value)  # True, False, None


def _encode_text(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def _cell(value: object) -> str | None:
    """One top-level value as a bronze string."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, _Number):
        return str.__str__(value)
    if isinstance(value, str):
        return value
    return _encode(value)


def _decoder() -> json.JSONDecoder:
    return json.JSONDecoder(parse_int=_Number, parse_float=_Number, parse_constant=_Number)


class _Columns:
    """Column lists built record by record, keys in first-seen order."""

    def __init__(self) -> None:
        self.data: dict[str, list[str | None]] = {}
        self.rows = 0

    def add(self, record: dict[str, object]) -> None:
        for key, value in record.items():
            if key not in self.data:
                self.data[key] = [None] * self.rows
            self.data[key].append(_cell(value))
        self.rows += 1
        for column in self.data.values():
            if len(column) < self.rows:
                column.append(None)

    def frame(self, path: Path) -> pl.LazyFrame:
        if self.rows == 0:
            raise _fail(path, "empty file")
        if not self.data:
            raise _fail(path, "records have no keys")
        if any(name.startswith("_") for name in self.data):
            raise _fail(path, "a key starts with '_' (reserved for bronze metadata)")
        schema = {name: pl.String for name in self.data}
        return pl.DataFrame(self.data, schema=schema).lazy()


def _records(path: Path, items: Iterable[object]) -> _Columns:
    columns = _Columns()
    for item in items:
        if not isinstance(item, dict):
            raise _fail(path, "a record is not an object")
        columns.add(item)
    return columns


def _array_items(path: Path, text: str) -> list[object]:
    items = _decoder().decode(text.removeprefix("﻿"))
    if not isinstance(items, list):
        raise _fail(path, "top level is not an array")
    return items


def _lines(path: Path, encoding: str) -> Iterable[str]:
    with path.open(encoding=encoding, newline=None) as handle:
        for number, line in enumerate(handle):
            text = line.removeprefix("﻿") if number == 0 else line
            if text.strip():
                yield text


def _ndjson_items(path: Path, encoding: str) -> Iterable[object]:
    decoder = _decoder()
    for line in _lines(path, encoding):
        yield decoder.decode(line.strip())


def _build(path: Path, load: Iterable[object] | list[object]) -> list[ParsedPart]:
    try:
        columns = _records(path, load)
    except (UnicodeError, JsonReadError):
        raise
    except Exception as error:
        raise _fail(path, type(error).__name__) from None
    return [ParsedPart(None, columns.frame(path))]


def read_json(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    """Read a JSON file holding one array of objects as a single all-string part.

    Raises JsonReadError for invalid JSON, a non-array top level, a non-object element, an empty
    array or file, records with no keys or a key starting with `_`. UnicodeError (not decodable)
    is left to bronze to hold. `flag` (the --delimiter value) does not apply.
    """
    try:
        text = path.read_text(encoding=config.encoding)
        if not text.strip():
            raise _fail(path, "empty file")
        items = _array_items(path, text)
    except (UnicodeError, JsonReadError):
        raise
    except Exception as error:
        raise _fail(path, type(error).__name__) from None
    return _build(path, items)


def read_ndjson(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    """Read newline-delimited JSON (one object per line, blank lines skipped) as one part.

    Raises JsonReadError like `read_json`, with a line that is not an object in place of a
    non-array top level. `flag` (the --delimiter value) does not apply.
    """
    return _build(path, _ndjson_items(path, config.encoding))
