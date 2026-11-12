"""Read Parquet files for bronze with Polars, converting every column to `pl.String`.

Parquet is the one source format with its own types, so here string conversion is a real
conversion (not just keeping text). Nothing is trimmed or reformatted beyond the string form
Polars (2.1) gives; the original file stays in the raw zone. Observed forms:

- integers: plain digits (`42`). Booleans: `true` / `false`.
- floats: Polars' shortest round-trip text (`1.0`, `0.1`, `1e+20`, `NaN`).
- decimals: exact digits at the column scale (`1.50`).
- dates: `YYYY-MM-DD`. Times: `HH:MM:SS`.
- timestamps: `YYYY-MM-DD HH:MM:SS.<fraction>`, where the fraction follows the column's time unit
  (ms gives 3 digits, us 6, ns 9). A time-zone-aware column adds the offset (`+00:00`, `-05:00`).
- durations: ISO-8601 text as Polars writes it (`PT1M30.000005S`, `-P1D`; `PT90S` for 90 s).
- strings and categoricals: unchanged.
- binary: lowercase hex (`00ab`). Raw bytes may not be valid text, so hex is the safe form.
- struct, list, array and map (a list of key/value structs): compact JSON text
  (`{"a":1,"b":"x"}`, `[1,2]`). Inside that JSON, numbers and booleans are JSON values; dates,
  times, timestamps, decimals and durations are JSON strings (`"2020-01-02"`, `"1.50"`).
  Binary inside a nested value is not supported by Polars and fails the file.
- null stays null, in nested values too.

All columns are collected in full, so memory is unbounded for large files (caps come with
archives). A column name starting with `_` (this includes a bronze Parquet fed back in) fails the
file. A file with zero columns fails. A file with columns and zero rows becomes a zero-row part.

Errors name the error type and the file only, never column names, values or library messages;
they are chained with `from None`.
"""

import secrets
from collections.abc import Callable
from pathlib import Path

import polars as pl

from ingestion.parsed import ParsedPart
from ingestion.source_config import SourceConfig

_NESTED = (pl.Struct, pl.List, pl.Array)
_WRAP = "bronze_wrap"
_WRAP_PREFIX = len(f'{{"{_WRAP}":')


class ParquetReadError(ValueError):
    """A Parquet file cannot be read (corrupt, no columns, reserved column or unsupported type)."""


def _fail(path: Path, problem: str) -> ParquetReadError:
    return ParquetReadError(f"cannot read Parquet {path.name}: {problem}")


def _contains_binary(dtype: pl.DataType) -> bool:
    if dtype == pl.Binary:
        return True
    if isinstance(dtype, pl.Struct):
        return any(_contains_binary(field.dtype) for field in dtype.fields)
    if isinstance(dtype, pl.List | pl.Array):
        return _contains_binary(dtype.inner)
    return False


def _has_float(dtype: pl.DataType) -> bool:
    if dtype in (pl.Float32, pl.Float64):
        return True
    if isinstance(dtype, pl.Struct):
        return any(_has_float(field.dtype) for field in dtype.fields)
    if isinstance(dtype, pl.List | pl.Array):
        return _has_float(dtype.inner)
    return False


def _mark_floats(expr: pl.Expr, dtype: pl.DataType, token: str) -> pl.Expr:
    """Turn every float inside a nested value into `<token><text>` so JSON encoding keeps it.

    Polars' JSON writer nulls NaN and inf, so floats go through the string cast (the same text as
    at top level) and the marker lets `_nested_to_json` write them back as bare tokens.
    """
    if not _has_float(dtype):
        return expr
    if isinstance(dtype, pl.Struct):
        fields = [
            _mark_floats(expr.struct.field(f.name), f.dtype, token).alias(f.name)
            for f in dtype.fields
        ]
        return pl.when(expr.is_null()).then(None).otherwise(pl.struct(fields))
    if isinstance(dtype, pl.List):
        return expr.list.eval(_mark_floats(pl.element(), dtype.inner, token))
    if isinstance(dtype, pl.Array):
        return expr.arr.eval(_mark_floats(pl.element(), dtype.inner, token))
    return pl.concat_str([pl.lit(token), expr.cast(pl.String)])


def _nested_to_json(series: pl.Series) -> pl.Series:
    """JSON text per value, null where the value is null."""
    token = secrets.token_hex(8)
    marked = (
        series.alias(_WRAP)
        .to_frame()
        .select(_mark_floats(pl.col(_WRAP), series.dtype, token).alias(_WRAP))
    )
    encoded = marked.select(pl.struct(_WRAP).struct.json_encode().alias(_WRAP)).to_series()
    text = encoded.str.slice(_WRAP_PREFIX).str.head(-1)
    text = text.str.replace_all(f'"{token}([^"]*)"', "${1}")
    return pl.select(
        pl.when(series.is_null()).then(None).otherwise(text).alias(series.name)
    ).to_series()


def _as_string(series: pl.Series) -> pl.Series:
    if isinstance(series.dtype, _NESTED):
        if _contains_binary(series.dtype):
            raise ValueError("binary inside a nested value")
        return _nested_to_json(series).alias(series.name)
    if series.dtype == pl.Binary:
        return series.bin.encode("hex")
    if isinstance(series.dtype, pl.Duration):
        return series.dt.to_string()
    return series.cast(pl.String)


def _guarded[T](work: Callable[[], T]) -> tuple[T | None, str | None]:
    """Run `work`; return (result, None) or (None, error type name).

    The caller raises outside this block, so no library error ends up as `__context__`.
    """
    try:
        return work(), None
    except Exception as error:
        return None, type(error).__name__


def read_parquet_as_strings(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    """Read a Parquet file as a single all-string part.

    Raises ParquetReadError for a file that cannot be read, has no columns, has a column
    starting with `_`, or has a type that cannot be converted. `config` and `flag` do not apply.
    The error carries no `__context__` or `__cause__`.
    """
    frame, problem = _guarded(lambda: pl.read_parquet(path))
    if frame is None:
        raise _fail(path, problem or "unreadable")
    if frame.width == 0:
        raise _fail(path, "no columns")
    if any(name.startswith("_") for name in frame.columns):
        raise _fail(path, "a column name starts with '_' (reserved for bronze metadata)")
    converted, problem = _guarded(
        lambda: pl.DataFrame([_as_string(series) for series in frame.get_columns()])
    )
    if converted is None:
        raise _fail(path, problem or "unconvertible")
    return [ParsedPart(None, converted.lazy())]
