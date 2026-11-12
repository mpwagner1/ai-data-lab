"""Read Parquet files for bronze with Polars, converting every column to `pl.String`.

Parquet is the one source format with its own types, so here string conversion is a real
conversion (not just keeping text). Nothing is trimmed or reformatted beyond the string form
Polars (2.1) gives; the original file stays in the raw zone. Observed forms:

- integers: plain digits (`42`). Booleans: `true` / `false`.
- floats: Polars' shortest round-trip text (`1.0`, `0.1`, `1e+20`, `NaN`).
- decimals: exact digits at the column scale (`1.50`).
- dates: `YYYY-MM-DD`. Times: `HH:MM:SS`.
- timestamps: `YYYY-MM-DD HH:MM:SS.ffffff` (microseconds; a time zone adds `+00:00`).
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

from pathlib import Path

import polars as pl

from ingestion.parsed import ParsedPart
from ingestion.source_config import SourceConfig

_NESTED = (pl.Struct, pl.List, pl.Array)
_WRAP_PREFIX = len('{"v":')


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


def _nested_to_json(series: pl.Series) -> pl.Series:
    """JSON text per value, null where the value is null."""
    wrapped = series.alias("v").to_frame().select(pl.struct("v").struct.json_encode().alias("v"))
    text = wrapped.to_series().str.slice(_WRAP_PREFIX).str.head(-1)
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
    return series.cast(pl.String)


def _load(path: Path) -> pl.DataFrame:
    try:
        return pl.read_parquet(path)
    except Exception as error:
        raise _fail(path, type(error).__name__) from None


def read_parquet_as_strings(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    """Read a Parquet file as a single all-string part.

    Raises ParquetReadError for a file that cannot be read, has no columns, has a column
    starting with `_`, or has a type that cannot be converted. `config` and `flag` do not apply.
    """
    frame = _load(path)
    if frame.width == 0:
        raise _fail(path, "no columns")
    if any(name.startswith("_") for name in frame.columns):
        raise _fail(path, "a column name starts with '_' (reserved for bronze metadata)")
    try:
        converted = pl.DataFrame([_as_string(series) for series in frame.get_columns()])
    except Exception as error:
        raise _fail(path, type(error).__name__) from None
    return [ParsedPart(None, converted.lazy())]
