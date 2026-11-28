"""A deterministic damaged Parquet file shared by tests. Synthetic data only."""

import io
import struct

import polars as pl


def panicking_parquet() -> bytes:
    """A small Parquet file with one footer byte changed so that Polars panics reading it."""
    buffer = io.BytesIO()
    pl.DataFrame({"id": ["1", "2"], "code": ["a", "b"]}).write_parquet(buffer)
    data = bytearray(buffer.getvalue())
    footer_length = struct.unpack("<I", data[-8:-4])[0]
    data[len(data) - 8 - footer_length + 79] = 1
    return bytes(data)
