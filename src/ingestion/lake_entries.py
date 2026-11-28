"""Checks on entries already in the lake, before anything relies on them.

An entry is found by its name, which says nothing about what is behind it. Every matching entry
is checked; one bad entry raises, even when another match is good. Nothing is changed here: an
operator repairs a bad entry (docs/BRONZE.md section 5).
"""

from collections.abc import Callable, Iterable
from pathlib import Path

import polars as pl

from ingestion.metadata import hash_file

PROBLEM_SYMLINK = "symlink"
PROBLEM_NOT_FILE = "not_a_regular_file"
PROBLEM_FOOTER = "unreadable_parquet_footer"
PROBLEM_HASH = "hash_mismatch"

# Some damaged inputs make Polars panic. This is not an `Exception`, so `except Exception` misses
# it: every handler around a Polars read of untrusted bytes must name it.
POLARS_PANIC = pl.exceptions.PanicException


class BadLakeEntryError(RuntimeError):
    """A lake entry for a file hash cannot be used.

    Holds only the zone, the hash, `<partition>/<entry name>:<problem>` labels and a count, so
    every field is safe to log (no row data and no path outside the lake).
    """

    def __init__(self, zone: str, file_hash: str, bad: list[str], usable: int) -> None:
        super().__init__(f"unusable {zone} entry for hash {file_hash}")
        self.zone = zone
        self.file_hash = file_hash
        self.bad = bad
        self.usable = usable

    def log_fields(self) -> dict[str, object]:
        return {
            "zone": self.zone,
            "file_hash": self.file_hash,
            "bad_entries": ",".join(self.bad),
            "usable_entries": self.usable,
        }


def error_fields(error: Exception) -> dict[str, object]:
    """Log fields for a failed item: the error type, plus the entry details we built ourselves."""
    fields: dict[str, object] = {"error": type(error).__name__}
    if isinstance(error, BadLakeEntryError):
        fields.update(error.log_fields())
    return fields


def _kind_problem(path: Path) -> str | None:
    if path.is_symlink():  # first: `is_file` follows links
        return PROBLEM_SYMLINK
    if not path.is_file():
        return PROBLEM_NOT_FILE
    return None


def parquet_problem(path: Path) -> str | None:
    """Why `path` is not a usable bronze file, or None. Reads the footer, not the data."""
    problem = _kind_problem(path)
    if problem is not None:
        return problem
    with path.open("rb") as handle:
        try:
            pl.read_parquet_schema(handle)
        except (pl.exceptions.PolarsError, POLARS_PANIC):
            return PROBLEM_FOOTER
    return None


def copy_problem(path: Path, file_hash: str) -> str | None:
    """Why `path` is not an exact copy of the bytes with `file_hash`, or None."""
    problem = _kind_problem(path)
    if problem is not None:
        return problem
    return None if hash_file(path) == file_hash else PROBLEM_HASH


def usable_entries(
    zone: str,
    file_hash: str,
    matches: Iterable[Path],
    problem_of: Callable[[Path], str | None],
) -> list[Path]:
    """Return the matching entries, all checked; raise BadLakeEntryError if any one is bad."""
    usable: list[Path] = []
    bad: list[str] = []
    for path in sorted(matches):
        problem = problem_of(path)
        if problem is None:
            usable.append(path)
            continue
        bad.append(f"{path.parent.name}/{path.name}:{problem}")
    if bad:
        raise BadLakeEntryError(zone, file_hash, bad, len(usable))
    return usable
