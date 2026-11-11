"""Bronze write path: one append-only Parquet file per load of a source CSV.

Layout: `<lake_root>/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>.parquet`.
See docs/decisions/0001-bronze-parquet-storage.md.
"""

import argparse
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from ingestion.csv_reader import read_csv_as_strings
from ingestion.metadata import add_bronze_metadata, hash_file

logger = logging.getLogger(__name__)

_RUN_TS_FORMAT = "%Y%m%dT%H%M%SZ"
_SOURCE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
LANDING_ROOT = Path("data/landing")
LAKE_ROOT = Path("lake")


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one `ingest_csv` call."""

    file_name: str
    file_hash: str
    rows: int
    skipped: bool


def _validate_source(source: str) -> None:
    # The source becomes a directory name, so keep it from escaping the lake.
    if not _SOURCE_PATTERN.fullmatch(source):
        raise ValueError(f"invalid source name: {source!r}")


def _source_dir(lake_root: Path, source: str) -> Path:
    return lake_root / "bronze" / source


def _already_ingested(source_dir: Path, file_hash: str) -> bool:
    return any(source_dir.glob(f"ingest_date=*/{file_hash}*.parquet"))


def _target_path(source_dir: Path, file_hash: str, run_at: datetime) -> Path:
    partition = f"ingest_date={run_at:%Y-%m-%d}"
    return source_dir / partition / f"{file_hash}{run_at.strftime(_RUN_TS_FORMAT)}.parquet"


def _load_with_metadata(path: Path, run_at: datetime) -> tuple[pl.DataFrame, str]:
    """Hash and collect back to back, then re-hash to prove the file did not change.

    Otherwise a file edited mid-read could leave `_file_hash` not matching the rows.
    """
    file_hash = hash_file(path)
    lazy = add_bronze_metadata(read_csv_as_strings(path), path.name, file_hash, run_at)
    frame = lazy.collect()
    if hash_file(path) != file_hash:
        raise RuntimeError(f"file changed during ingestion: {path.name}")
    return frame, file_hash


def _log(event: str, **fields: object) -> None:
    logger.info("%s", " ".join([f"event={event}", *(f"{k}={v}" for k, v in fields.items())]))


def ingest_csv(
    path: Path,
    source: str,
    lake_root: Path,
    force: bool = False,
    run_at: datetime | None = None,
) -> IngestResult:
    """Load one CSV into bronze; skip it if its hash was already loaded unless `force`.

    `run_at` (UTC) is injectable for tests; it defaults to now. Existing files are never
    overwritten: a name collision raises FileExistsError.
    """
    _validate_source(source)
    started = time.monotonic()
    run_at = (run_at or datetime.now(UTC)).replace(microsecond=0)
    source_dir = _source_dir(lake_root, source)
    file_hash = hash_file(path)
    if not force and _already_ingested(source_dir, file_hash):
        _log("bronze_skip", source=source, file_name=path.name, file_hash=file_hash)
        return IngestResult(path.name, file_hash, 0, skipped=True)
    frame, loaded_hash = _load_with_metadata(path, run_at)
    if loaded_hash != file_hash:
        raise RuntimeError(f"file changed during ingestion: {path.name}")
    if frame.height == 0:
        _log(
            "bronze_empty", source=source, file_name=path.name, file_hash=file_hash, rows_ingested=0
        )
        return IngestResult(path.name, file_hash, 0, skipped=False)
    target = _target_path(source_dir, file_hash, run_at)
    if target.exists():
        raise FileExistsError(f"bronze file already exists: {target.name}")
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(target)
    _log(
        "bronze_write",
        source=source,
        file_name=path.name,
        file_hash=file_hash,
        rows_ingested=frame.height,
        duration_s=f"{time.monotonic() - started:.3f}",
    )
    return IngestResult(path.name, file_hash, frame.height, skipped=False)


def read_bronze(source: str, lake_root: Path) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation over every bronze file of a source (missing columns are null)."""
    _validate_source(source)
    pattern = str(_source_dir(lake_root, source) / "**" / "*.parquet")
    return duckdb.sql(
        "select * from read_parquet(?, hive_partitioning = true, union_by_name = true)",
        params=[pattern],
    )


def ingest_landing(source: str, landing_root: Path, lake_root: Path, force: bool) -> int:
    """Ingest every CSV under `<landing_root>/<source>/`; return how many were written."""
    _validate_source(source)
    files = sorted((landing_root / source).glob("*.csv"))
    results = [ingest_csv(path, source, lake_root, force=force) for path in files]
    return sum(1 for result in results if not result.skipped)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingestion.bronze", description=__doc__)
    parser.add_argument("source", help="source name; reads data/landing/<source>/*.csv")
    parser.add_argument("--force", action="store_true", help="reload files already ingested")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ingest_landing(args.source, LANDING_ROOT, LAKE_ROOT, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
