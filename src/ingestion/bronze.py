"""Bronze write path: one append-only Parquet file per load of a source CSV.

Layout: `<lake_root>/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<suffix>.parquet`.
`<suffix>` is 8 random hex chars so two loads in the same second never collide.
See docs/decisions/0001-bronze-parquet-storage.md.
"""

import argparse
import json
import logging
import os
import re
import secrets
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
    run_ts = run_at.strftime(_RUN_TS_FORMAT)
    return source_dir / partition / f"{file_hash}{run_ts}_{secrets.token_hex(4)}.parquet"


def _load_with_metadata(path: Path, file_hash: str, run_at: datetime) -> pl.DataFrame:
    """Collect right after hashing, then re-hash to prove the file did not change.

    Otherwise a file edited mid-read could leave `_file_hash` not matching the rows.
    """
    lazy = add_bronze_metadata(read_csv_as_strings(path), path.name, file_hash, run_at)
    frame = lazy.collect()
    if hash_file(path) != file_hash:
        raise RuntimeError(f"file changed during ingestion: {path.name}")
    return frame


def _quote(value: object) -> str:
    return str(value) if isinstance(value, int) else json.dumps(str(value))


def _log(event: str, **fields: object) -> None:
    # json.dumps quotes values so spaces or newlines in a file name cannot break key=value.
    pairs = (f"{key}={_quote(value)}" for key, value in fields.items())
    logger.info("%s", " ".join([f"event={event}", *pairs]))


def _elapsed(started: float) -> str:
    return f"{time.monotonic() - started:.3f}"


def _publish_new_file(frame: pl.DataFrame, target: Path) -> None:
    """Write to a temp file, then link it to `target`; fails if `target` exists.

    A crash leaves only a `.tmp` file, which the skip check and readers ignore.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f"{target.name}.tmp")
    try:
        frame.write_parquet(temp)
        os.link(temp, target)  # no-replace: raises FileExistsError
    finally:
        temp.unlink(missing_ok=True)


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
        _log(
            "bronze_skip",
            source=source,
            file_name=path.name,
            file_hash=file_hash,
            duration_s=_elapsed(started),
        )
        return IngestResult(path.name, file_hash, 0, skipped=True)
    frame = _load_with_metadata(path, file_hash, run_at)
    if frame.height == 0:
        _log(
            "bronze_empty",
            source=source,
            file_name=path.name,
            file_hash=file_hash,
            rows_ingested=0,
            duration_s=_elapsed(started),
        )
        return IngestResult(path.name, file_hash, 0, skipped=False)
    target = _target_path(source_dir, file_hash, run_at)
    _publish_new_file(frame, target)
    _log(
        "bronze_write",
        source=source,
        file_name=path.name,
        file_hash=file_hash,
        rows_ingested=frame.height,
        duration_s=_elapsed(started),
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
    """Ingest every CSV under `<landing_root>/<source>/`; return how many files failed.

    One bad file does not stop the rest. Failures are logged with the error type and
    file name only (messages can carry values, so they are not logged).
    Raises FileNotFoundError if the landing folder is missing.
    """
    _validate_source(source)
    folder = landing_root / source
    if not folder.is_dir():
        raise FileNotFoundError(f"landing folder not found: {folder}")
    files = sorted(folder.glob("*.csv"))
    if not files:
        logger.warning("event=bronze_no_files source=%s", json.dumps(source))
        return 0
    failed = 0
    written = skipped = 0
    for path in files:
        try:
            result = ingest_csv(path, source, lake_root, force=force)
        except Exception as error:  # noqa: BLE001 - keep going; reported via exit code
            failed += 1
            _log("bronze_error", source=source, file_name=path.name, error=type(error).__name__)
            continue
        skipped += result.skipped
        written += not result.skipped
    _log(
        "bronze_run",
        source=source,
        files=len(files),
        written=written,
        skipped=skipped,
        failed=failed,
    )
    return failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingestion.bronze", description=__doc__)
    parser.add_argument("source", help="source name; reads data/landing/<source>/*.csv")
    parser.add_argument("--force", action="store_true", help="reload files already ingested")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        failed = ingest_landing(args.source, LANDING_ROOT, LAKE_ROOT, force=args.force)
    except FileNotFoundError as error:
        logger.error("event=bronze_error error=%s", json.dumps(str(error)))
        return 2
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
