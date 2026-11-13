"""Command line for the bronze write path: `python -m ingestion.bronze <source> ...`.

`bronze.py` holds the write path; this module only parses flags, maps errors to exit codes
and calls it. Exit codes: 0 ok, 1 a file failed, 2 bad source or folder, 3 files held.
"""

import argparse
import json
import logging
from pathlib import Path

from ingestion import bronze
from ingestion.archive_ingest import ARCHIVE_FORMATS, list_pending_members
from ingestion.detect import detect_format
from ingestion.metadata import hash_file
from ingestion.pending import REASON_FAILED, Outcome, log_pending, outcome_once
from ingestion.raw_zone import landing_files
from ingestion.source_config import SourceConfig, validate_delimiter

logger = logging.getLogger(__name__)


def _pending_file(
    path: Path,
    source: str,
    source_dir: Path,
    config: SourceConfig,
    delimiter: str | None,
    seen: dict[str, Outcome],
) -> int:
    """Log what a real run would do with one landing file; return how many items are pending.

    Same order and guard as `bronze.ingest_landing`: hash, then detect, then read. A step that
    raises is `failed` with the error type only (never the message, which can hold the path).
    """
    file_hash: str | None = None
    problem = ""
    try:
        file_hash = hash_file(path)
        if bronze._already_ingested(source_dir, file_hash):
            return 0
        file_format = detect_format(path, config)
        if file_format in ARCHIVE_FORMATS:
            return list_pending_members(
                path, file_format, source, source_dir, config, delimiter, seen
            )
        outcome = outcome_once(seen, path, file_hash, config, delimiter, file_format)
    except Exception as error:  # noqa: BLE001 - reported by type, like a real run's bronze_error
        problem = type(error).__name__
    if problem:
        outcome = Outcome(REASON_FAILED, problem)
    log_pending(source, path.name, file_hash, outcome)
    return 1


def list_pending(
    source: str,
    landing_root: Path,
    lake_root: Path,
    delimiter: str | None = None,
    config_root: Path | None = None,
) -> int:
    """Log one `bronze_pending` line per landing file with no bronze rows yet. Writes nothing.

    `reason` says why it is held (`no_reader`, `needs_config`, `encoding`), `failed` (with the error
    type, and no `file_hash` if the file could not be hashed) if a real run would fail it,
    `skipped` if an earlier file in this call has identical content, or `ready` if it would load.
    Files go in the same sorted order as a real run. Every file is parsed through the real read
    path (see `pending.py`), so this costs as much memory as a run. Returns the count.
    """
    folder, config = bronze._prepare(source, landing_root, config_root)
    source_dir = bronze._source_dir(lake_root, source)
    seen: dict[str, Outcome] = {}
    count = 0
    for path in landing_files(folder)[0]:
        count += _pending_file(path, source, source_dir, config, delimiter, seen)
    bronze._log("bronze_pending_run", source=source, pending=count)
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ingestion.bronze",
        description=bronze.__doc__,
        epilog="exit codes: 0 ok, 1 a file failed, 2 bad source or folder, 3 files held (pending)",
    )
    parser.add_argument("source", help="source name; reads files in data/landing/<source>/")
    parser.add_argument(
        "--pending",
        action="store_true",
        help="list files with no bronze rows yet and why (writes nothing); exit 3 if any",
    )
    parser.add_argument("--force", action="store_true", help="reload files already ingested")
    parser.add_argument(
        "--delimiter",
        help="single character for .txt/.dat files; beats config, never changes .csv/.tsv/.psv",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.delimiter is not None:
        try:
            validate_delimiter(args.delimiter)
        except ValueError:
            logger.error("event=bronze_error error=InvalidDelimiter")
            return 2
    try:
        bronze._validate_source(args.source)
    except ValueError as error:
        logger.error(
            "event=bronze_error error=%s source=%s",
            type(error).__name__,
            json.dumps(args.source),
        )
        return 2
    try:
        if args.pending:
            count = list_pending(
                args.source, bronze.LANDING_ROOT, bronze.LAKE_ROOT, delimiter=args.delimiter
            )
            return bronze.EXIT_HELD if count else 0
        summary = bronze.ingest_landing(
            args.source,
            bronze.LANDING_ROOT,
            bronze.LAKE_ROOT,
            force=args.force,
            delimiter=args.delimiter,
        )
    except (OSError, ValueError) as error:
        # OSError covers a missing, unlistable or non-directory landing folder (FileNotFoundError,
        # PermissionError, NotADirectoryError...). Its message carries the landing path and a
        # config error's may echo config; log the type and source only. Per-file errors never
        # get here: every file is guarded by its own `except Exception` (see bronze.py).
        logger.error(
            "event=bronze_error error=%s source=%s", type(error).__name__, json.dumps(args.source)
        )
        return 2
    if summary.failed:
        return 1
    return bronze.EXIT_HELD if summary.pending else 0
