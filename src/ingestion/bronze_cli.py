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
from ingestion.pending import file_outcome, log_pending
from ingestion.raw_zone import landing_files
from ingestion.source_config import validate_delimiter

logger = logging.getLogger(__name__)


def list_pending(
    source: str,
    landing_root: Path,
    lake_root: Path,
    delimiter: str | None = None,
    config_root: Path | None = None,
) -> int:
    """Log one `bronze_pending` line per landing file with no bronze rows yet. Writes nothing.

    `reason` says why it is held (`no_reader`, `needs_config`, `encoding`), `failed` (with the error
    type) if a real run would fail it, or `ready` if it would load. Every file is parsed through the
    real read path (see `pending.py`), so this costs as much memory as a run. Returns the count.
    """
    folder, config = bronze._prepare(source, landing_root, config_root)
    source_dir = bronze._source_dir(lake_root, source)
    count = 0
    for path in landing_files(folder)[0]:
        file_format = detect_format(path, config)
        if file_format in ARCHIVE_FORMATS:
            count += list_pending_members(path, file_format, source, source_dir, config, delimiter)
            continue
        file_hash = hash_file(path)
        if bronze._already_ingested(source_dir, file_hash):
            continue
        count += 1
        outcome = file_outcome(path, file_hash, config, delimiter, file_format)
        log_pending(source, path.name, file_hash, outcome)
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
    except FileNotFoundError as error:
        # The message carries the landing path; log the type and source only.
        logger.error(
            "event=bronze_error error=%s source=%s", type(error).__name__, json.dumps(args.source)
        )
        return 2
    except ValueError as error:
        # Config errors: log the type and source only, never the message (it may echo config).
        logger.error(
            "event=bronze_error error=%s source=%s", type(error).__name__, json.dumps(args.source)
        )
        return 2
    if summary.failed:
        return 1
    return bronze.EXIT_HELD if summary.pending else 0
