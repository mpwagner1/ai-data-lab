"""Bronze for archives: every member of a zip or gzip is its own unit (ADR 0002, Archives).

Each member goes through the normal `ingest_file` path (skip by member hash, detection, readers,
re-hash check, atomic write) with `_file_name = "<archive>!<member>"`. Member names are untrusted
data: they are stored in `_file_name` but never logged; logs carry the archive name, the 1-based
member index and the member hash.

A member that is held or fails does not stop the others. An archive-level failure (corrupt,
encrypted, over a cap) fails the archive; members finished before it stay, since each is its own
atomic unit, and none after it is written.
"""

import logging
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ingestion import bronze
from ingestion.archives import (
    MEMBER_SEPARATOR,
    ArchiveCounts,
    ArchiveReadError,
    ArchiveUnit,
    expand_archive,
)
from ingestion.detect import FileFormat, detect_format
from ingestion.source_config import SourceConfig

logger = logging.getLogger(__name__)

ARCHIVE_FORMATS = frozenset({FileFormat.ZIP, FileFormat.GZIP})
REASON_ARCHIVE_ERROR = "archive_error"


@dataclass
class ArchiveOutcome:
    """Counts for one archive."""

    written: int = 0
    skipped: int = 0
    pending: int = 0
    failed: int = 0


def _log_ignored(source: str, archive: Path, counts: ArchiveCounts) -> None:
    if counts.ignored:
        bronze._log(
            "bronze_ignored",
            source=source,
            file_name=archive.name,
            members=counts.ignored,
            symlinks=counts.symlinks,
        )


def _ingest_unit(
    unit: ArchiveUnit,
    archive: Path,
    source: str,
    lake_root: Path,
    force: bool,
    run_at: datetime,
    config: SourceConfig,
    delimiter: str | None,
    outcome: ArchiveOutcome,
) -> None:
    log_name = f"{archive.name}{MEMBER_SEPARATOR}#{unit.index}"
    try:
        result = bronze.ingest_file(
            unit.path,
            source,
            lake_root,
            force=force,
            run_at=run_at,
            config=config,
            delimiter=delimiter,
            file_hash=unit.file_hash,
            file_name=f"{archive.name}{MEMBER_SEPARATOR}{unit.name}",
            log_name=log_name,
        )
    except bronze.FileHeld as held:
        outcome.pending += 1
        bronze._log(
            "bronze_unparsed",
            source=source,
            file_name=log_name,
            file_hash=unit.file_hash,
            reason=held.reason,
        )
        return
    except Exception as error:  # noqa: BLE001 - one bad member must not stop the others
        outcome.failed += 1
        bronze._log("bronze_error", source=source, file_name=log_name, error=type(error).__name__)
        return
    outcome.skipped += result.skipped
    outcome.written += not result.skipped


def ingest_archive(
    path: Path,
    file_format: FileFormat,
    source: str,
    lake_root: Path,
    force: bool,
    run_at: datetime,
    config: SourceConfig,
    delimiter: str | None,
) -> ArchiveOutcome:
    """Load every member of an archive that was already received into the raw zone."""
    outcome = ArchiveOutcome()
    counts = ArchiveCounts()
    units = expand_archive(path, file_format is FileFormat.GZIP, counts)
    try:
        with closing(units):
            for unit in units:
                _ingest_unit(
                    unit, path, source, lake_root, force, run_at, config, delimiter, outcome
                )
    except ArchiveReadError as error:
        outcome.failed += 1
        bronze._log("bronze_error", source=source, file_name=path.name, error=type(error).__name__)
    _log_ignored(source, path, counts)
    return outcome


def _member_reason(unit: ArchiveUnit, config: SourceConfig, delimiter: str | None) -> str:
    try:
        bronze._check_readable(unit.path, detect_format(unit.path, config), config, delimiter)
    except bronze.FileHeld as held:
        return held.reason
    return bronze.REASON_READY


def list_pending_members(
    path: Path,
    file_format: FileFormat,
    source: str,
    source_dir: Path,
    config: SourceConfig,
    delimiter: str | None,
) -> int:
    """Log `bronze_pending` for each member with no bronze file yet; return how many. Read-only.

    Members are extracted to a private temporary directory outside the lake (and removed), so
    nothing is written to the lake. An archive that cannot be read counts as one pending item with
    `reason=archive_error` (it still has no bronze rows).
    """
    count = 0
    counts = ArchiveCounts()
    units = expand_archive(path, file_format is FileFormat.GZIP, counts)
    try:
        with closing(units):
            for unit in units:
                if bronze._already_ingested(source_dir, unit.file_hash):
                    continue
                count += 1
                bronze._log(
                    "bronze_pending",
                    source=source,
                    file_name=path.name,
                    member=unit.index,
                    file_hash=unit.file_hash,
                    reason=_member_reason(unit, config, delimiter),
                )
    except ArchiveReadError:
        count += 1
        bronze._log(
            "bronze_pending", source=source, file_name=path.name, reason=REASON_ARCHIVE_ERROR
        )
    _log_ignored(source, path, counts)
    return count
