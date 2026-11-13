"""What a real run would do with one file, without writing anything: the `--pending` check.

`file_outcome` runs the same steps `ingest_file` runs before the write (detection, the readable
check, then the reader and `with_metadata` through `_load_with_metadata`) and drops the frame.
Because the code is shared, `--pending` and a real run cannot disagree on a file.

Cost: every pending file is parsed in full and held in memory while it is checked, so `--pending`
is as heavy as a real run minus the write. Nothing is written under the lake or raw zone.

Outcomes: `ready` (a run would load it), a held reason (`no_reader`, `needs_config`, `encoding`)
or `failed` with the error type only (never a message, which can hold file content).
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ingestion import bronze
from ingestion.detect import FileFormat, detect_format
from ingestion.source_config import SourceConfig

REASON_FAILED = "failed"


@dataclass(frozen=True)
class Outcome:
    """`reason` is a REASON_* value; `error` is the exception type name when it is `failed`."""

    reason: str
    error: str | None = None


def file_outcome(
    path: Path,
    file_hash: str,
    config: SourceConfig,
    delimiter: str | None,
    file_format: FileFormat | None = None,
) -> Outcome:
    """Return what a real run would do with `path` (already hashed). Never raises, never writes."""
    problem = ""
    try:
        file_format = bronze._check_readable(
            path, file_format or detect_format(path, config), config, delimiter
        )
        run_at = datetime.now(UTC).replace(microsecond=0)
        bronze._load_with_metadata(
            path, file_hash, run_at, config, delimiter, file_format, path.name
        )
    except bronze.FileHeld as held:
        return Outcome(held.reason)
    except Exception as error:  # noqa: BLE001 - reported by type, like a real run's bronze_error
        problem = type(error).__name__
    if problem:
        return Outcome(REASON_FAILED, problem)
    return Outcome(bronze.REASON_READY)


def log_pending(source: str, name: str, file_hash: str, outcome: Outcome, **extra: object) -> None:
    """Write one `bronze_pending` line (the `error` key only when the file would fail)."""
    fields: dict[str, object] = {"source": source, "file_name": name, **extra}
    fields.update(file_hash=file_hash, reason=outcome.reason)
    if outcome.error is not None:
        fields["error"] = outcome.error
    bronze._log("bronze_pending", **fields)
