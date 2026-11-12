"""Bronze write path: every landing file is kept in the raw zone, then parsed if we can.

Raw zone: `<lake_root>/raw/<source>/receive_date=YYYY-MM-DD/<file_hash><ext>`, the exact bytes.
Bronze: `<lake_root>/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<suffix>.parquet`.
`<suffix>` is 8 random hex chars so two loads in the same second never collide.
A file with no reader, missing config or an unusable encoding is held (pending): it stays in the
raw zone, `bronze_unparsed` is logged and the run exits 3. Pending is derived: no bronze file
starts with the file's hash. See docs/decisions/0001-bronze-parquet-storage.md and 0002.
"""

import json
import logging
import secrets
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from ingestion.csv_reader import DEFAULT_SEPARATOR, read_csv_as_strings
from ingestion.detect import FileFormat, detect_format, validate_format
from ingestion.excel_reader import read_workbook
from ingestion.json_reader import read_json, read_ndjson
from ingestion.metadata import hash_file, with_metadata
from ingestion.parquet_reader import read_parquet_as_strings
from ingestion.parsed import ParsedPart
from ingestion.raw_zone import SOURCE_PATTERN, landing_files, publish, receive_raw
from ingestion.source_config import (
    CONFIG_ROOT,
    SourceConfig,
    load_source_config,
)

logger = logging.getLogger(__name__)

_RUN_TS_FORMAT = "%Y%m%dT%H%M%SZ"
LANDING_ROOT = Path("data/landing")
LAKE_ROOT = Path("lake")
# Delimiter implied by the extension. `.txt` and `.dat` have none: they need config or --delimiter.
_EXTENSION_DELIMITERS = {".csv": DEFAULT_SEPARATOR, ".tsv": "\t", ".psv": "|"}
EXIT_HELD = 3

REASON_NO_READER = "no_reader"
REASON_NEEDS_CONFIG = "needs_config"
REASON_ENCODING = "encoding"
REASON_READY = "ready"


class MissingDelimiterError(ValueError):
    """A delimited file has no delimiter from the flag, config or extension."""


class FileHeld(ValueError):
    """The file is kept but cannot be parsed yet. `reason` is one of the REASON_* values."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


Reader = Callable[[Path, SourceConfig, str | None], list[ParsedPart]]


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one `ingest_file` call."""

    file_name: str
    file_hash: str
    rows: int
    skipped: bool


@dataclass(frozen=True)
class RunSummary:
    """Outcome of one `ingest_landing` run."""

    failed: int
    pending: int


def _validate_source(source: str) -> None:
    # The source becomes a directory name, so keep it from escaping the lake.
    if not SOURCE_PATTERN.fullmatch(source):
        raise ValueError(f"invalid source name: {source!r}")


def _source_dir(lake_root: Path, source: str) -> Path:
    return lake_root / "bronze" / source


def _already_ingested(source_dir: Path, file_hash: str) -> bool:
    return any(source_dir.glob(f"ingest_date=*/{file_hash}*.parquet"))


def _target_path(source_dir: Path, file_hash: str, run_at: datetime) -> Path:
    partition = f"ingest_date={run_at:%Y-%m-%d}"
    run_ts = run_at.strftime(_RUN_TS_FORMAT)
    return source_dir / partition / f"{file_hash}{run_ts}_{secrets.token_hex(4)}.parquet"


def resolve_delimiter(path: Path, config: SourceConfig, flag: str | None) -> str:
    """Pick the delimiter: the flag (no-default extensions only), then config, then extension."""
    extension = path.suffix.lower()
    if flag is not None and extension not in _EXTENSION_DELIMITERS:
        return flag
    if config.delimiter is not None:
        return config.delimiter
    default = _EXTENSION_DELIMITERS.get(extension)
    if default is None:
        raise MissingDelimiterError(
            f"no delimiter for {path.name}: set `delimiter` in config or --delimiter"
        )
    return default


def _read_delimited(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]:
    separator = resolve_delimiter(path, config, flag)
    frame = read_csv_as_strings(path, separator=separator, encoding=config.encoding)
    return [ParsedPart(None, frame)]


READERS: dict[FileFormat, Reader] = {
    FileFormat.DELIMITED: _read_delimited,
    FileFormat.XLSX: read_workbook,
    FileFormat.XLS: read_workbook,
    FileFormat.ODS: read_workbook,
    FileFormat.JSON: read_json,
    FileFormat.NDJSON: read_ndjson,
    FileFormat.PARQUET: read_parquet_as_strings,
}


def _check_readable(
    path: Path, file_format: FileFormat | None, config: SourceConfig, flag: str | None
) -> FileFormat:
    """Return the format to parse with, or raise FileHeld saying why the file must wait."""
    if file_format is None or file_format not in READERS:
        raise FileHeld(REASON_NO_READER, f"no reader for {path.name}")
    if file_format is FileFormat.DELIMITED:
        try:
            resolve_delimiter(path, config, flag)
        except MissingDelimiterError as error:
            raise FileHeld(REASON_NEEDS_CONFIG, str(error)) from None
    return file_format


def _load_with_metadata(
    path: Path,
    file_hash: str,
    run_at: datetime,
    config: SourceConfig,
    flag: str | None,
    file_format: FileFormat,
    file_name: str,
) -> pl.DataFrame:
    """Collect right after hashing, then re-hash to prove the file did not change.

    Otherwise a file edited mid-read could leave `_file_hash` not matching the rows.
    """
    try:
        parts = READERS[file_format](path, config, flag)
        frame = with_metadata(parts, file_name, file_hash, run_at)
    except UnicodeError:
        raise FileHeld(REASON_ENCODING, f"cannot decode {path.name}") from None
    if hash_file(path) != file_hash:
        raise RuntimeError(f"file changed during ingestion: {path.name}")
    return frame


def _quote(value: object) -> str:
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(str(value))


def _log(event: str, **fields: object) -> None:
    # json.dumps quotes values so spaces or newlines in a file name cannot break key=value.
    pairs = (f"{key}={_quote(value)}" for key, value in fields.items())
    logger.info("%s", " ".join([f"event={event}", *pairs]))


def _elapsed(started: float) -> float:
    return round(time.monotonic() - started, 3)


def _publish_new_file(frame: pl.DataFrame, target: Path) -> None:
    publish(target, frame.write_parquet)


def _warn_stale_temp_files(source: str, source_dir: Path) -> None:
    """Report `.parquet.tmp` files left by a killed run. They are never deleted here."""
    names = sorted(p.name for p in source_dir.glob("ingest_date=*/*.parquet.tmp"))
    if not names:
        return
    logger.warning(
        "event=bronze_stale_temp source=%s count=%d files=%s",
        json.dumps(source),
        len(names),
        json.dumps(names),
    )


def ingest_csv(
    path: Path,
    source: str,
    lake_root: Path,
    force: bool = False,
    run_at: datetime | None = None,
) -> IngestResult:
    """Load one CSV into bronze. Thin wrapper over `ingest_file` with default config."""
    return ingest_file(path, source, lake_root, force=force, run_at=run_at)


def ingest_file(
    path: Path,
    source: str,
    lake_root: Path,
    force: bool = False,
    run_at: datetime | None = None,
    config: SourceConfig | None = None,
    delimiter: str | None = None,
    file_hash: str | None = None,
    file_format: FileFormat | None = None,
    file_name: str | None = None,
    log_name: str | None = None,
) -> IngestResult:
    """Load one file into bronze; skip it if its hash was already loaded unless `force`.

    A header-only file is written as a zero-row Parquet file so later runs skip it too.

    `config` holds per-source settings (defaults if None). `delimiter` is the --delimiter flag
    value; it only applies to files with no extension default (`.txt`, `.dat`).

    The format comes from `file_format`, else detection (config, signature, text). A file that
    cannot be parsed yet raises `FileHeld` (a ValueError) with a `reason`. `file_hash` saves a
    second hashing pass when the caller already has it.

    `file_name` is the stored `_file_name` (default: the file's name). Archive members pass
    `<archive>!<member>` here and a member-free `log_name` (default: the file's name) for logs,
    because member names are untrusted and never logged.

    `run_at` (UTC) is injectable for tests; it defaults to now. Existing files are never
    overwritten: a name collision raises FileExistsError.
    """
    _validate_source(source)
    started = time.monotonic()
    log_name = log_name or path.name
    file_name = file_name or path.name
    run_at = (run_at or datetime.now(UTC)).replace(microsecond=0)
    source_dir = _source_dir(lake_root, source)
    config = config or SourceConfig()
    file_hash = file_hash or hash_file(path)
    if not force and _already_ingested(source_dir, file_hash):
        _log(
            "bronze_skip",
            source=source,
            file_name=log_name,
            file_hash=file_hash,
            duration_s=_elapsed(started),
        )
        return IngestResult(log_name, file_hash, 0, skipped=True)
    file_format = _check_readable(
        path, file_format or detect_format(path, config), config, delimiter
    )
    frame = _load_with_metadata(path, file_hash, run_at, config, delimiter, file_format, file_name)
    target = _target_path(source_dir, file_hash, run_at)
    _publish_new_file(frame, target)
    _log(
        "bronze_write",
        source=source,
        file_name=log_name,
        file_hash=file_hash,
        rows_ingested=frame.height,
        duration_s=_elapsed(started),
    )
    return IngestResult(log_name, file_hash, frame.height, skipped=False)


def read_bronze(source: str, lake_root: Path) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation over every bronze file of a source (missing columns are null)."""
    _validate_source(source)
    pattern = str(_source_dir(lake_root, source) / "**" / "*.parquet")
    return duckdb.sql(
        "select * from read_parquet(?, hive_partitioning = true, union_by_name = true)",
        params=[pattern],
    )


def _prepare(
    source: str, landing_root: Path, config_root: Path | None
) -> tuple[Path, SourceConfig]:
    _validate_source(source)
    folder = landing_root / source
    if not folder.is_dir():
        raise FileNotFoundError(f"landing folder not found: {folder}")
    config = load_source_config(source, config_root or CONFIG_ROOT)
    if config.format is not None:
        validate_format(config.format)  # a config error: fail before anything is copied
    return folder, config


def ingest_landing(
    source: str,
    landing_root: Path,
    lake_root: Path,
    force: bool,
    delimiter: str | None = None,
    config_root: Path | None = None,
) -> RunSummary:
    """Receive and ingest every file in `<landing_root>/<source>/` (hidden and lock files aside).

    Each file is copied to the raw zone, then parsed. Files that cannot be parsed yet are
    counted as pending (`bronze_unparsed` is logged with a reason). Returns the counts.

    One bad file does not stop the rest. Failures are logged with the error type and
    file name only (messages can carry values, so they are not logged).
    Raises FileNotFoundError if the landing folder is missing.
    """
    from ingestion.archive_ingest import ARCHIVE_FORMATS, ingest_archive  # avoids a cycle

    folder, config = _prepare(source, landing_root, config_root)
    _warn_stale_temp_files(source, _source_dir(lake_root, source))
    files, symlinks = landing_files(folder)
    if symlinks:
        _log("bronze_ignored", source=source, symlinks=symlinks)
    if not files:
        logger.warning("event=bronze_no_files source=%s", json.dumps(source))
        return RunSummary(failed=0, pending=0)
    run_at = datetime.now(UTC).replace(microsecond=0)
    failed = pending = written = skipped = 0
    for path in files:
        try:
            file_hash = hash_file(path)
            is_new = receive_raw(path, source, lake_root, file_hash, run_at)
            _log(
                "bronze_receive",
                source=source,
                file_name=path.name,
                file_hash=file_hash,
                new=int(is_new),
            )
            file_format = detect_format(path, config)
            if file_format in ARCHIVE_FORMATS:
                outcome = ingest_archive(
                    path, file_format, source, lake_root, force, run_at, config, delimiter
                )
                failed += outcome.failed
                pending += outcome.pending
                written += outcome.written
                skipped += outcome.skipped
                continue
            result = ingest_file(
                path,
                source,
                lake_root,
                force=force,
                run_at=run_at,
                config=config,
                delimiter=delimiter,
                file_hash=file_hash,
                file_format=file_format,
            )
        except FileHeld as held:
            pending += 1
            _log(
                "bronze_unparsed",
                source=source,
                file_name=path.name,
                file_hash=file_hash,
                reason=held.reason,
            )
            continue
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
        pending=pending,
        failed=failed,
    )
    return RunSummary(failed=failed, pending=pending)


def list_pending(
    source: str,
    landing_root: Path,
    lake_root: Path,
    delimiter: str | None = None,
    config_root: Path | None = None,
) -> int:
    """Log one `bronze_pending` line per landing file with no bronze rows yet. Writes nothing.

    Lives in `bronze_cli`; imported here on first call to avoid a circular import.
    """
    from ingestion.bronze_cli import list_pending as _list_pending

    return _list_pending(source, landing_root, lake_root, delimiter, config_root)


def main(argv: list[str] | None = None) -> int:
    """Run the command line (see `bronze_cli.main`); kept so `python -m ingestion.bronze` works."""
    from ingestion.bronze_cli import main as _main

    return _main(argv)


if __name__ == "__main__":
    sys.exit(main())
