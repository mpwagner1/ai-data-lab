"""Stage test files from an Azure Blob container into the landing folder (read-only).

Command: `python -m ingestion.azure_stage <source> <prefix>`. It downloads the blobs directly
under `<prefix>` (no recursion) into `data/landing/<source>/<name after prefix>`; bronze then runs
unchanged on that folder. See docs/decisions/0003-azure-blob-source.md.

Settings: env vars `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_CONTAINER` only. Credential:
`AzureCliCredential` only (the `az login` session). `DefaultAzureCredential` is deliberately not
used: its chain can fall back to another identity (environment, managed identity, editor). The
credential and the client are closed after the run. Only list and download calls are made; nothing
is ever uploaded, deleted, re-tiered or leased.

Rules: blob names are untrusted (no `..`, control characters, separators beyond the prefix, empty
names, a lone `.`, or the reserved `.tmp` suffix compared case-insensitively; names must start
with the prefix); folder markers and sub-prefixes are ignored and counted; an
existing local file is never overwritten (same bytes: skipped, different bytes: that blob fails
with `NameCollisionError`); caps (default 256 MiB per blob, 100 blobs) are checked on the blob
properties and again on the bytes received. A rejected or failed blob does not stop the rest.
More than `--max-blobs` blobs under the prefix counts as one extra failure, so it is never silent.
`--max-total-mib` (default 2048, 2 GiB) caps every byte received in the run, staged and
identical-skipped alike. Before a blob, if received-so-far plus its listed size would exceed the
cap, staging stops there (that blob and later ones are not downloaded), `azure_stage_error` is
logged with `error="TotalSizeCap"`, and one extra failure is counted so the run exits 1. The
streamed bytes are checked too. Blobs already staged stay.

Exit codes: 0 done or nothing to stage, 1 a blob failed, 2 setup, sign-in or access error
(only `AzureError` and `OSError` from the factory or the listing; other errors propagate).

Test seam: everything talks to a `BlobContainer` (two methods: `list_blobs`, `read_blob`). Tests
pass a fake to `stage(...)`, or a `container_factory` and `landing_root` to `main(...)`, so no
network or credential is touched. The real adapter is `AzureContainer`.

Logs are `key=value` only: counts, blob names, sizes, SHA-256 hashes, durations and error types.
Never tokens, URLs, connection strings, the account or container values, or blob content. Error
messages from libraries are never logged (they can carry URLs or details).
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from azure.core.exceptions import AzureError
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobPrefix, BlobServiceClient, ContainerClient

from ingestion import bronze
from ingestion.metadata import hash_file
from ingestion.raw_zone import publish

logger = logging.getLogger(__name__)

ENV_ACCOUNT = "AZURE_STORAGE_ACCOUNT"
ENV_CONTAINER = "AZURE_STORAGE_CONTAINER"
DEFAULT_MAX_BLOB_MIB = 256
DEFAULT_MAX_BLOBS = 100
DEFAULT_MAX_TOTAL_MIB = 2048
_MIB = 1024 * 1024
_ACCOUNT_PATTERN = re.compile(r"[a-z0-9]{3,24}")
_CONTAINER_PATTERN = re.compile(r"(?!.*--)[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")  # 3-63, no "--"
_RESERVED_SUFFIX = ".tmp"  # the temp name `publish` uses; a blob with it could clobber a temp file
_EVENT_LEVELS = {"azure_stage_error": logging.ERROR, "azure_stage_rejected": logging.WARNING}


class NameCollisionError(Exception):
    """A different local file already has this name; nothing was replaced."""


class BlobTooLargeError(ValueError):
    """The blob is larger than the per-blob cap."""


class TotalSizeCapError(ValueError):
    """The bytes received in this run passed the total cap."""


class RejectedNameError(ValueError):
    """The blob name is unsafe to use as a local file name."""


@dataclass(frozen=True)
class BlobInfo:
    """One listing entry: a blob (with its size in bytes) or a sub-prefix (`is_prefix`)."""

    name: str
    size: int = 0
    is_prefix: bool = False


class BlobContainer(Protocol):
    """The only two operations this module needs from a container (the test seam)."""

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        """Yield blobs and sub-prefixes directly under `prefix` (non-recursive)."""
        ...

    def read_blob(self, name: str) -> Iterator[bytes]:
        """Yield the blob's bytes in chunks."""
        ...


ContainerFactory = Callable[[str, str], BlobContainer]


@dataclass(frozen=True)
class StageSummary:
    """Counts for one run."""

    staged: int = 0
    skipped: int = 0
    ignored: int = 0
    failed: int = 0


class AzureContainer:
    """Real adapter over `ContainerClient`. Read-only: `walk_blobs` and `download_blob` only."""

    def __init__(
        self, client: ContainerClient, closers: tuple[Callable[[], None], ...] = ()
    ) -> None:
        self._client = client
        self._closers = closers

    def close(self) -> None:
        """Close the client and the credential (the callables given at build time)."""
        for closer in self._closers:
            closer()

    def list_blobs(self, prefix: str) -> Iterator[BlobInfo]:
        for item in self._client.walk_blobs(name_starts_with=prefix, delimiter="/"):
            if isinstance(item, BlobPrefix):
                yield BlobInfo(item.name, is_prefix=True)
            else:
                yield BlobInfo(item.name, size=item.size or 0)

    def read_blob(self, name: str) -> Iterator[bytes]:
        yield from self._client.download_blob(name).chunks()


def open_container(account: str, container: str) -> BlobContainer:
    """Build the real container client from the signed-in `AzureCliCredential`.

    The caller closes the result (`AzureContainer.close`) to release the client and credential.
    """
    url = f"https://{account}.blob.core.windows.net"
    credential = AzureCliCredential()
    service = BlobServiceClient(url, credential=credential)
    closers = (service.close, credential.close)
    return AzureContainer(service.get_container_client(container), closers)


def _quote(value: object) -> str:
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(str(value))  # quoting keeps odd blob names from breaking key=value


def _log(event: str, **fields: object) -> None:
    pairs = (f"{key}={_quote(value)}" for key, value in fields.items())
    logger.log(_EVENT_LEVELS.get(event, logging.INFO), "%s", " ".join([f"event={event}", *pairs]))


def normalize_prefix(prefix: str) -> str:
    """Return the prefix with a trailing slash; raise ValueError if it is unsafe or empty."""
    cleaned = prefix if prefix.endswith("/") else f"{prefix}/"
    unsafe = (
        cleaned == "/"
        or cleaned.startswith("/")
        or "\\" in cleaned
        or ".." in cleaned
        or _has_control_char(cleaned)
    )
    if unsafe:
        raise ValueError("invalid prefix")
    return cleaned


def _has_control_char(text: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in text)


def local_name(blob_name: str, prefix: str) -> str:
    """Return the file name after the prefix, or raise RejectedNameError."""
    if not blob_name.startswith(prefix):
        raise RejectedNameError("blob name is outside the prefix")
    name = blob_name.removeprefix(prefix)
    unsafe = (
        not name
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or ".." in name
        or _has_control_char(name)
        or name.casefold().endswith(_RESERVED_SUFFIX)
    )
    if unsafe:
        raise RejectedNameError("unsafe blob name")
    return name


@dataclass
class _Received:
    """What `_download` saw: filled in only once all bytes arrived."""

    sha256: str = ""
    size: int = 0


@dataclass
class _Budget:
    """Size limits for one run. `used` counts every byte received so far (all blobs)."""

    max_blob_bytes: int
    max_total_bytes: int
    used: int = 0
    stopped: bool = False  # the total cap was hit; no further blob is downloaded

    def would_exceed(self, listed_size: int) -> bool:
        return self.used + listed_size > self.max_total_bytes


def _download(
    container: BlobContainer, name: str, target: Path, budget: _Budget, received: _Received
) -> None:
    """Stream a blob to a temp file, then link it to `target` (no replace).

    `received` is filled once all bytes arrived, even if the link then fails because `target`
    exists, so the caller can compare against the existing file without a second download.
    """

    def write(temp: Path) -> None:
        digest = hashlib.sha256()
        size = 0
        with temp.open("wb") as handle:
            for chunk in container.read_blob(name):
                size += len(chunk)
                budget.used += len(chunk)
                if size > budget.max_blob_bytes:
                    raise BlobTooLargeError("blob exceeds the size cap while streaming")
                if budget.used > budget.max_total_bytes:
                    raise TotalSizeCapError("run exceeds the total size cap while streaming")
                digest.update(chunk)
                handle.write(chunk)
        received.sha256 = digest.hexdigest()
        received.size = size

    publish(target, write)


def _is_same_file(target: Path, sha256: str) -> bool:
    if target.is_symlink() or not target.is_file():
        return False
    return hash_file(target) == sha256


def _stage_blob(
    container: BlobContainer,
    info: BlobInfo,
    name: str,
    folder: Path,
    budget: _Budget,
    received: _Received,
) -> bool:
    """Download one blob. Return True if staged, False if an identical file was already there.

    Raises NameCollisionError if a different file has the name, BlobTooLargeError over the cap.
    """
    if info.size > budget.max_blob_bytes:
        raise BlobTooLargeError("blob exceeds the size cap")
    target = folder / name
    exists = False
    try:
        _download(container, info.name, target, budget, received)
    except FileExistsError:
        if not os.path.lexists(target):
            raise  # not a name clash (for example the folder path is a file)
        exists = True
    if not exists:
        return True
    if not _is_same_file(target, received.sha256):
        raise NameCollisionError("a different local file has this name")
    return False


def _list_candidates(
    container: BlobContainer, prefix: str, max_blobs: int
) -> tuple[list[BlobInfo], int, bool]:
    """Return (blobs to consider, ignored count, whether there were more than `max_blobs`)."""
    blobs: list[BlobInfo] = []
    ignored = 0
    for info in container.list_blobs(prefix):
        if info.is_prefix or info.name.endswith("/"):
            ignored += 1
            _log("azure_stage_ignored", file_name=info.name)
            continue
        if len(blobs) >= max_blobs:
            return blobs, ignored, True
        blobs.append(info)
    return blobs, ignored, False


def _process(
    container: BlobContainer,
    info: BlobInfo,
    prefix: str,
    folder: Path,
    source: str,
    budget: _Budget,
) -> str:
    """Stage one blob; return `staged`, `skipped` or `failed`. Never raises."""
    started = time.monotonic()
    problem = ""
    staged = False
    sha256 = ""
    received = _Received()
    try:
        name = local_name(info.name, prefix)
        staged = _stage_blob(container, info, name, folder, budget, received)
        sha256 = hash_file(folder / name)
    except TotalSizeCapError as error:
        budget.stopped = True
        problem = type(error).__name__
    except Exception as error:  # noqa: BLE001 - one bad blob must not stop the rest
        problem = type(error).__name__  # the message can carry paths or library details
    duration = round(time.monotonic() - started, 3)
    if problem:
        _log("azure_stage_error", source=source, blob_name=info.name, error=problem)
        return "failed"
    event = "azure_stage_write" if staged else "azure_stage_skip"
    _log(
        event,
        source=source,
        blob_name=info.name,
        size=received.size,  # the bytes actually received, not the listed size
        file_hash=sha256,
        duration_s=duration,
    )
    return "staged" if staged else "skipped"


def _process_all(
    container: BlobContainer,
    blobs: list[BlobInfo],
    prefix: str,
    folder: Path,
    source: str,
    budget: _Budget,
) -> list[str]:
    """Process blobs in order; stop before the first one that would pass the total cap."""
    outcomes: list[str] = []
    for info in blobs:
        if budget.would_exceed(info.size):
            budget.stopped = True
        if budget.stopped:
            break
        outcomes.append(_process(container, info, prefix, folder, source, budget))
    return outcomes


def stage(
    container: BlobContainer,
    source: str,
    prefix: str,
    landing_root: Path,
    max_blob_bytes: int = DEFAULT_MAX_BLOB_MIB * _MIB,
    max_blobs: int = DEFAULT_MAX_BLOBS,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_MIB * _MIB,
) -> StageSummary:
    """Download the blobs directly under `prefix` into `<landing_root>/<source>/`.

    `prefix` must already be normalized (see `normalize_prefix`). Listing errors propagate (the
    CLI maps them to exit 2); per-blob errors are counted as failed. Hitting the blob-count cap or
    the total size cap counts one extra failure each.
    """
    blobs, ignored, over_limit = _list_candidates(container, prefix, max_blobs)
    folder = landing_root / source
    budget = _Budget(max_blob_bytes, max_total_bytes)
    outcomes = _process_all(container, blobs, prefix, folder, source, budget)
    failed = outcomes.count("failed") + int(over_limit) + int(budget.stopped)
    if over_limit:
        _log("azure_stage_error", source=source, error="TooManyBlobs", max_blobs=max_blobs)
    if budget.stopped:
        _log("azure_stage_error", source=source, error="TotalSizeCap", received=budget.used)
    summary = StageSummary(outcomes.count("staged"), outcomes.count("skipped"), ignored, failed)
    _log(
        "azure_stage_run",
        source=source,
        staged=summary.staged,
        skipped=summary.skipped,
        ignored=summary.ignored,
        failed=summary.failed,
    )
    return summary


def _positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ingestion.azure_stage",
        description="Download blobs under a container prefix into data/landing/<source>/",
        epilog="exit codes: 0 ok, 1 a blob failed, 2 setup, sign-in or access error",
    )
    parser.add_argument("source", help="source name; files go to data/landing/<source>/")
    parser.add_argument("prefix", help="container prefix to read, for example orders/")
    parser.add_argument(
        "--max-blob-mib",
        type=_positive_int,
        default=DEFAULT_MAX_BLOB_MIB,
        help=f"per-blob size cap in MiB (default {DEFAULT_MAX_BLOB_MIB})",
    )
    parser.add_argument(
        "--max-blobs",
        type=_positive_int,
        default=DEFAULT_MAX_BLOBS,
        help=f"most blobs per run (default {DEFAULT_MAX_BLOBS})",
    )
    parser.add_argument(
        "--max-total-mib",
        type=_positive_int,
        default=DEFAULT_MAX_TOTAL_MIB,
        help=(
            f"cap on all bytes received in the run, in MiB (default {DEFAULT_MAX_TOTAL_MIB}); "
            "staging stops before the blob that would pass it and the run exits 1"
        ),
    )
    return parser.parse_args(argv)


def _read_settings(environ: Mapping[str, str]) -> tuple[str, str]:
    """Return (account, container) from the environment; raise ValueError naming the setting."""
    values = []
    for key, pattern in ((ENV_ACCOUNT, _ACCOUNT_PATTERN), (ENV_CONTAINER, _CONTAINER_PATTERN)):
        value = environ.get(key, "")
        if not value:
            raise ValueError(f"missing setting {key}")
        if not pattern.fullmatch(value):
            raise ValueError(f"invalid setting {key}")  # never echo the value
        values.append(value)
    return values[0], values[1]


def _setup_error(error: str, **fields: object) -> int:
    _log("azure_stage_error", error=error, **fields)
    return 2


def _run_stage(
    container_factory: ContainerFactory,
    account: str,
    container_name: str,
    prefix: str,
    args: argparse.Namespace,
    landing_root: Path | None,
) -> StageSummary:
    """Build the container, stage, and always close the real client and credential."""
    container = container_factory(account, container_name)
    try:
        return stage(
            container,
            args.source,
            prefix,
            landing_root or bronze.LANDING_ROOT,
            max_blob_bytes=args.max_blob_mib * _MIB,
            max_blobs=args.max_blobs,
            max_total_bytes=args.max_total_mib * _MIB,
        )
    finally:
        if isinstance(container, AzureContainer):
            container.close()


def main(
    argv: list[str] | None = None,
    container_factory: ContainerFactory = open_container,
    landing_root: Path | None = None,
) -> int:
    """Run the command line. `container_factory` and `landing_root` are test seams."""
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # azure-core logs request URLs and headers at INFO; keep them out of our output.
    logging.getLogger("azure").setLevel(logging.WARNING)
    try:
        bronze._validate_source(args.source)
    except ValueError as error:
        return _setup_error(type(error).__name__, source=args.source)
    try:
        prefix = normalize_prefix(args.prefix)
    except ValueError:
        return _setup_error("InvalidPrefix")
    try:
        account, container_name = _read_settings(os.environ)
    except ValueError as error:
        message = str(error)  # our own fixed text: names the setting, never its value
        return _setup_error("InvalidSetting", detail=message)
    try:
        summary = _run_stage(container_factory, account, container_name, prefix, args, landing_root)
    except (AzureError, OSError) as error:  # sign-in, access or listing failure
        # Library messages can carry URLs; log the type only. Other errors are bugs: let them out.
        return _setup_error(type(error).__name__, source=args.source)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
