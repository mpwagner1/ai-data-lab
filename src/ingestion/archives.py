"""Expand `.zip` and `.gz` archives into units for bronze (ADR 0002, Archives).

The archive itself is the raw-zone copy. Each member is streamed to a private temporary directory
under a GENERATED name (`member-<index><whitelisted extension>`) and handed out with its SHA-256.

Safety rules:

- Member names are untrusted text. They are never used (not even in part) to build a path; they
  come back only as data for `_file_name`. They never appear in exceptions or logs: use the
  1-based member index and the member hash instead.
- Caps are enforced on the bytes actually decompressed, not on sizes in the zip headers, which can
  lie: `MAX_MEMBERS`, `MAX_TOTAL_UNCOMPRESSED_BYTES` per archive and `MAX_MEMBER_BYTES` per member.
- Directory entries, hidden (`.`) and `~$` members, `__MACOSX/` entries and symlink entries are
  skipped and counted. Encrypted members, unsupported compression, a corrupt, truncated or bad-CRC
  archive, and any exceeded cap raise `ArchiveReadError` (the archive fails; it is not held).
- Nested archives are not expanded here: the caller detects the member like any file and holds a
  zip or gzip as `no_reader`.
"""

import gzip
import hashlib
import stat
import tempfile
import zipfile
from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

MAX_MEMBERS = 100
MAX_TOTAL_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
MAX_MEMBER_BYTES = 256 * 1024 * 1024
_CHUNK_BYTES = 1024 * 1024
MEMBER_SEPARATOR = "!"
# Only these extensions reach the temp file name, so detection and delimiters work like landing
# files. Anything else gets no extension.
_MEMBER_EXTENSIONS = frozenset(
    {".csv", ".tsv", ".psv", ".txt", ".dat", ".json", ".ndjson", ".jsonl"}
    | {".xlsx", ".xlsm", ".xls", ".ods", ".parquet"}
)
_SUPPORTED_COMPRESSION = frozenset(
    {
        zipfile.ZIP_STORED,
        zipfile.ZIP_DEFLATED,
        zipfile.ZIP_BZIP2,
        zipfile.ZIP_LZMA,
    }
)


class ArchiveReadError(ValueError):
    """An archive cannot be read. Names the error type and the archive file only."""


@dataclass(frozen=True)
class ArchiveUnit:
    """One member, extracted to `path` (a generated name; valid until the next unit)."""

    index: int
    path: Path
    name: str
    file_hash: str


@dataclass
class ArchiveCounts:
    """Entries skipped while expanding (counts only)."""

    ignored: int = 0
    symlinks: int = 0


@dataclass
class _Budget:
    """Running total of decompressed bytes for one archive."""

    path: Path
    total: int = 0
    member: int = field(default=0)

    def start_member(self) -> None:
        self.member = 0

    def add(self, size: int) -> None:
        self.member += size
        self.total += size
        if self.member > MAX_MEMBER_BYTES:
            raise _fail(self.path, "member too large")
        if self.total > MAX_TOTAL_UNCOMPRESSED_BYTES:
            raise _fail(self.path, "archive too large")


def _fail(path: Path, problem: str) -> ArchiveReadError:
    return ArchiveReadError(f"cannot read archive {path.name}: {problem}")


def _guarded[T](path: Path, work: Callable[[], T]) -> T:
    """Run `work`; any error but our own becomes an ArchiveReadError with no cause or context.

    Only the error type name is kept inside the block; the new error is raised after it, so
    library text (which can hold member names or content) never travels with it.
    """
    problem = ""
    try:
        return work()
    except ArchiveReadError:
        raise
    except Exception as error:
        problem = type(error).__name__
    raise _fail(path, problem)


def member_extension(name: str) -> str:
    """The member's extension if it is on the whitelist, else empty. Never part of a path itself."""
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    dot = base.rfind(".")
    extension = base[dot:].lower() if dot >= 0 else ""
    return extension if extension in _MEMBER_EXTENSIONS else ""


def _is_ignored_member(name: str) -> bool:
    normal = name.replace("\\", "/")
    base = normal.rsplit("/", 1)[-1]
    return (
        not base
        or base.startswith((".", "~$"))
        or normal.startswith("__MACOSX/")
        or "/__MACOSX/" in f"/{normal}"
    )


def _is_symlink(info: zipfile.ZipInfo) -> bool:
    return stat.S_ISLNK(info.external_attr >> 16)


def _stream_to_file(source: IO[bytes], target: Path, budget: _Budget) -> str:
    """Copy `source` (a binary reader) to `target` in chunks, counting bytes; return the SHA-256."""
    digest = hashlib.sha256()
    budget.start_member()
    with target.open("xb") as out:
        while chunk := source.read(_CHUNK_BYTES):
            budget.add(len(chunk))
            digest.update(chunk)
            out.write(chunk)
    return digest.hexdigest()


def _check_zip_entries(path: Path, infos: list[zipfile.ZipInfo]) -> None:
    if len(infos) > MAX_MEMBERS:
        raise _fail(path, "too many members")
    for info in infos:
        if info.is_dir() or _is_symlink(info) or _is_ignored_member(info.filename):
            continue
        if info.flag_bits & 0x1:
            raise _fail(path, "encrypted member")
        if info.compress_type not in _SUPPORTED_COMPRESSION:
            raise _fail(path, "unsupported compression")


def _zip_units(path: Path, workdir: Path, counts: ArchiveCounts) -> Iterator[ArchiveUnit]:
    budget = _Budget(path)
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        _check_zip_entries(path, infos)
        for index, info in enumerate(infos, start=1):
            if info.is_dir() or _is_ignored_member(info.filename):
                counts.ignored += 1
                continue
            if _is_symlink(info):
                counts.ignored += 1
                counts.symlinks += 1
                continue
            target = workdir / f"member-{index}{member_extension(info.filename)}"
            with archive.open(info) as source:
                file_hash = _stream_to_file(source, target, budget)
            yield ArchiveUnit(index, target, info.filename, file_hash)
            target.unlink(missing_ok=True)


def _gzip_member_name(path: Path) -> str:
    name = path.name
    return name[: -len(".gz")] if name.lower().endswith(".gz") else name


def _gzip_units(path: Path, workdir: Path) -> Iterator[ArchiveUnit]:
    name = _gzip_member_name(path)
    target = workdir / f"member-1{member_extension(name)}"
    with gzip.open(path, "rb") as source:
        file_hash = _stream_to_file(source, target, _Budget(path))
    yield ArchiveUnit(1, target, name, file_hash)
    target.unlink(missing_ok=True)


def _guarded_units(path: Path, units: Iterator[ArchiveUnit]) -> Iterator[ArchiveUnit]:
    """Advance `units` one step at a time, turning any library error into ArchiveReadError."""
    while True:
        unit = _guarded(path, lambda: next(units, None))
        if unit is None:
            return
        yield unit


def expand_archive(
    path: Path, is_gzip: bool, counts: ArchiveCounts | None = None
) -> Iterator[ArchiveUnit]:
    """Yield each member of a zip (or the one member of a gzip) as an `ArchiveUnit`.

    The member file lives in a private temporary directory that is removed when the iterator is
    exhausted or closed; a unit's file is deleted when the next one is requested. Nothing is
    written anywhere else. Raises ArchiveReadError (no `__cause__` or `__context__`).
    `counts` collects skipped entries.
    """
    counts = counts if counts is not None else ArchiveCounts()
    with tempfile.TemporaryDirectory(prefix="bronze-archive-") as directory:
        workdir = Path(directory)
        units = _gzip_units(path, workdir) if is_gzip else _zip_units(path, workdir, counts)
        with closing(units):
            yield from _guarded_units(path, units)
