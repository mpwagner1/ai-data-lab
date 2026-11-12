"""Raw zone: exact copies of landing files, and the scan of the landing folder.

Layout: `<lake_root>/raw/<source>/receive_date=YYYY-MM-DD/<file_hash><ext>`. Landing files are
never moved or deleted. Symlinks are never followed, so nothing outside the landing folder can
reach the lake.
"""

import os
import re
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from ingestion.metadata import hash_file

_RAW_EXTENSION = re.compile(r"\.[a-z0-9]{1,10}")
_LOCK_SUFFIXES = (".lock", ".lck")
SOURCE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


def publish(target: Path, write: Callable[[Path], object]) -> None:
    """Write to a temp file, then link it to `target`; fails if `target` exists.

    A crash leaves only a `.tmp` file, which the skip checks and readers ignore.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f"{target.name}.tmp")
    try:
        write(temp)
        os.link(temp, target)  # no-replace: raises FileExistsError
    finally:
        temp.unlink(missing_ok=True)


def _raw_extension(path: Path) -> str:
    extension = path.suffix.lower()
    return extension if _RAW_EXTENSION.fullmatch(extension) else ""


def _already_received(raw_dir: Path, file_hash: str) -> bool:
    candidates = raw_dir.glob(f"receive_date=*/{file_hash}*")
    return any(
        p.name == file_hash or p.name.startswith(f"{file_hash}.")
        for p in candidates
        if not p.name.endswith(".tmp")
    )


def receive_raw(path: Path, source: str, lake_root: Path, file_hash: str, run_at: datetime) -> bool:
    """Copy a landing file unchanged into the raw zone. Returns False if its hash is already there.

    The copy is hashed again so the stored bytes are proven equal to the landing bytes.
    Raises ValueError for a symlink (never followed).
    """
    if not SOURCE_PATTERN.fullmatch(source):
        raise ValueError(f"invalid source name: {source!r}")
    if path.is_symlink():
        raise ValueError(f"symlinks are not received: {path.name}")
    raw_dir = lake_root / "raw" / source
    if _already_received(raw_dir, file_hash):
        return False
    target = raw_dir / f"receive_date={run_at:%Y-%m-%d}" / f"{file_hash}{_raw_extension(path)}"

    def copy(temp: Path) -> None:
        shutil.copyfile(path, temp)
        if hash_file(temp) != file_hash:
            raise RuntimeError(f"file changed during receive: {path.name}")

    publish(target, copy)
    return True


def _is_ignored_name(name: str) -> bool:
    return name.startswith((".", "~$")) or name.lower().endswith(_LOCK_SUFFIXES)


def landing_files(folder: Path) -> tuple[list[Path], int]:
    """Return (real files to process, number of symlinks skipped).

    Hidden files, Office `~$` owner files, lock files and subfolders are ignored. Symlinks are
    skipped and counted, whatever they point at.
    """
    files: list[Path] = []
    symlinks = 0
    for path in sorted(folder.iterdir()):
        if path.is_symlink():
            symlinks += 1
        elif not _is_ignored_name(path.name) and path.is_file():
            files.append(path)
    return files, symlinks
