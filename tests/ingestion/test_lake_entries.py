"""A lake entry must be usable before bronze relies on it. Synthetic data only."""

import io
import logging
import struct
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion import bronze, lake_entries, raw_zone
from ingestion.lake_entries import BadLakeEntryError, parquet_problem
from ingestion.metadata import hash_file
from ingestion.pending import failed_outcome
from ingestion.raw_zone import raw_copy_path, receive_raw

RUN_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
SECRET = "SECRETCELL"
CSV_BYTES = f"id,code\n1,{SECRET}\n2,b\n".encode()
BAD = 'error="BadLakeEntryError"'


def _dangling_symlink(entry: Path) -> None:
    entry.symlink_to(entry.parent / "missing-target")


def _directory(entry: Path) -> None:
    entry.mkdir()


def _empty_file(entry: Path) -> None:
    entry.write_bytes(b"")


def _truncated_parquet(entry: Path) -> None:
    entry.write_bytes(b"PAR1\x00\x00")


def _symlink_to_good_parquet(entry: Path) -> None:
    target = entry.parents[3] / "elsewhere.parquet"
    pl.DataFrame({"id": ["1"]}).write_parquet(target)
    entry.symlink_to(target)


def _truncated_copy(entry: Path) -> None:
    entry.write_bytes(CSV_BYTES[:5])


def _same_size_other_bytes(entry: Path) -> None:
    entry.write_bytes(b"x" * len(CSV_BYTES))


def _symlink_to_good_copy(entry: Path) -> None:
    target = entry.parents[3] / "elsewhere.csv"
    target.write_bytes(CSV_BYTES)
    entry.symlink_to(target)


BRONZE_KINDS: dict[str, Callable[[Path], None]] = {
    "dangling_symlink": _dangling_symlink,
    "directory": _directory,
    "empty_file": _empty_file,
    "truncated_parquet": _truncated_parquet,
    "symlink_to_good_parquet": _symlink_to_good_parquet,
}
RAW_KINDS: dict[str, Callable[[Path], None]] = {
    "dangling_symlink": _dangling_symlink,
    "directory": _directory,
    "empty_file": _empty_file,
    "truncated": _truncated_copy,
    "same_size_other_bytes": _same_size_other_bytes,
    "symlink_to_good_copy": _symlink_to_good_copy,
}


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "landing" / "demo").mkdir(parents=True)
    (tmp_path / "config").mkdir()
    return tmp_path


def _landing(project: Path, name: str = "a.csv", data: bytes = CSV_BYTES) -> Path:
    path = project / "landing" / "demo" / name
    path.write_bytes(data)
    return path


def _run(project: Path, force: bool = False) -> bronze.RunSummary:
    return bronze.ingest_landing(
        "demo", project / "landing", project / "lake", force, config_root=project / "config"
    )


def _pending(project: Path) -> int:
    return bronze.list_pending(
        "demo", project / "landing", project / "lake", config_root=project / "config"
    )


def _entries(project: Path, zone: str) -> list[Path]:
    return sorted((project / "lake" / zone / "demo").glob("*_date=*/*"))


def _plant_bronze(
    project: Path, digest: str, make: Callable[[Path], None], day: str = "2026-01-01"
) -> Path:
    folder = project / "lake" / "bronze" / "demo" / f"ingest_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    entry = folder / f"{digest}20260101T000000Z_00000000.parquet"
    make(entry)
    return entry


def _plant_raw(
    project: Path, digest: str, make: Callable[[Path], None], day: str = "2026-01-01"
) -> Path:
    folder = project / "lake" / "raw" / "demo" / f"receive_date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    entry = folder / f"{digest}.csv"
    make(entry)
    return entry


def _good_raw(entry: Path) -> None:
    entry.write_bytes(CSV_BYTES)


def _good_bronze(entry: Path) -> None:
    pl.DataFrame({"id": ["1"]}).write_parquet(entry)


@pytest.mark.parametrize("kind", BRONZE_KINDS)
def test_unusable_bronze_entry_fails_the_file_and_adds_nothing(
    project: Path, caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    digest = hash_file(_landing(project))
    entry = _plant_bronze(project, digest, BRONZE_KINDS[kind])
    caplog.set_level(logging.INFO)

    summary = _run(project)

    assert summary.failed == 1
    assert _entries(project, "bronze") == [entry]
    assert "event=bronze_skip" not in caplog.text
    assert BAD in caplog.text
    assert 'zone="bronze"' in caplog.text
    assert f"ingest_date=2026-01-01/{entry.name}" in caplog.text
    assert digest in caplog.text
    assert SECRET not in caplog.text
    assert str(project) not in caplog.text


@pytest.mark.parametrize("kind", BRONZE_KINDS)
def test_pending_reports_a_file_with_an_unusable_bronze_entry(
    project: Path, caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    digest = hash_file(_landing(project))
    entry = _plant_bronze(project, digest, BRONZE_KINDS[kind])
    caplog.set_level(logging.INFO)

    assert _pending(project) == 1

    assert 'reason="failed"' in caplog.text
    assert BAD in caplog.text
    assert 'zone="bronze"' in caplog.text
    assert _entries(project, "bronze") == [entry]


def test_bad_bronze_entry_beside_a_good_one_still_fails(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    digest = hash_file(_landing(project))
    _plant_bronze(project, digest, _good_bronze, day="2026-01-01")
    _plant_bronze(project, digest, _empty_file, day="2026-01-02")
    caplog.set_level(logging.INFO)

    assert _run(project).failed == 1

    assert "usable_entries=1" in caplog.text
    assert len(_entries(project, "bronze")) == 2


@pytest.mark.parametrize("kind", BRONZE_KINDS)
def test_force_does_not_write_beside_an_unusable_bronze_entry(
    project: Path, caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    digest = hash_file(_landing(project))
    entry = _plant_bronze(project, digest, BRONZE_KINDS[kind])
    caplog.set_level(logging.INFO)

    assert _run(project, force=True).failed == 1

    assert _entries(project, "bronze") == [entry]
    assert 'zone="bronze"' in caplog.text


def test_force_still_reloads_a_file_with_a_usable_entry(project: Path) -> None:
    _landing(project)
    assert _run(project).failed == 0

    assert _run(project, force=True).failed == 0

    assert len(_entries(project, "bronze")) == 2


def test_force_does_not_write_beside_an_unusable_member_entry(project: Path) -> None:
    _archive_with_one_bad_member_entry(project)

    assert _run(project, force=True).failed == 1

    assert len(_entries(project, "bronze")) == 2  # the planted entry and the other member


def _panicking_parquet() -> bytes:
    """A small Parquet file with one footer byte changed so that Polars panics reading it."""
    buffer = io.BytesIO()
    pl.DataFrame({"id": ["1", "2"], "code": ["a", "b"]}).write_parquet(buffer)
    data = bytearray(buffer.getvalue())
    footer_length = struct.unpack("<I", data[-8:-4])[0]
    data[len(data) - 8 - footer_length + 79] = 1
    return bytes(data)


def _panicking_footer(entry: Path) -> None:
    entry.write_bytes(_panicking_parquet())


def test_the_corrupt_footer_fixture_makes_polars_panic() -> None:
    # Guards the tests below: if a Polars upgrade changes the layout, find a new offset.
    with pytest.raises(pl.exceptions.PanicException):
        pl.read_parquet_schema(io.BytesIO(_panicking_parquet()))


def test_footer_that_panics_polars_is_reported_as_unreadable(tmp_path: Path) -> None:
    entry = tmp_path / "a.parquet"
    _panicking_footer(entry)

    assert parquet_problem(entry) == "unreadable_parquet_footer"


@pytest.mark.parametrize("force", [False, True])
def test_footer_that_panics_polars_fails_only_that_file(
    project: Path, caplog: pytest.LogCaptureFixture, force: bool
) -> None:
    digest = hash_file(_landing(project))
    entry = _plant_bronze(project, digest, _panicking_footer)
    _landing(project, "b.csv", b"id\n3\n")
    caplog.set_level(logging.INFO)

    assert _run(project, force=force).failed == 1

    assert len(_entries(project, "bronze")) == 2
    assert entry.read_bytes() == _panicking_parquet()
    assert "unreadable_parquet_footer" in caplog.text


def test_pending_reports_a_footer_that_panics_polars(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    digest = hash_file(_landing(project))
    _plant_bronze(project, digest, _panicking_footer)
    caplog.set_level(logging.INFO)

    assert _pending(project) == 1

    assert "unreadable_parquet_footer" in caplog.text


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_footer_check_lets_interrupts_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupt: type[BaseException]
) -> None:
    entry = tmp_path / "a.parquet"
    _good_bronze(entry)

    def stop(source: object) -> None:
        raise interrupt

    monkeypatch.setattr(lake_entries.pl, "read_parquet_schema", stop)

    with pytest.raises(interrupt):
        parquet_problem(entry)


def test_failed_outcome_names_the_error_type() -> None:
    error = BadLakeEntryError("raw", "0" * 64, ["receive_date=2026-01-01/x:symlink"], 0)

    outcome = failed_outcome(error)

    assert (outcome.reason, outcome.error) == ("failed", "BadLakeEntryError")
    assert outcome.detail["zone"] == "raw"


def _fail_folder_sync(monkeypatch: pytest.MonkeyPatch, prefix: str) -> None:
    real_sync = raw_zone._sync

    def sync(path: Path) -> None:
        if path.is_dir() and path.name.startswith(prefix):
            raise OSError("synthetic folder sync failure")
        real_sync(path)

    monkeypatch.setattr(raw_zone, "_sync", sync)


def test_bronze_folder_sync_failure_fails_the_run_and_the_next_one_skips(
    project: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _landing(project)
    with monkeypatch.context() as patch:
        _fail_folder_sync(patch, "ingest_date=")
        assert _run(project).failed == 1
    [stored] = _entries(project, "bronze")
    caplog.set_level(logging.INFO)

    assert _run(project).failed == 0

    assert _entries(project, "bronze") == [stored]
    assert "event=bronze_skip" in caplog.text


def test_raw_folder_sync_failure_fails_the_run_and_the_next_one_uses_the_copy(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _landing(project)
    with monkeypatch.context() as patch:
        _fail_folder_sync(patch, "receive_date=")
        assert _run(project).failed == 1
    [stored] = _entries(project, "raw")
    assert _entries(project, "bronze") == []

    assert _run(project).failed == 0

    assert _entries(project, "raw") == [stored]
    assert len(_entries(project, "bronze")) == 1


def test_one_unusable_entry_does_not_stop_the_other_files(project: Path) -> None:
    digest = hash_file(_landing(project))
    _plant_bronze(project, digest, _empty_file)
    _landing(project, "b.csv", b"id\n3\n")

    assert _run(project).failed == 1

    assert len(_entries(project, "bronze")) == 2


def _archive_with_one_bad_member_entry(project: Path) -> None:
    path = project / "landing" / "demo" / "a.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("a.csv", CSV_BYTES)
        archive.writestr("b.csv", b"id\n3\n")
    member = project / "member.csv"
    member.write_bytes(CSV_BYTES)
    _plant_bronze(project, hash_file(member), _empty_file)


def test_unusable_entry_fails_only_its_archive_member(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _archive_with_one_bad_member_entry(project)
    caplog.set_level(logging.INFO)

    assert _run(project).failed == 1

    assert len(_entries(project, "bronze")) == 2
    assert 'file_name="a.zip!#1"' in caplog.text
    assert 'zone="bronze"' in caplog.text


def test_pending_reports_an_unusable_member_entry_and_lists_the_rest(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _archive_with_one_bad_member_entry(project)
    caplog.set_level(logging.INFO)

    assert _pending(project) == 2

    assert BAD in caplog.text
    assert "member=1" in caplog.text
    assert "member=2" in caplog.text
    assert 'reason="ready"' in caplog.text


@pytest.mark.parametrize("kind", RAW_KINDS)
def test_unusable_raw_entry_is_not_treated_as_received(project: Path, kind: str) -> None:
    path = _landing(project)
    digest = hash_file(path)
    entry = _plant_raw(project, digest, RAW_KINDS[kind])
    was_symlink = entry.is_symlink()

    with pytest.raises(RuntimeError, match="unusable raw entry"):
        receive_raw(path, "demo", project / "lake", digest, RUN_AT)

    assert _entries(project, "raw") == [entry]
    assert entry.is_symlink() is was_symlink


@pytest.mark.parametrize("kind", RAW_KINDS)
def test_unusable_raw_entry_fails_the_file_and_publishes_no_bronze(
    project: Path, caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    digest = hash_file(_landing(project))
    entry = _plant_raw(project, digest, RAW_KINDS[kind])
    caplog.set_level(logging.INFO)

    assert _run(project).failed == 1

    assert _entries(project, "bronze") == []
    assert _entries(project, "raw") == [entry]
    assert BAD in caplog.text
    assert 'zone="raw"' in caplog.text
    assert f"receive_date=2026-01-01/{digest}.csv" in caplog.text
    assert "usable_entries=0" in caplog.text
    assert SECRET not in caplog.text


@pytest.mark.parametrize(("good_day", "bad_day"), [("01", "02"), ("02", "01")])
def test_bad_raw_copy_fails_even_with_a_good_copy_elsewhere(
    project: Path, caplog: pytest.LogCaptureFixture, good_day: str, bad_day: str
) -> None:
    digest = hash_file(_landing(project))
    _plant_raw(project, digest, _good_raw, day=f"2026-01-{good_day}")
    _plant_raw(project, digest, _empty_file, day=f"2026-01-{bad_day}")
    caplog.set_level(logging.INFO)

    assert _run(project).failed == 1

    assert _entries(project, "bronze") == []
    assert "usable_entries=1" in caplog.text
    assert f"receive_date=2026-01-{bad_day}/{digest}.csv:hash_mismatch" in caplog.text


def test_several_good_raw_copies_are_accepted(project: Path) -> None:
    path = _landing(project)
    digest = hash_file(path)
    _plant_raw(project, digest, _good_raw, day="2026-01-01")
    _plant_raw(project, digest, _good_raw, day="2026-01-02")

    assert receive_raw(path, "demo", project / "lake", digest, RUN_AT) is False
    assert hash_file(raw_copy_path(project / "lake", "demo", digest)) == digest


def test_force_does_not_bypass_the_raw_check(project: Path) -> None:
    digest = hash_file(_landing(project))
    _plant_raw(project, digest, _empty_file)

    assert _run(project, force=True).failed == 1

    assert _entries(project, "bronze") == []


def test_pending_reports_a_loaded_file_whose_raw_copy_went_bad(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _landing(project)
    assert _run(project).failed == 0
    [raw] = _entries(project, "raw")
    raw.write_bytes(b"")
    caplog.set_level(logging.INFO)

    assert _pending(project) == 1

    assert 'reason="failed"' in caplog.text
    assert 'zone="raw"' in caplog.text
    assert raw.read_bytes() == b""
