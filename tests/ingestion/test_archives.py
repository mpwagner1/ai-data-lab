import gzip
import hashlib
import io
import logging
import os
import re
import struct
import tarfile
import tempfile
import traceback
import uuid
import zipfile
from collections.abc import Callable
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from ingestion import archives, bronze
from ingestion.archives import ArchiveReadError, expand_archive
from ingestion.bronze import main

SECRET_NAME = "SECRETMEMBERNAME"
SECRET_TEXT = "SECRETCONTENT"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _landing(project: Path) -> Path:
    return project / "data" / "landing" / "demo"


def _zip(path: Path, members: dict[str, bytes], method: int = zipfile.ZIP_DEFLATED) -> Path:
    with zipfile.ZipFile(path, "w", compression=method) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def _xlsx(rows: list[list[str]]) -> bytes:
    book = openpyxl.Workbook()
    for row in rows:
        book.active.append(row)
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def _frames(project: Path) -> pl.DataFrame:
    files = sorted((project / "lake" / "bronze" / "demo").glob("ingest_date=*/*.parquet"))
    return (
        pl.concat([pl.read_parquet(f) for f in files], how="diagonal") if files else pl.DataFrame()
    )


def _bronze_count(project: Path) -> int:
    return len(list((project / "lake" / "bronze" / "demo").glob("ingest_date=*/*.parquet")))


def _raw_count(project: Path) -> int:
    return len(list((project / "lake" / "raw" / "demo").glob("receive_date=*/*")))


def _names(project: Path) -> set[str]:
    return set(_frames(project)["_file_name"].to_list())


def test_zip_with_csv_and_xlsx_gives_two_units_and_rerun_skips(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    csv = b"id,name\n1,a\n2,b\n"
    xlsx = _xlsx([["id", "name"], ["9", "z"]])
    archive = _zip(_landing(project) / "pack.zip", {"a.csv": csv, "b.xlsx": xlsx})

    assert main(["demo"]) == 0

    frame = _frames(project)
    assert _bronze_count(project) == 2
    assert _names(project) == {"pack.zip!a.csv", "pack.zip!b.xlsx"}
    hashes = dict(zip(frame["_file_name"], frame["_file_hash"], strict=True))
    assert hashes["pack.zip!a.csv"] == hashlib.sha256(csv).hexdigest()
    assert hashes["pack.zip!b.xlsx"] == hashlib.sha256(xlsx).hexdigest()
    assert frame.filter(pl.col("_file_name") == "pack.zip!b.xlsx")["_sheet_name"][0] == "Sheet"
    raw = list((project / "lake" / "raw" / "demo").glob("receive_date=*/*"))
    assert [p.read_bytes() for p in raw] == [archive.read_bytes()]
    assert raw[0].suffix == ".zip"

    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 0
    assert _bronze_count(project) == 2
    assert caplog.text.count("event=bronze_skip") == 2


def test_gz_loads_with_member_name(project: Path) -> None:
    (_landing(project) / "orders.csv.gz").write_bytes(gzip.compress(b"id\n1\n2\n"))
    assert main(["demo"]) == 0
    frame = _frames(project)
    assert set(frame["_file_name"]) == {"orders.csv.gz!orders.csv"}
    assert frame["id"].to_list() == ["1", "2"]
    assert main(["demo"]) == 0
    assert _bronze_count(project) == 1


def test_zip_saved_with_csv_extension_is_still_a_zip(project: Path) -> None:
    _zip(_landing(project) / "data.csv", {"in.csv": b"id\n1\n"})
    assert main(["demo"]) == 0
    assert _names(project) == {"data.csv!in.csv"}


HOSTILE_NAMES = [
    "../x.csv",
    "/etc/x.csv",
    "a/../../x.csv",
    "C:\\x.csv",
    "..\\..\\y.csv",
    "n" * 300 + ".csv",
    "nul.csv\x00.exe",
    "\\\\server\\share\\x.csv",
    "line\nbreak\x01\x1b[31m.csv",
    "member-1.csv",
    "A.csv",
    "a.csv",
]


@pytest.fixture
def real_temp_roots() -> list[Path]:
    """The REAL system temp root and /tmp, taken before `temp_root` redirects `tempfile`."""
    roots = {Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve()}
    return sorted(roots)


@pytest.fixture
def temp_root(project: Path, monkeypatch: pytest.MonkeyPatch, real_temp_roots: list[Path]) -> Path:
    """Redirect the system temp root into the test tree, as a sibling of the lake and landing."""
    root = project / "systmp"
    root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(root))
    for variable in ("TMPDIR", "TEMP", "TMP"):
        monkeypatch.setenv(variable, str(root))
    return root


def _spy_on_extraction(
    monkeypatch: pytest.MonkeyPatch, project: Path, temp_root: Path
) -> list[set[Path]]:
    """Snapshot everything outside the lake while each member sits extracted on disk.

    Extracted files are deleted again, so only a look taken during the run can see a write that
    went to the wrong place.
    """
    real = bronze.ingest_file
    snapshots: list[set[Path]] = []

    def spy(path: Path, *args: object, **kwargs: object) -> object:
        snapshots.append(
            {p.relative_to(project) for p in project.rglob("*") if "lake" not in p.parts}
        )
        assert path.parent.parent == temp_root
        return real(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(bronze, "ingest_file", spy)
    return snapshots


_STRAY_NAME = re.compile(r"member-\d+.*|escape-.*")


def _snapshot_entries(roots: list[Path]) -> dict[Path, set[str]]:
    return {root: set(os.listdir(root)) for root in roots}


def _is_ours(path: Path, token: str) -> bool:
    """True for a file or tree that this test's code could have created (name or content)."""
    if _STRAY_NAME.fullmatch(path.name):
        return True
    files = [path] if path.is_file() else [p for p in path.rglob("*") if p.is_file()]
    for file in files:
        if _STRAY_NAME.fullmatch(file.name):
            return True
        try:
            if token.encode() in file.read_bytes()[:4096]:
                return True
        except OSError:
            continue  # another process's file we cannot read is not ours
    return False


def _stray_writes(before: dict[Path, set[str]], token: str) -> list[str]:
    """New entries in the real temp roots that match what an extraction would create.

    Other processes also write to the real temp dir, so only entries whose name looks like ours
    (`member-<n>...`, `escape-...`) or whose content carries this test's unique token count.
    """
    found: list[str] = []
    for root, names in before.items():
        for name in sorted(set(os.listdir(root)) - names):
            if _is_ours(root / name, token):
                found.append(str(root / name))
    return found


def test_hostile_member_names_cannot_escape(
    project: Path,
    temp_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    real_temp_roots: list[Path],
) -> None:
    token = uuid.uuid4().hex  # makes our files recognisable in a temp dir other processes share
    members = {name: f"id,tag\n{i},{token}\n".encode() for i, name in enumerate(HOSTILE_NAMES)}
    archive = _zip(_landing(project) / "evil.zip", members)
    snapshots = _spy_on_extraction(monkeypatch, project, temp_root)
    real_before = _snapshot_entries(real_temp_roots)

    assert main(["demo"]) == 0

    # (e) a hard-coded real temp path (not reached by the `tempfile` redirect) stays untouched
    assert _stray_writes(real_before, token) == []

    assert len(snapshots) == len(members)
    for seen in snapshots:  # (a') while a member is on disk, only its private dir holds it
        in_temp = {p for p in seen if p.parts[0] == "systmp"}
        assert {p.parts[0] for p in seen} == {"data", "systmp"}
        assert len(in_temp) == 3  # systmp, the private dir, one generated member file
        assert {p.parts[2][:7] for p in in_temp if len(p.parts) == 3} == {"member-"}

    stored = {f"evil.zip!{name.split(chr(0))[0]}" for name in members}
    assert _names(project) == stored
    assert sorted(_frames(project)["id"].to_list()) == sorted(str(i) for i in range(len(members)))
    # (a) nothing exists outside the lake, the landing folder, the raw zone and the temp root
    everything = {p.relative_to(project) for p in project.rglob("*")}
    assert {p.parts[0] for p in everything} == {"data", "lake", "systmp"}
    assert {p for p in everything if p.parts[0] == "data"} == {
        Path("data"),
        Path("data/landing"),
        Path("data/landing/demo"),
        Path("data/landing/demo/evil.zip"),
    }
    assert not list(project.parent.glob("x.csv")) + list(project.parent.glob("y.csv"))
    # (b) the redirected temp root is empty: the private directory was removed
    assert list(temp_root.iterdir()) == []
    # (c) the lake holds exactly the raw archive copy and one parquet file per member
    lake_files = {p for p in (project / "lake").rglob("*") if p.is_file()}
    hashes = {hashlib.sha256(data).hexdigest() for data in members.values()}
    parquet = {p for p in lake_files if p.suffix == ".parquet"}
    assert {p.name[:64] for p in parquet} == hashes
    assert len(parquet) == len(members)
    assert lake_files - parquet == {
        next((project / "lake" / "raw" / "demo").glob("receive_date=*/*.zip"))
    }
    assert archive.exists()


def test_symlink_directory_hidden_and_macosx_members_are_ignored(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = _landing(project) / "pack.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("keep.csv", b"id\n1\n")
        link = zipfile.ZipInfo("link.csv")
        link.external_attr = 0o120777 << 16
        archive.writestr(link, b"/etc/passwd")
        archive.writestr("folder/", b"")
        archive.writestr(".hidden.csv", b"id\n2\n")
        archive.writestr("sub/~$lock.xlsx", b"x")
        archive.writestr("__MACOSX/._keep.csv", b"x")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 0

    assert _names(project) == {"pack.zip!keep.csv"}
    assert "event=bronze_ignored" in caplog.text
    assert "members=5 symlinks=1" in caplog.text


def test_nested_archive_is_held_and_other_members_load(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    inner = _zip(project / "inner.zip", {"deep.csv": b"id\n9\n"}).read_bytes()
    _zip(
        _landing(project) / "pack.zip",
        {"a.csv": b"id\n1\n", "b.zip": inner, "c.gz": gzip.compress(b"x")},
    )
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 3

    assert _names(project) == {"pack.zip!a.csv"}
    assert caplog.text.count('reason="no_reader"') == 2
    assert main(["demo"]) == 3  # still held, still not expanded
    assert _bronze_count(project) == 1


def test_failed_member_gives_exit_1_and_others_load(project: Path) -> None:
    _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n", "b.csv": b"", "c.bin": b"\x00\x01"})
    assert main(["demo"]) == 1
    assert _names(project) == {"pack.zip!a.csv"}


def test_held_member_with_good_members_gives_exit_3(project: Path) -> None:
    _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n", "b.txt": b"a|b\n1|2\n"})
    assert main(["demo"]) == 3
    assert _names(project) == {"pack.zip!a.csv"}
    assert main(["demo", "--delimiter", "|"]) == 0  # the flag applies to members
    assert _names(project) == {"pack.zip!a.csv", "pack.zip!b.txt"}


def test_gz_member_uses_source_config(project: Path) -> None:
    config = project / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text("delimiter: ';'\n")
    (_landing(project) / "x.txt.gz").write_bytes(gzip.compress(b"a;b\n1;2\n"))
    assert main(["demo"]) == 0
    assert _frames(project)["b"].to_list() == ["2"]


def test_identical_member_in_another_archive_is_skipped_by_hash(project: Path) -> None:
    _zip(_landing(project) / "a.zip", {"x.csv": b"id\n1\n"})
    _zip(_landing(project) / "b.zip", {"y.csv": b"id\n1\n"})
    assert main(["demo"]) == 0
    assert _names(project) == {"a.zip!x.csv"}


@pytest.mark.parametrize(
    ("cap", "value", "members"),
    [
        ("MAX_MEMBERS", 2, {"a.csv": b"id\n1\n", "b.csv": b"id\n2\n", "c.csv": b"id\n3\n"}),
        ("MAX_MEMBER_BYTES", 8, {"a.csv": b"id\n1\n", "b.csv": b"id\n" + b"2\n" * 20}),
        ("MAX_TOTAL_UNCOMPRESSED_BYTES", 10, {"a.csv": b"id\n1\n", "b.csv": b"id\n2\n3\n4\n"}),
    ],
)
def test_caps_fail_the_archive_only(
    project: Path,
    monkeypatch: pytest.MonkeyPatch,
    cap: str,
    value: int,
    members: dict[str, bytes],
) -> None:
    monkeypatch.setattr(archives, cap, value)
    _zip(_landing(project) / "pack.zip", members)
    (_landing(project) / "z.csv").write_text("id\n7\n")

    assert main(["demo"]) == 1

    assert _raw_count(project) == 2
    assert "z.csv" in _names(project)
    # A member finished before the failure may stay; nothing after the failure is written.
    assert "pack.zip!c.csv" not in _names(project)
    assert "pack.zip!b.csv" not in _names(project)


def test_gzip_bomb_is_caught_while_streaming(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 1000)
    (_landing(project) / "bomb.csv.gz").write_bytes(gzip.compress(b"0" * 5_000_000))
    assert main(["demo"]) == 1
    assert _bronze_count(project) == 0
    assert _raw_count(project) == 1


def _patched(path: Path, offset_central: int, fmt: str, value: int, offset_local: int) -> None:
    data = bytearray(path.read_bytes())
    struct.pack_into(fmt, data, data.find(b"PK\x01\x02") + offset_central, value)
    struct.pack_into(fmt, data, data.find(b"PK\x03\x04") + offset_local, value)
    path.write_bytes(data)


def _broken_zip(kind: str, path: Path) -> None:
    _zip(path, {"a.csv": b"id\n1\n2\n3\n"})
    data = path.read_bytes()
    if kind == "corrupt":
        path.write_bytes(b"PK\x03\x04" + b"garbage" * 20)
    elif kind == "truncated":
        path.write_bytes(data[: len(data) // 2])
    elif kind == "encrypted":
        _patched(path, 8, "<H", 1, 6)
    elif kind == "method":
        _patched(path, 10, "<H", 99, 8)
    elif kind == "crc":
        _patched(path, 16, "<I", 12345, 14)


@pytest.mark.parametrize("kind", ["corrupt", "truncated", "encrypted", "method", "crc"])
def test_broken_archive_fails_alone_and_keeps_raw_copy(
    project: Path, kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    _broken_zip(kind, _landing(project) / "bad.zip")
    (_landing(project) / "z.csv").write_text("id\n7\n")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 1

    assert _names(project) == {"z.csv"}
    assert _raw_count(project) == 2
    assert 'error="ArchiveReadError"' in caplog.text


def test_corrupt_gzip_fails(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    good = gzip.compress(f"id\n{SECRET_TEXT}\n".encode())
    (_landing(project) / "a.csv.gz").write_bytes(good[:-6] + b"zzzzzz")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 1

    assert _raw_count(project) == 1
    assert _bronze_count(project) == 0  # no bronze file, not even a partial one
    assert not list((project / "lake" / "bronze").rglob("*"))
    assert "event=bronze_error" in caplog.text
    assert 'error="ArchiveReadError"' in caplog.text
    assert SECRET_TEXT not in caplog.text  # the error log carries no content
    assert "event=bronze_write" not in caplog.text


def test_pending_expands_archives_and_writes_nothing(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n", "b.txt": b"a|b\n1|2\n"})
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    assert not (project / "lake").exists()
    assert caplog.text.count("event=bronze_pending ") == 2
    # anchored: a bare "member=1" would also match member=10
    assert re.findall(r"\bmember=(\d+)(?=\s|$)", caplog.text) == ["1", "2"]
    assert 'reason="ready"' in caplog.text
    assert 'reason="needs_config"' in caplog.text
    assert main(["demo", "--delimiter", "|"]) == 0
    caplog.clear()
    assert main(["demo", "--pending"]) == 0
    assert "event=bronze_pending " not in caplog.text


def test_pending_reports_a_broken_archive(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    _broken_zip("corrupt", _landing(project) / "bad.zip")
    caplog.set_level(logging.INFO)
    assert main(["demo", "--pending"]) == 3
    assert 'reason="archive_error"' in caplog.text
    assert not (project / "lake").exists()


def test_member_names_and_content_never_appear_in_logs(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _zip(
        _landing(project) / "pack.zip",
        {
            f"{SECRET_NAME}_ok.csv": f"id\n{SECRET_TEXT}\n".encode(),
            f"{SECRET_NAME}_held.bin": b"\x00\x01",
            f"{SECRET_NAME}_bad.json": f'{{"{SECRET_TEXT}": '.encode(),
            f"{SECRET_NAME}_empty.csv": b"",
        },
    )
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 1
    assert main(["demo", "--pending"]) == 3

    assert SECRET_NAME not in caplog.text
    assert SECRET_TEXT not in caplog.text
    assert SECRET_NAME in "".join(_names(project))  # stored as data


def _assert_clean(error: BaseException) -> None:
    assert isinstance(error, ArchiveReadError)
    assert error.__context__ is None
    assert error.__cause__ is None
    text = "".join(traceback.format_exception(error))
    assert SECRET_NAME not in text
    assert SECRET_TEXT not in text


def test_archive_errors_carry_no_names_content_or_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = f"id\n{SECRET_TEXT}\n".encode()
    crc = _zip(tmp_path / "crc.zip", {f"{SECRET_NAME}.csv": good})
    _patched(crc, 16, "<I", 1, 14)
    cut = _zip(tmp_path / "cut.zip", {f"{SECRET_NAME}.csv": good})
    cut.write_bytes(cut.read_bytes()[:40])
    enc = _zip(tmp_path / "enc.zip", {f"{SECRET_NAME}.csv": good})
    _patched(enc, 8, "<H", 1, 6)
    many = _zip(tmp_path / "many.zip", {f"{SECRET_NAME}{i}.csv": good for i in range(3)})
    big = _zip(tmp_path / "big.zip", {f"{SECRET_NAME}.csv": good})
    monkeypatch.setattr(archives, "MAX_MEMBERS", 2)
    gz = tmp_path / "plain.csv.gz"
    gz.write_bytes(gzip.compress(good)[:-4])

    for path, is_gzip in [(crc, False), (cut, False), (enc, False), (many, False), (gz, True)]:
        with pytest.raises(ArchiveReadError) as caught:
            list(expand_archive(path, is_gzip))
        _assert_clean(caught.value)
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 3)
    with pytest.raises(ArchiveReadError) as caught:
        list(expand_archive(big, False))
    _assert_clean(caught.value)


def test_expand_extracts_only_to_generated_names_and_cleans_up(tmp_path: Path) -> None:
    path = _zip(tmp_path / "p.zip", {"../../evil.csv": b"id\n1\n", "x/y.weird": b"abc"})
    seen: list[tuple[int, str, str]] = []
    for unit in expand_archive(path, False):
        seen.append((unit.index, unit.path.name, unit.name))
        assert unit.path.read_bytes() in {b"id\n1\n", b"abc"}
        assert unit.path.parent != tmp_path
    assert seen == [(1, "member-1.csv", "../../evil.csv"), (2, "member-2", "x/y.weird")]
    assert not unit.path.exists()
    assert not unit.path.parent.exists()


def _drain(path: Path, is_gzip: bool) -> tuple[list[int], ArchiveReadError | None]:
    """Indexes of the units yielded before the archive failed, and the failure if any."""
    seen: list[int] = []
    try:
        for unit in expand_archive(path, is_gzip):
            seen.append(unit.index)
    except ArchiveReadError as error:
        return seen, error
    return seen, None


class _EndlessSource:
    """A binary reader that yields far more bytes than anything declared about it."""

    def __init__(self) -> None:
        self.reads = 0

    def read(self, size: int) -> bytes:
        self.reads += 1
        assert self.reads < 1000, "the cap never tripped"
        return b"0" * min(size, 10)


def test_streaming_copy_trips_the_cap_on_actual_bytes_not_declared_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # zipfile stops a member at its declared size, so a zip cannot over-deliver. The copy
    # function is the place that must count what actually arrives.
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 100)
    source = _EndlessSource()
    budget = archives._Budget(tmp_path / "x.zip")

    with pytest.raises(ArchiveReadError, match="member too large"):
        archives._stream_to_file(source, tmp_path / "out", budget)  # type: ignore[arg-type]

    assert source.reads == 11


def test_zip_member_far_over_the_cap_with_a_tiny_compressed_size_is_caught(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 1000)
    path = _zip(tmp_path / "z.zip", {"a.csv": b"0" * 3_000_000})
    assert path.stat().st_size < 3_000_000 // 100  # honest headers, tiny on disk

    seen, error = _drain(path, False)

    assert seen == []
    assert error is not None
    assert "member too large" in str(error)


def test_zip_total_cap_trips_on_the_member_that_crosses_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 100)
    monkeypatch.setattr(archives, "MAX_TOTAL_UNCOMPRESSED_BYTES", 100)
    members = {f"{n}.csv": b"id\n" + bytes([48 + n]) * 37 for n in range(3)}  # 40 bytes each
    path = _zip(tmp_path / "z.zip", members)

    seen, error = _drain(path, False)

    assert seen == [1, 2]
    assert error is not None
    assert "archive too large" in str(error)


@pytest.mark.parametrize("trailer", ["wrong_size", "no_size", "no_trailer"])
def test_gzip_over_the_cap_is_caught_whatever_its_trailer_says(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, trailer: str
) -> None:
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 1000)
    data = gzip.compress(b"0" * 3_000_000)
    cut = {
        "wrong_size": data[:-4] + struct.pack("<I", 3),
        "no_size": data[:-4],
        "no_trailer": data[:-8],
    }[trailer]
    path = tmp_path / "a.csv.gz"
    path.write_bytes(cut)

    seen, error = _drain(path, True)

    assert seen == []
    assert error is not None
    assert "member too large" in str(error)


def _raw_files(project: Path) -> list[Path]:
    return list((project / "lake" / "raw" / "demo").glob("receive_date=*/*"))


def test_temp_dir_is_cleaned_after_oserror_mid_copy(
    project: Path, temp_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []

    def fail_on_second_chunk(self: archives._Budget, size: int) -> None:
        calls.append(size)
        if len(calls) == 2:
            raise OSError(28, "no space left")

    monkeypatch.setattr(archives._Budget, "add", fail_on_second_chunk)
    (_landing(project) / "big.csv.gz").write_bytes(gzip.compress(b"0" * 3_000_000))

    assert main(["demo"]) == 1

    assert len(calls) == 2  # it really failed mid-stream, after a partial file was written
    assert list(temp_root.iterdir()) == []


def test_temp_dir_is_cleaned_after_keyboard_interrupt_in_the_consumer(
    project: Path, temp_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def interrupt(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("ingestion.bronze.ingest_file", interrupt)
    _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n", "b.csv": b"id\n2\n"})

    with pytest.raises(KeyboardInterrupt):
        main(["demo"])

    assert list(temp_root.iterdir()) == []
    assert len(_raw_files(project)) == 1  # the raw copy was made before the interrupt


@pytest.mark.parametrize("name", ["logs.tar.gz", "logs.tgz"])
def test_tar_gz_is_held_as_no_reader_and_never_expanded(
    project: Path, caplog: pytest.LogCaptureFixture, name: str
) -> None:
    inner = project / "in.csv"
    inner.write_text("id\n1\n")
    path = _landing(project) / name
    with tarfile.open(path, "w:gz") as tar:
        tar.add(inner, arcname="in.csv")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 3

    assert _bronze_count(project) == 0
    assert caplog.text.count('reason="no_reader"') == 1
    assert [p.read_bytes() for p in _raw_files(project)] == [path.read_bytes()]


def _members(count: int) -> dict[str, bytes]:
    return {f"m{i}.csv": f"id\n{i}\n".encode() for i in range(count)}


def test_exactly_max_members_loads(project: Path) -> None:
    assert archives.MAX_MEMBERS == 100  # the real constant; this test pins its boundary
    _zip(_landing(project) / "pack.zip", _members(archives.MAX_MEMBERS))

    assert main(["demo"]) == 0

    assert _bronze_count(project) == archives.MAX_MEMBERS


def test_one_more_than_max_members_fails_the_archive(project: Path) -> None:
    _zip(_landing(project) / "pack.zip", _members(archives.MAX_MEMBERS + 1))

    assert main(["demo"]) == 1

    assert _bronze_count(project) == 0
    assert len(_raw_files(project)) == 1


def test_literal_101_members_fail_the_archive_whatever_the_cap_constant_is(
    project: Path,
) -> None:
    # A literal 101, not MAX_MEMBERS + 1: this still guards the boundary if the pin above moves.
    _zip(_landing(project) / "pack.zip", _members(101))

    assert main(["demo"]) == 1

    assert _bronze_count(project) == 0
    assert len(_raw_files(project)) == 1


def test_one_more_than_max_members_says_too_many_members(tmp_path: Path) -> None:
    path = _zip(tmp_path / "pack.zip", _members(archives.MAX_MEMBERS + 1))

    seen, error = _drain(path, False)

    assert seen == []
    assert error is not None
    assert "too many members" in str(error)


def _good_zip(path: Path) -> None:
    _zip(path, {"a.csv": b"id\n1\n"})


def _bad_zip(path: Path) -> None:
    _broken_zip("crc", path)


def _good_gz(path: Path) -> None:
    path.write_bytes(gzip.compress(b"id\n1\n"))


def _bad_gz(path: Path) -> None:
    path.write_bytes(gzip.compress(b"id\n1\n")[:-6] + b"zzzzzz")


@pytest.mark.parametrize(
    ("name", "build", "exit_code"),
    [
        ("ok.zip", _good_zip, 0),
        ("bad.zip", _bad_zip, 1),
        ("ok.csv.gz", _good_gz, 0),
        ("bad.csv.gz", _bad_gz, 1),
    ],
)
def test_raw_copy_of_an_archive_is_byte_equal_to_the_landing_archive(
    project: Path, name: str, build: Callable[[Path], None], exit_code: int
) -> None:
    path = _landing(project) / name
    build(path)
    landing_bytes = path.read_bytes()

    assert main(["demo"]) == exit_code

    assert [p.read_bytes() for p in _raw_files(project)] == [landing_bytes]
    assert path.read_bytes() == landing_bytes


def test_format_in_source_config_reads_the_archive_as_that_format_without_expanding(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = project / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text("format: delimited\n")
    path = _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n"})
    caplog.set_level(logging.INFO)

    code = main(["demo"])

    # The zip bytes go to the delimited reader (which holds them), not to the expander: no
    # member is read, no "!" name exists, and the raw copy stays.
    assert code == 3
    assert "event=bronze_unparsed" in caplog.text
    assert "bronze_ignored" not in caplog.text
    assert _bronze_count(project) == 0
    assert [p.read_bytes() for p in _raw_files(project)] == [path.read_bytes()]


def test_gz_whose_content_is_a_zip_is_held_as_no_reader(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    inner = _zip(project / "inner.zip", {"a.csv": b"id\n1\n"})
    path = _landing(project) / "inner.zip.gz"
    path.write_bytes(gzip.compress(inner.read_bytes()))
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 3

    assert _bronze_count(project) == 0
    assert caplog.text.count('reason="no_reader"') == 1
    assert [p.read_bytes() for p in _raw_files(project)] == [path.read_bytes()]


# Issue #33 item 7: workbook members with the other whitelisted extensions.
def _xlsm_bytes(tmp_path: Path) -> bytes:
    book = openpyxl.Workbook()
    book.active.append(["id", "n"])
    book.active.append(["007", "x"])
    path = tmp_path / "book.xlsm"
    book.save(path)
    return path.read_bytes()


@pytest.mark.parametrize("extension", [".xlsm", ".ods"])
def test_zip_member_with_a_macro_or_ods_extension_loads_as_a_workbook(
    project: Path, ods_bytes: Callable[..., bytes], extension: str
) -> None:
    data = _xlsm_bytes(project) if extension == ".xlsm" else ods_bytes([["id", "n"], ["007", "x"]])
    _zip(_landing(project) / "pack.zip", {f"book{extension}": data, "a.csv": b"id\n1\n"})

    assert main(["demo"]) == 0

    frame = _frames(project)
    book = frame.filter(pl.col("_file_name") == f"pack.zip!book{extension}")
    assert book.select("id", "n").rows() == [("007", "x")]
    assert book["_sheet_name"].to_list() == ["Sheet" if extension == ".xlsm" else "S"]
    assert _bronze_count(project) == 2


def test_zip_member_that_claims_to_be_xls_but_is_not_a_workbook_fails_not_held(
    project: Path,
) -> None:
    # A real .xls cannot be built without a new dependency. An OLE signature with no workbook
    # inside stands in for it and checks the failure path: the file fails, the others still load.
    _zip(
        _landing(project) / "pack.zip",
        {"old.xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 600, "a.csv": b"id\n1\n"},
    )

    assert main(["demo"]) == 1

    assert _names(project) == {"pack.zip!a.csv"}
    assert _raw_count(project) == 1


def test_gz_of_an_xlsm_workbook_is_expanded_and_read_by_its_member_name(
    project: Path,
) -> None:
    (_landing(project) / "book.xlsm.gz").write_bytes(gzip.compress(_xlsm_bytes(project)))

    assert main(["demo"]) == 0

    frame = _frames(project)
    assert set(frame["_file_name"]) == {"book.xlsm.gz!book.xlsm"}
    assert frame["id"].to_list() == ["007"]
