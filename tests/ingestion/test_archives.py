import gzip
import hashlib
import io
import logging
import struct
import traceback
import zipfile
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from ingestion import archives
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


def test_hostile_member_names_cannot_escape(project: Path) -> None:
    long_name = "n" * 300 + ".csv"
    members = {
        "../x.csv": b"id\n1\n",
        "/etc/x.csv": b"id\n2\n",
        "a/../../x.csv": b"id\n3\n",
        "C:\\x.csv": b"id\n4\n",
        "..\\..\\y.csv": b"id\n5\n",
        long_name: b"id\n6\n",
        "nul.csv\x00.exe": b"id\n7\n",
    }
    _zip(_landing(project) / "evil.zip", members)
    before = {p for p in project.parent.glob("*")}

    assert main(["demo"]) == 0

    assert sorted(_frames(project)["id"].to_list()) == ["1", "2", "3", "4", "5", "6", "7"]
    assert "evil.zip!../x.csv" in _names(project)
    assert f"evil.zip!{long_name}" in _names(project)
    assert {p for p in project.parent.glob("*")} == before
    assert not (project.parent / "x.csv").exists()
    assert not (project.parent / "y.csv").exists()
    assert not Path("/etc/x.csv").exists()
    inside = {p.relative_to(project).parts[0] for p in project.rglob("*") if p.is_file()}
    assert inside <= {"data", "lake"}
    stray = [p for p in project.rglob("*") if p.is_file() and p.name.startswith("member-")]
    assert stray == []


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


def test_understated_header_size_does_not_defeat_the_cap(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _zip(_landing(project) / "lie.zip", {"a.csv": b"id\n" + b"1\n" * 5000})
    data = bytearray(path.read_bytes())
    local = data.find(b"PK\x03\x04")
    central = data.find(b"PK\x01\x02")
    struct.pack_into("<I", data, local + 22, 3)
    struct.pack_into("<I", data, central + 24, 3)
    path.write_bytes(data)
    monkeypatch.setattr(archives, "MAX_MEMBER_BYTES", 100)
    (_landing(project) / "z.csv").write_text("id\n7\n")

    assert main(["demo"]) == 1

    assert _names(project) == {"z.csv"}


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


def test_corrupt_gzip_fails(project: Path) -> None:
    (_landing(project) / "a.csv.gz").write_bytes(gzip.compress(b"id\n1\n")[:-6] + b"zzzzzz")
    assert main(["demo"]) == 1
    assert _raw_count(project) == 1


def test_pending_expands_archives_and_writes_nothing(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _zip(_landing(project) / "pack.zip", {"a.csv": b"id\n1\n", "b.txt": b"a|b\n1|2\n"})
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    assert not (project / "lake").exists()
    assert caplog.text.count("event=bronze_pending ") == 2
    assert "member=1" in caplog.text
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
