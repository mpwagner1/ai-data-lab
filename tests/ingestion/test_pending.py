"""`--pending` runs the real read path (read-only) and must agree with a real run."""

import io
import logging
import os
import re
import tempfile
import zipfile
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from ingestion.bronze import main

SECRET = "SECRETCONTENT"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    (tmp_path / "systmp").mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "systmp"))
    return tmp_path


def _landing(project: Path) -> Path:
    return project / "data" / "landing" / "demo"


def _config(project: Path, text: str) -> None:
    folder = project / "config" / "sources"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "demo.yaml").write_text(text)


def _xlsx() -> bytes:
    book = openpyxl.Workbook()
    book.active.append(["id"])
    book.active.append([SECRET])
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def _parquet() -> bytes:
    buffer = io.BytesIO()
    pl.DataFrame({"id": [SECRET] * 50}).write_parquet(buffer)
    return buffer.getvalue()


def _zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _corrupt_xlsx() -> bytes:
    return _zip({"xl/workbook.xml": SECRET.encode()})


def _tree(root: Path) -> dict[str, tuple[bool, int, bytes]]:
    """Every path under `root` with kind, mtime (files) and bytes, to prove nothing changed.

    A directory's mtime is skipped: the private temp dir comes and goes inside `systmp`.
    """
    return {
        str(p.relative_to(root)): (
            p.is_dir(),
            0 if p.is_dir() else p.stat().st_mtime_ns,
            b"" if p.is_dir() else p.read_bytes(),
        )
        for p in root.rglob("*")
    }


# name -> (landing files, config yaml or None)
Case = tuple[dict[str, bytes], str | None]
CASES: dict[str, Case] = {
    "ready_csv": ({"a.csv": b"id\n1\n"}, None),
    "cp1252_no_config": ({"a.csv": "id\ncaf\xe9\n".encode("cp1252")}, None),
    "cp1252_with_config": ({"a.csv": "id\ncaf\xe9\n".encode("cp1252")}, "encoding: cp1252\n"),
    "needs_config": ({"a.txt": b"a|b\n1|2\n"}, None),
    "no_reader": ({"a.bin": b"\x00\x01\x02binary"}, None),
    "corrupt_xlsx": ({"a.xlsx": _corrupt_xlsx()}, None),
    "truncated_parquet": ({"a.parquet": _parquet()[:-30]}, None),
    "invalid_json": ({"a.json": f'{{"{SECRET}": '.encode()}, None),
    "utf16_wrong_endian": (
        {"a.csv": "id\n1\n".encode("utf-16-be")},
        "encoding: utf-16-le\n",
    ),
    "xlsx_header_row_past_end": ({"a.xlsx": _xlsx()}, "header_row: 9\n"),
    "reserved_column": ({"a.csv": b"_x\n1\n"}, None),
    "empty_csv": ({"a.csv": b""}, None),
    "good_parquet": ({"a.parquet": _parquet()}, None),
    "archive_mixed": (
        {"a.zip": _zip({"ok.csv": b"id\n1\n", "held.bin": b"\x00\x01", "bad.json": b'{"x": '})},
        None,
    ),
    "broken_archive": ({"a.zip": b"PK\x03\x04" + b"\x00" * 40}, None),
    "identical_files": ({"a.csv": b"id\n1\n", "b.csv": b"id\n1\n"}, None),
    "identical_held_files": ({"a.bin": b"\x00\x01", "b.bin": b"\x00\x01"}, None),
    "identical_failed_files": ({"a.json": b'{"x": ', "b.json": b'{"x": '}, None),
    "zip_member_copy_of_earlier_file": (
        {"a.csv": b"id\n1\n", "b.zip": _zip({"x.csv": b"id\n1\n", "y.csv": b"id\n2\n"})},
        None,
    ),
    "landing_file_copy_of_earlier_member": (
        {"a.zip": _zip({"x.csv": b"id\n1\n"}), "b.csv": b"id\n1\n"},
        None,
    ),
    "identical_members_in_one_zip": (
        {"a.zip": _zip({"x.csv": b"id\n1\n", "y.csv": b"id\n1\n"})},
        None,
    ),
}


def _outcomes(text: str) -> list[str]:
    """Reasons from the log, in one vocabulary for `--pending` and a real run."""
    found = []
    for line in text.splitlines():
        reason = re.search(r'reason="(\w+)"', line)
        error = re.search(r'error="(\w+)"', line)
        if "event=bronze_pending " in line or "event=bronze_unparsed" in line:
            assert reason
            found.append(reason[1] + (f":{error[1]}" if error else ""))
        elif "event=bronze_write" in line:
            found.append("ready")
        elif "event=bronze_skip" in line:
            found.append("skipped")
        elif "event=bronze_error" in line:
            assert error
            found.append(
                "archive_error" if error[1] == "ArchiveReadError" else f"failed:{error[1]}"
            )
    return sorted(found)


@pytest.mark.parametrize("name", list(CASES))
def test_pending_agrees_with_the_real_run(
    project: Path, caplog: pytest.LogCaptureFixture, name: str
) -> None:
    files, config = CASES[name]
    for file_name, data in files.items():
        (_landing(project) / file_name).write_bytes(data)
    if config:
        _config(project, config)
    caplog.set_level(logging.INFO)

    main(["demo", "--pending"])
    predicted = _outcomes(caplog.text)
    caplog.clear()
    main(["demo"])
    actual = _outcomes(caplog.text)

    assert predicted == actual
    assert predicted  # every case produces at least one outcome
    assert SECRET not in caplog.text


@pytest.mark.parametrize("name", list(CASES))
def test_pending_leaves_the_whole_tree_untouched(project: Path, name: str) -> None:
    files, config = CASES[name]
    for file_name, data in files.items():
        (_landing(project) / file_name).write_bytes(data)
    if config:
        _config(project, config)
    before = _tree(project)

    main(["demo", "--pending"])

    assert _tree(project) == before
    assert not (project / "lake").exists()
    assert list((project / "systmp").iterdir()) == []


def _reasons(project: Path, caplog: pytest.LogCaptureFixture) -> list[str]:
    caplog.clear()
    main(["demo", "--pending"])
    return _outcomes(caplog.text)


def test_cp1252_is_encoding_until_config_is_added(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.csv").write_bytes("id\ncaf\xe9\n".encode("cp1252"))
    caplog.set_level(logging.INFO)
    assert _reasons(project, caplog) == ["encoding"]
    _config(project, "encoding: cp1252\n")
    assert _reasons(project, caplog) == ["ready"]
    assert main(["demo"]) == 0
    assert _reasons(project, caplog) == []  # loaded files are not listed


def test_failed_files_show_the_error_type_and_no_content(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    for name in ("corrupt_xlsx", "truncated_parquet", "invalid_json"):
        for file_name, data in CASES[name][0].items():
            (_landing(project) / file_name).write_bytes(data)
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    assert caplog.text.count('reason="failed"') == 3
    assert caplog.text.count("error=") == 3
    assert SECRET not in caplog.text


def test_archive_with_good_held_and_corrupt_members(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.zip").write_bytes(CASES["archive_mixed"][0]["a.zip"])
    caplog.set_level(logging.INFO)
    assert main(["demo", "--pending"]) == 3
    assert _outcomes(caplog.text) == sorted(["ready", "no_reader", "failed:JsonReadError"])
    assert re.findall(r"\bmember=(\d+)(?=\s|$)", caplog.text) == ["1", "2", "3"]


def test_pending_all_ready_still_exits_3(project: Path) -> None:
    (_landing(project) / "a.csv").write_bytes(b"id\n1\n")
    assert main(["demo", "--pending"]) == 3


def test_temp_dir_is_clean_after_a_failing_member(project: Path) -> None:
    (_landing(project) / "a.zip").write_bytes(CASES["archive_mixed"][0]["a.zip"])
    main(["demo", "--pending"])
    assert list((project / "systmp").iterdir()) == []


def test_event_levels(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    landing = _landing(project)
    (landing / "ok.csv").write_bytes(b"id\n1\n")
    (landing / "held.bin").write_bytes(b"\x00\x01")
    (landing / "bad.json").write_bytes(b'{"x": ')
    caplog.set_level(logging.INFO)

    main(["demo"])

    levels = {
        re.search(r"event=(\w+)", r.getMessage())[1]: r.levelno  # type: ignore[index]
        for r in caplog.records
        if "event=" in r.getMessage()
    }
    assert levels["bronze_error"] == logging.ERROR
    assert levels["bronze_unparsed"] == logging.WARNING
    for event in ("bronze_receive", "bronze_write", "bronze_run"):
        assert levels[event] == logging.INFO


def test_archive_member_failure_logs_at_error(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.zip").write_bytes(CASES["archive_mixed"][0]["a.zip"])
    caplog.set_level(logging.INFO)
    main(["demo"])
    by_event = {
        re.search(r"event=(\w+)", r.getMessage())[1]: r.levelno  # type: ignore[index]
        for r in caplog.records
    }
    assert by_event["bronze_error"] == logging.ERROR
    assert by_event["bronze_unparsed"] == logging.WARNING


def _reason_lines(text: str) -> dict[str, str]:
    """`file_name` -> reason for each `bronze_pending` line."""
    return {
        m[0]: m[1]
        for m in re.findall(
            r'event=bronze_pending source="demo" file_name="([^"]+)".*?reason="(\w+)"', text
        )
    }


def test_pending_marks_a_later_identical_file_skipped_and_still_exits_3(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    for name in ("a.csv", "b.csv"):
        (_landing(project) / name).write_bytes(b"id\n1\n")
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3
    assert _reason_lines(caplog.text) == {"a.csv": "ready", "b.csv": "skipped"}
    assert "pending=2" in caplog.text
    caplog.clear()

    main(["demo"])
    assert caplog.text.count("event=bronze_write") == 1
    assert caplog.text.count("event=bronze_skip") == 1
    assert len(list((project / "lake").rglob("*.parquet"))) == 1


def test_pending_member_copy_of_a_landing_file_is_skipped(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    for name, data in CASES["zip_member_copy_of_earlier_file"][0].items():
        (_landing(project) / name).write_bytes(data)
    caplog.set_level(logging.INFO)

    main(["demo", "--pending"])

    assert _outcomes(caplog.text) == ["ready", "ready", "skipped"]
    assert re.findall(r'file_name="b.zip" member=(\d) .*?reason="(\w+)"', caplog.text) == [
        ("1", "skipped"),
        ("2", "ready"),
    ]


def test_pending_failed_first_copy_gives_the_same_failure(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    for name in ("a.json", "b.json"):
        (_landing(project) / name).write_bytes(b'{"x": ')
    caplog.set_level(logging.INFO)

    main(["demo", "--pending"])

    assert _outcomes(caplog.text) == ["failed:JsonReadError"] * 2


@pytest.fixture
def locked_file(project: Path) -> Path:
    if os.name != "posix" or os.geteuid() == 0:
        pytest.skip("chmod 000 does not deny reads on this platform or as root")
    path = _landing(project) / "locked.csv"
    path.write_bytes(b"id\n2\n")
    path.chmod(0)
    try:
        if os.access(path, os.R_OK):
            pytest.skip("chmod 000 does not deny reads here")
        yield path
    finally:
        path.chmod(0o644)


def test_pending_reports_an_unreadable_file_and_lists_the_rest(
    project: Path, locked_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "ok.csv").write_bytes(b"id\n1\n")
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    lines = [r for r in caplog.text.splitlines() if "event=bronze_pending " in r]
    locked = next(line for line in lines if "locked.csv" in line)
    assert 'reason="failed"' in locked
    assert 'error="PermissionError"' in locked
    assert "file_hash" not in locked
    assert _reason_lines(caplog.text) == {"locked.csv": "failed", "ok.csv": "ready"}
    assert str(project) not in caplog.text
    assert "Traceback" not in caplog.text
    assert "pending=2" in caplog.text


def test_unreadable_file_agrees_with_the_real_run(
    project: Path, locked_file: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "ok.csv").write_bytes(b"id\n1\n")
    caplog.set_level(logging.INFO)

    main(["demo", "--pending"])
    predicted = _outcomes(caplog.text)
    caplog.clear()
    assert main(["demo"]) == 1
    actual = _outcomes(caplog.text)

    assert predicted == actual == ["failed:PermissionError", "ready"]
    assert len(list((project / "lake").rglob("*.parquet"))) == 1


def test_pending_unreadable_archive_never_tracebacks(
    project: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.zip").write_bytes(_zip({"x.csv": b"id\n1\n"}))

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError(f"{SECRET} /some/path")

    monkeypatch.setattr("ingestion.archive_ingest.expand_archive", boom)
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    assert _outcomes(caplog.text) == ["failed:OSError"]
    assert SECRET not in caplog.text
    assert "/some/path" not in caplog.text
