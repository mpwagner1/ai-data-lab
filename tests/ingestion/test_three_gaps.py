"""Issue #84: unlistable landing folder, `_row_number` contract text, ExcelReadError base class."""

import logging
import os
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from ingestion.bronze import main
from ingestion.excel_reader import ExcelReadError

SECRET = "SECRETCONTENT"
CONTRACT = Path(__file__).parents[2] / "contracts" / "bronze" / "_metadata.yaml"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    (tmp_path / "systmp").mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "systmp"))
    return tmp_path


def _landing(project: Path) -> Path:
    return project / "data" / "landing" / "demo"


@pytest.fixture
def locked_folder(project: Path) -> Iterator[Path]:
    if os.name != "posix" or os.geteuid() == 0:
        pytest.skip("chmod 000 does not deny listing on this platform or as root")
    folder = _landing(project)
    folder.chmod(0)
    try:
        if os.access(folder, os.R_OK):
            pytest.skip("chmod 000 does not deny listing here")
        yield folder
    finally:
        folder.chmod(0o755)


def _assert_setup_error(caplog: pytest.LogCaptureFixture, error_type: str, project: Path) -> None:
    lines = [r for r in caplog.records if "event=bronze_error" in r.getMessage()]
    assert [r.getMessage() for r in lines] == [
        f'event=bronze_error error={error_type} source="demo"'
    ]
    assert all(r.exc_info is None for r in caplog.records)
    assert "Traceback" not in caplog.text
    assert "landing" not in caplog.text  # the path is not logged
    assert not (project / "lake").exists()


@pytest.mark.parametrize("args", [["demo"], ["demo", "--pending"]])
def test_unlistable_landing_folder_exits_2(
    project: Path, locked_folder: Path, caplog: pytest.LogCaptureFixture, args: list[str]
) -> None:
    caplog.set_level(logging.INFO)

    assert main(args) == 2

    _assert_setup_error(caplog, "PermissionError", project)


@pytest.mark.parametrize("args", [["demo"], ["demo", "--pending"]])
def test_landing_path_that_is_a_file_exits_2(
    project: Path, caplog: pytest.LogCaptureFixture, args: list[str]
) -> None:
    landing = _landing(project)
    landing.rmdir()
    landing.write_bytes(b"id\n1\n")
    caplog.set_level(logging.INFO)

    assert main(args) == 2

    lines = [r.getMessage() for r in caplog.records if "event=bronze_error" in r.getMessage()]
    assert len(lines) == 1
    assert lines[0].startswith("event=bronze_error error=")
    assert lines[0].endswith(' source="demo"')
    assert "Traceback" not in caplog.text
    assert not (project / "lake").exists()


def test_excel_read_error_is_a_value_error() -> None:
    assert issubclass(ExcelReadError, ValueError)


@pytest.mark.parametrize("pending", [False, True])
def test_corrupt_xlsx_still_fails_only_that_file(
    project: Path, caplog: pytest.LogCaptureFixture, pending: bool
) -> None:
    landing = _landing(project)
    with zipfile.ZipFile(landing / "bad.xlsx", "w") as archive:
        archive.writestr("xl/workbook.xml", SECRET)
    (landing / "good.csv").write_bytes(b"id\n1\n")
    caplog.set_level(logging.INFO)

    code = main(["demo", "--pending"] if pending else ["demo"])

    assert SECRET not in caplog.text
    if pending:
        assert code == 3  # the readable file is still pending; nothing is a setup error
        assert 'reason="failed" error="ExcelReadError"' in caplog.text
        assert not (project / "lake").exists()
        return
    assert code == 1
    assert 'event=bronze_error source="demo" file_name="bad.xlsx" error="ExcelReadError"' in (
        caplog.text
    )
    assert len(list((project / "lake" / "bronze" / "demo").rglob("*.parquet"))) == 1


def test_contract_describes_row_number_for_every_format() -> None:
    columns = yaml.safe_load(CONTRACT.read_text())["columns"]
    assert [c["name"] for c in columns] == [
        "_file_name",
        "_ingested_at",
        "_file_hash",
        "_row_number",
        "_sheet_name",
    ]
    description = next(c for c in columns if c["name"] == "_row_number")["description"].lower()
    for keyword in ("delimited", "workbook", "json", "ndjson", "parquet", "archive"):
        assert keyword in description
