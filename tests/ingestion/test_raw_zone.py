import logging
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion import bronze
from ingestion.bronze import FileHeld, ingest_file, main, receive_raw
from ingestion.detect import FileFormat, detect_format
from ingestion.metadata import hash_file
from ingestion.source_config import SourceConfig

RUN_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
SECRET = "SECRETCELL"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _landing(project: Path) -> Path:
    return project / "data" / "landing" / "demo"


def _txt(path: Path, sep: str = "|") -> Path:
    path.write_text(f"id{sep}code\n001{sep}{SECRET}\n")
    return path


def _xlsx_bytes(path: Path) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", "<x/>")
        archive.writestr("xl/workbook.xml", "<x/>")
    return path


def _raw_files(project: Path) -> list[Path]:
    return sorted((project / "lake" / "raw" / "demo").glob("receive_date=*/*"))


def _bronze_files(project: Path) -> list[Path]:
    return sorted((project / "lake" / "bronze" / "demo").glob("ingest_date=*/*.parquet"))


def test_raw_bytes_hash_equal_landing_bytes(project: Path) -> None:
    landing = _landing(project)
    _txt(landing / "a.txt")
    (landing / "b.bin").write_bytes(b"\x00\x01\x02binary")
    main(["demo"])
    raw = {p.stem: p for p in _raw_files(project)}
    for source in (landing / "a.txt", landing / "b.bin"):
        digest = hash_file(source)
        assert hash_file(raw[digest]) == digest
    assert {p.suffix for p in raw.values()} == {".txt", ".bin"}


def test_raw_copy_is_not_duplicated_on_rerun(project: Path) -> None:
    (_landing(project) / "a.csv").write_text("id\n1\n")
    main(["demo"])
    main(["demo"])
    assert len(_raw_files(project)) == 1


def test_receive_raw_detects_change_during_copy(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _txt(_landing(project) / "a.txt")
    with pytest.raises(RuntimeError, match="changed during receive"):
        receive_raw(path, "demo", project / "lake", "0" * 64, RUN_AT)
    assert _raw_files(project) == []
    assert not list((project / "lake").rglob("*.tmp"))


def test_pending_then_config_resolves(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    landing = _landing(project)
    _txt(landing / "a.txt")
    (landing / "b.bin").write_bytes(b"\x00\x01\x02binary")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 3
    assert _bronze_files(project) == []
    assert 'reason="needs_config"' in caplog.text
    assert 'reason="no_reader"' in caplog.text
    assert len(_raw_files(project)) == 2

    config = project / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text("delimiter: '|'\n")
    (landing / "b.bin").unlink()  # the .bin stays pending when present; here only the txt
    assert main(["demo"]) == 0
    frame = pl.read_parquet(_bronze_files(project)[0])
    assert frame["code"].to_list() == [SECRET]


def test_bin_stays_pending_while_txt_parses(project: Path) -> None:
    landing = _landing(project)
    _txt(landing / "a.txt")
    (landing / "b.bin").write_bytes(b"\x00\x01\x02binary")
    assert main(["demo", "--delimiter", "|"]) == 3
    assert len(_bronze_files(project)) == 1


def test_flag_resolves_pending_txt(project: Path) -> None:
    _txt(_landing(project) / "a.txt")
    assert main(["demo"]) == 3
    assert main(["demo", "--delimiter", "|"]) == 0
    assert main(["demo"]) == 0  # now skipped, no longer pending


def test_xlsx_saved_as_csv_is_detected_as_xlsx(project: Path) -> None:
    path = _xlsx_bytes(_landing(project) / "fake.csv")
    assert detect_format(path, SourceConfig()) is FileFormat.XLSX
    assert main(["demo"]) == 3  # no Excel reader yet
    assert _bronze_files(project) == []
    assert len(_raw_files(project)) == 1


def test_csv_header_starting_with_bracket_is_delimited(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text("[id],{x}\n1,2\n")
    assert detect_format(path, SourceConfig()) is FileFormat.DELIMITED


@pytest.mark.parametrize(
    ("name", "content", "expected"),
    [
        ("a.parquet", b"PAR1xxxx", FileFormat.PARQUET),
        ("a.dat", b"\x1f\x8b\x08\x00", FileFormat.GZIP),
        ("a.xls", b"\xd0\xcf\x11\xe0\xa1", FileFormat.XLS),
        ("a.txt", b'[{"a": 1}]', FileFormat.JSON),
        ("a.txt", b'{"a": 1}\n{"a": 2}\n', FileFormat.NDJSON),
        ("a.txt", b'{\n  "a": 1\n}\n', FileFormat.JSON),
        ("a.txt", b"a|b\n1|2\n", FileFormat.DELIMITED),
        ("a.bin", b"a|b\n1|2\n", None),
        ("a.bin", b"\x00\x01\x02", None),
        ("noext", b"a,b\n", None),
    ],
)
def test_detect_format(tmp_path: Path, name: str, content: bytes, expected: FileFormat) -> None:
    path = tmp_path / name
    path.write_bytes(content)
    assert detect_format(path, SourceConfig()) is expected


def test_plain_zip_is_zip_and_corrupt_zip_does_not_raise(tmp_path: Path) -> None:
    plain = tmp_path / "a.zip"
    with zipfile.ZipFile(plain, "w") as archive:
        archive.writestr("x.csv", "a\n1\n")
    corrupt = tmp_path / "b.zip"
    corrupt.write_bytes(b"PK\x03\x04garbage")
    assert detect_format(plain, SourceConfig()) is FileFormat.ZIP
    assert detect_format(corrupt, SourceConfig()) is FileFormat.ZIP


def test_config_format_wins_and_unknown_format_raises(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_text("a,b\n")
    assert detect_format(path, SourceConfig(format="json")) is FileFormat.JSON
    with pytest.raises(ValueError, match="unknown `format`") as error:
        detect_format(path, SourceConfig(format="nonsense"))
    assert "nonsense" not in str(error.value)


def test_dispatch_goes_through_readers_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def fake_reader(path: Path, config: SourceConfig, flag: str | None) -> pl.LazyFrame:
        calls.append(path.name)
        return pl.LazyFrame({"x": ["1"]})

    monkeypatch.setitem(bronze.READERS, FileFormat.JSON, fake_reader)
    path = tmp_path / "a.csv"
    path.write_text("a,b\n1,2\n")
    ingest_file(path, "demo", tmp_path / "lake", config=SourceConfig(format="json"), run_at=RUN_AT)
    assert calls == ["a.csv"]


def test_ingest_file_holds_format_without_reader(tmp_path: Path) -> None:
    path = tmp_path / "a.bin"
    path.write_bytes(b"\x00\x01")
    with pytest.raises(FileHeld) as held:
        ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)
    assert held.value.reason == "no_reader"


def test_bad_encoding_is_held_with_reason_encoding(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.csv").write_bytes("id,name\n1,caf\xe9\n".encode("cp1252"))
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 3
    assert 'reason="encoding"' in caplog.text
    assert _bronze_files(project) == []
    assert len(_raw_files(project)) == 1


def test_hidden_and_lock_files_are_ignored(project: Path) -> None:
    landing = _landing(project)
    (landing / ".hidden.csv").write_text("a\n1\n")
    (landing / "~$book.xlsx").write_bytes(b"lock")
    (landing / "a.csv.lock").write_text("")
    (landing / "sub").mkdir()
    assert main(["demo"]) == 0
    assert _raw_files(project) == []


def test_failed_file_beats_pending_in_exit_code(project: Path) -> None:
    landing = _landing(project)
    (landing / "a_empty.csv").write_bytes(b"")
    (landing / "b.bin").write_bytes(b"\x00\x01")
    assert main(["demo"]) == 1
    assert len(_raw_files(project)) == 2


def test_pending_flag_lists_and_writes_nothing(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    _txt(landing / "a.txt")
    (landing / "b.bin").write_bytes(b"\x00\x01")
    (landing / "c.csv").write_text("id\n1\n")
    caplog.set_level(logging.INFO)

    assert main(["demo", "--pending"]) == 3

    assert 'reason="needs_config"' in caplog.text
    assert 'reason="no_reader"' in caplog.text
    assert 'reason="ready"' in caplog.text
    assert not (project / "lake").exists()
    caplog.clear()
    assert main(["demo", "--delimiter", "|"]) == 3
    assert main(["demo", "--pending"]) == 3  # the .bin
    assert caplog.text.count("event=bronze_pending ") == 1


def test_pending_flag_exits_0_when_nothing_pending(project: Path) -> None:
    (_landing(project) / "a.csv").write_text("id\n1\n")
    main(["demo"])
    assert main(["demo", "--pending"]) == 0


def test_logs_contain_no_cell_values(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    landing = _landing(project)
    _txt(landing / "a.txt")
    (landing / "b.csv").write_text(f"id,v\n1,{SECRET}\n")
    (landing / "c.bin").write_bytes(SECRET.encode() + b"\x00")
    caplog.set_level(logging.DEBUG)
    main(["demo"])
    main(["demo", "--pending"])
    main(["demo", "--delimiter", "|"])
    assert SECRET not in caplog.text
    assert "event=bronze_receive" in caplog.text
    assert "event=bronze_unparsed" in caplog.text


def _outside(project: Path) -> Path:
    outside = project / "outside.csv"
    outside.write_text(f"id,v\n9,{SECRET}\n")
    return outside


def test_symlink_is_not_received_parsed_or_counted_in_exit_code(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    (landing / "link.csv").symlink_to(_outside(project))
    (landing / "real.csv").write_text("id\n1\n")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 0

    assert len(_raw_files(project)) == 1
    assert len(_bronze_files(project)) == 1
    assert "event=bronze_ignored" in caplog.text
    assert "symlinks=1" in caplog.text
    assert SECRET not in caplog.text
    assert "link.csv" not in caplog.text


def test_only_symlinks_writes_nothing(project: Path) -> None:
    (_landing(project) / "link.csv").symlink_to(_outside(project))
    assert main(["demo"]) == 0
    assert not (project / "lake" / "raw").exists()


def test_pending_ignores_symlinks(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    (_landing(project) / "link.csv").symlink_to(_outside(project))
    caplog.set_level(logging.INFO)
    assert main(["demo", "--pending"]) == 0
    assert "event=bronze_pending " not in caplog.text


def test_receive_raw_refuses_symlink(project: Path) -> None:
    link = _landing(project) / "link.csv"
    link.symlink_to(_outside(project))
    with pytest.raises(ValueError, match="symlinks"):
        receive_raw(link, "demo", project / "lake", "0" * 64, RUN_AT)
    assert _raw_files(project) == []


def _config(project: Path, text: str) -> None:
    folder = project / "config" / "sources"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "demo.yaml").write_text(text)


@pytest.mark.parametrize("encoding", ["utf-16", "utf-16-le"])
def test_utf16_csv_with_config_encoding_parses(project: Path, encoding: str) -> None:
    (_landing(project) / "a.csv").write_bytes(f"id,code\n001,{SECRET}\n".encode(encoding))
    _config(project, f"encoding: {encoding}\n")
    assert main(["demo"]) == 0
    frame = pl.read_parquet(_bronze_files(project)[0])
    assert frame.columns[:2] == ["id", "code"]
    assert frame["id"].to_list() == ["001"]
    assert frame["code"].to_list() == [SECRET]


@pytest.mark.parametrize("encoding", ["utf-16", "utf-16-le", "utf-16-be"])
def test_utf16_csv_without_config_is_held_as_encoding(
    project: Path, caplog: pytest.LogCaptureFixture, encoding: str
) -> None:
    (_landing(project) / "a.csv").write_bytes(f"id,code\n1,{SECRET}\n".encode(encoding))
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 3
    assert 'reason="encoding"' in caplog.text
    assert 'reason="no_reader"' not in caplog.text
    assert _bronze_files(project) == []
    assert len(_raw_files(project)) == 1


def test_binary_with_nuls_and_csv_name_still_no_reader(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (_landing(project) / "a.csv").write_bytes(b"\x01\x00\x00\x00\x02\xff\x00\x07\x00\x00")
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 3
    assert 'reason="no_reader"' in caplog.text


@pytest.mark.parametrize("flag", [[], ["--pending"]])
def test_bad_config_format_exits_2_and_copies_nothing(
    project: Path, caplog: pytest.LogCaptureFixture, flag: list[str]
) -> None:
    (_landing(project) / "a.csv").write_text("id\n1\n")
    _config(project, "format: SECRETFORMAT\n")
    caplog.set_level(logging.INFO)
    assert main(["demo", *flag]) == 2
    assert not (project / "lake" / "raw").exists()
    assert "SECRETFORMAT" not in caplog.text
    assert "ValueError" in caplog.text


def _assert_held_as_encoding(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert main(["demo"]) == 3
    assert 'reason="encoding"' in caplog.text
    assert not (project / "lake" / "bronze").exists()
    assert len(_raw_files(project)) == 1
    assert SECRET not in caplog.text
    assert "utf-16" not in caplog.text
    caplog.clear()
    assert main(["demo", "--pending"]) == 3  # nothing was marked ingested
    assert "event=bronze_pending " in caplog.text
    assert not (project / "lake" / "bronze").exists()


@pytest.mark.parametrize(
    ("written", "configured"),
    [
        ("utf-16-le", "utf-16-be"),
        ("utf-16-be", "utf-16-le"),
        ("utf-32-le", "utf-32-be"),
        ("utf-32-be", "utf-32-le"),
        ("utf-8", "utf-16"),
        ("utf-16-le", "utf-16"),  # plain utf-16 needs a BOM
        ("utf-16-be", "utf-16"),
        ("utf-16", "latin-1"),
    ],
)
def test_wrong_config_encoding_is_held_and_writes_nothing(
    project: Path, caplog: pytest.LogCaptureFixture, written: str, configured: str
) -> None:
    (_landing(project) / "a.csv").write_bytes(f"id,code\n1,{SECRET}\n".encode(written))
    _config(project, f"encoding: {configured}\n")
    caplog.set_level(logging.INFO)
    _assert_held_as_encoding(project, caplog)


def test_correct_config_still_loads_values_unchanged(project: Path) -> None:
    (_landing(project) / "a.csv").write_bytes(f"id,code\n001,{SECRET}\n".encode("utf-16-be"))
    _config(project, "encoding: utf-16-be\n")
    assert main(["demo"]) == 0
    frame = pl.read_parquet(_bronze_files(project)[0])
    assert frame["id"].to_list() == ["001"]
    assert frame["code"].to_list() == [SECRET]


@pytest.mark.parametrize("padding", [0, 2_000_000])
def test_stray_nul_in_valid_utf8_loads_unchanged(project: Path, padding: int) -> None:
    rows = "".join(f"{n},filler\n" for n in range(padding // 10))
    stray = f"id,code\n1,AB\x00{SECRET}\n{rows}"
    if padding:
        stray += f"2,{SECRET}\x00\n"
    (_landing(project) / "a.csv").write_bytes(stray.encode())
    assert main(["demo"]) == 0
    frame = pl.read_parquet(_bronze_files(project)[0])
    assert frame["code"][0] == f"AB\x00{SECRET}"
    assert frame["code"][-1] == (f"{SECRET}\x00" if padding else f"AB\x00{SECRET}")


def test_stray_nul_with_invalid_utf8_is_held_as_encoding(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    rows = "".join(f"{n},filler\n" for n in range(200_000))
    data = f"id,code\n{rows}".encode() + b"9,\xff\x00\n"
    (_landing(project) / "a.csv").write_bytes(data)
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 3
    assert 'reason="encoding"' in caplog.text
    assert not (project / "lake" / "bronze").exists()


def test_symlinked_directory_is_skipped_and_counted(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    elsewhere = project / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "x.csv").write_text(f"id,v\n9,{SECRET}\n")
    (landing / "linked_dir").symlink_to(elsewhere, target_is_directory=True)
    (landing / "real.csv").write_text("id\n1\n")
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 0
    assert len(_raw_files(project)) == 1
    assert "symlinks=1" in caplog.text
    assert SECRET not in caplog.text


def test_symlink_to_target_inside_landing_is_skipped_and_counted(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(project)
    (landing / "real.csv").write_text("id\n1\n")
    (landing / "alias.csv").symlink_to(landing / "real.csv")
    caplog.set_level(logging.INFO)
    assert main(["demo"]) == 0
    assert len(_raw_files(project)) == 1
    assert len(_bronze_files(project)) == 1
    assert "symlinks=1" in caplog.text
