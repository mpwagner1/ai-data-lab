import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion.bronze import ingest_file, main
from ingestion.source_config import SourceConfig, load_source_config, parse_source_config

RUN_AT = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)


def _write(path: Path, sep: str, encoding: str = "utf-8") -> Path:
    text = f"id{sep}code\n001{sep}A1\n002{sep}B2\n"
    path.write_bytes(text.encode(encoding))
    return path


def _bronze(lake: Path, source: str = "demo") -> pl.DataFrame:
    return pl.read_parquet(sorted(lake.rglob("*.parquet")))


def _ids(lake: Path, source: str = "demo") -> list[str]:
    frame = _bronze(lake, source)
    assert {frame.schema[c] for c in ("id", "code")} == {pl.String}
    return sorted(frame["id"].to_list())


@pytest.mark.parametrize(("name", "sep"), [("a.psv", "|"), ("a.tsv", "\t")])
def test_extension_default_delimiters(tmp_path: Path, name: str, sep: str) -> None:
    ingest_file(_write(tmp_path / name, sep), "demo", tmp_path / "lake", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_with_config_delimiter(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    ingest_file(path, "demo", tmp_path / "lake", config=SourceConfig(delimiter="|"), run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_with_flag_delimiter(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    ingest_file(path, "demo", tmp_path / "lake", delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_txt_without_delimiter_fails(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    with pytest.raises(ValueError, match="no delimiter"):
        ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)


def test_flag_beats_config_for_txt(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.txt", "|")
    config = SourceConfig(delimiter=";")
    ingest_file(path, "demo", tmp_path / "lake", config=config, delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_flag_does_not_change_csv(tmp_path: Path) -> None:
    path = _write(tmp_path / "a.csv", ",")
    ingest_file(path, "demo", tmp_path / "lake", delimiter="|", run_at=RUN_AT)
    assert _ids(tmp_path / "lake") == ["001", "002"]


def test_cp1252_file_keeps_accents(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes("id,name\n001,caf\xe9\n".encode("cp1252"))
    ingest_file(
        path, "demo", tmp_path / "lake", config=SourceConfig(encoding="cp1252"), run_at=RUN_AT
    )
    assert _bronze(tmp_path / "lake")["name"].to_list() == ["café"]


def test_unknown_config_key_raises(tmp_path: Path) -> None:
    (tmp_path / "demo.yaml").write_text("delimeter: '|'\n")
    with pytest.raises(ValueError, match="unknown source config keys"):
        load_source_config("demo", tmp_path)


def test_config_loads_and_missing_file_is_default(tmp_path: Path) -> None:
    assert load_source_config("demo", tmp_path) == SourceConfig()
    (tmp_path / "demo.yaml").write_text("delimiter: '|'\nencoding: cp1252\nsheets: [A]\n")
    assert load_source_config("demo", tmp_path) == SourceConfig("|", "cp1252", None, ("A",))


@pytest.mark.parametrize("raw", [{"delimiter": "||"}, {"encoding": "nope"}, ["x"]])
def test_bad_config_values_raise(raw: object) -> None:
    with pytest.raises(ValueError):
        parse_source_config(raw)


@pytest.mark.parametrize("flag", ["||", ""])
def test_multi_character_flag_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["demo", "--delimiter", flag]) == 2


def test_cli_loads_txt_with_flag_and_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "data" / "landing" / "demo"
    landing.mkdir(parents=True)
    _write(landing / "a.txt", "|")
    _write(landing / "b.psv", "|").write_text("id|code\n003|C3\n")
    monkeypatch.chdir(tmp_path)

    assert main(["demo", "--delimiter", "|"]) == 0

    frame = _bronze(tmp_path / "lake")
    assert len(list((tmp_path / "lake").rglob("*.parquet"))) == 2
    assert {"id", "code"} <= set(frame.columns)
    assert sorted(frame["id"].to_list()) == ["001", "002", "003"]
    assert sorted(frame["code"].to_list()) == ["A1", "B2", "C3"]


def _config(tmp_path: Path, text: str, source: str = "demo") -> Path:
    (tmp_path / f"{source}.yaml").write_text(text)
    return tmp_path


@pytest.mark.parametrize(
    "text",
    ["delimiter: 5\n", "encoding: 7\n", "sheets: abc\n", "sheets: [1, 2]\n", "- a\n- b\n", "x"],
)
def test_wrong_typed_config_raises_value_error(tmp_path: Path, text: str) -> None:
    with pytest.raises(ValueError):
        load_source_config("demo", _config(tmp_path, text))


def test_malformed_yaml_error_has_no_config_text(tmp_path: Path) -> None:
    root = _config(tmp_path, "delimiter: [SECRETVALUE\n  : : {\n")
    with pytest.raises(ValueError) as info:
        load_source_config("demo", root)
    assert "SECRETVALUE" not in str(info.value)
    assert "demo.yaml" in str(info.value)


def test_config_uses_safe_load(tmp_path: Path) -> None:
    marker = tmp_path / "executed"
    text = (
        f"!!python/object/apply:pathlib.Path.touch [!!python/object/apply:pathlib.Path [{marker}]]"
    )
    with pytest.raises(ValueError):
        load_source_config("demo", _config(tmp_path, text))
    assert not marker.exists()


@pytest.mark.parametrize("delimiter", ["§", '"', "\n", "\r", "é"])
def test_bad_delimiters_rejected(delimiter: str) -> None:
    with pytest.raises(ValueError):
        parse_source_config({"delimiter": delimiter})


@pytest.mark.parametrize("encoding", ["hex", "base64", "zlib", "rot13", "undefined", "nope"])
def test_non_text_or_unknown_encoding_rejected(encoding: str) -> None:
    with pytest.raises(ValueError) as error:
        parse_source_config({"encoding": encoding})
    assert encoding not in str(error.value)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "cp1252", "latin-1"])
def test_text_encodings_accepted(encoding: str) -> None:
    assert parse_source_config({"encoding": encoding}).encoding == encoding


def test_unknown_key_error_does_not_echo_key_names() -> None:
    with pytest.raises(ValueError, match="unknown source config keys") as error:
        parse_source_config({"SECRETKEY": 1})
    assert "SECRETKEY" not in str(error.value)


def _landing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    landing = tmp_path / "data" / "landing" / "demo"
    landing.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return landing


def test_main_bad_config_exits_2_without_config_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    _write(landing / "a.csv", ",")
    config_dir = tmp_path / "config" / "sources"
    config_dir.mkdir(parents=True)
    (config_dir / "demo.yaml").write_text("delimiter: SECRETVALUE\n")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 2

    assert "SECRETVALUE" not in caplog.text
    assert "event=bronze_error" in caplog.text
    assert not (tmp_path / "lake").exists()


def test_main_malformed_yaml_exits_2_without_config_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    _write(landing / "a.csv", ",")
    config_dir = tmp_path / "config" / "sources"
    config_dir.mkdir(parents=True)
    (config_dir / "demo.yaml").write_text("key: [SECRETVALUE\n  : {\n")
    caplog.set_level(logging.INFO)

    assert main(["demo"]) == 2

    assert "SECRETVALUE" not in caplog.text


def test_main_missing_landing_still_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["demo"]) == 2


@pytest.mark.parametrize("flag", ["§", "ab", '"', "\n"])
def test_bad_flag_exits_2_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: str
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    _write(landing / "a.txt", "|")

    assert main(["demo", "--delimiter", flag]) == 2

    assert not (tmp_path / "lake").exists()


def test_semicolon_flag_works_on_txt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    landing = _landing(tmp_path, monkeypatch)
    _write(landing / "a.txt", ";")

    assert main(["demo", "--delimiter", ";"]) == 0

    assert _ids(tmp_path / "lake") == ["001", "002"]


@pytest.mark.parametrize(("name", "sep"), [("a.csv", ","), ("a.tsv", "\t"), ("a.psv", "|")])
@pytest.mark.parametrize("flag", [";", "|", "\t"])
def test_delimiter_flag_is_ignored_for_files_with_an_extension_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, sep: str, flag: str
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    _write(landing / name, sep)

    assert main(["demo", "--delimiter", flag]) == 0

    assert _ids(tmp_path / "lake") == ["001", "002"]
    assert list(_bronze(tmp_path / "lake").columns[:2]) == ["id", "code"]


def test_header_only_non_utf8_file_loads_as_zero_rows_with_its_columns(tmp_path: Path) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes("id,café\n".encode("cp1252"))

    result = ingest_file(
        path, "demo", tmp_path / "lake", config=SourceConfig(encoding="cp1252"), run_at=RUN_AT
    )

    frame = _bronze(tmp_path / "lake")
    assert (result.rows, frame.height) == (0, 0)
    assert frame.columns[:2] == ["id", "café"]


@pytest.mark.parametrize("encoding", ["cp1252", "utf-16", "utf-16-le", "utf-16-be", "utf-32-le"])
def test_flag_delimiter_and_config_encoding_work_together_on_txt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, encoding: str
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    (landing / "a.txt").write_bytes("id|name\n001|café\n002|zoë\n".encode(encoding))
    config = tmp_path / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text(f"encoding: {encoding}\n")

    assert main(["demo", "--delimiter", "|"]) == 0

    frame = _bronze(tmp_path / "lake")
    assert frame["id"].to_list() == ["001", "002"]
    assert frame["name"].to_list() == ["café", "zoë"]


def test_config_delimiter_and_encoding_work_together_on_dat(tmp_path: Path) -> None:
    path = tmp_path / "a.dat"
    path.write_bytes("id;name\n001;café\n".encode("utf-16-le"))
    config = SourceConfig(delimiter=";", encoding="utf-16-le")

    ingest_file(path, "demo", tmp_path / "lake", config=config, run_at=RUN_AT)

    assert _bronze(tmp_path / "lake")["name"].to_list() == ["café"]


@pytest.mark.parametrize("bom", [b"\xff\xfe", b"\xfe\xff"], ids=["bom_le", "bom_be"])
def test_cli_loads_utf16_file_with_bom_and_keeps_header_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bom: bytes
) -> None:
    landing = _landing(tmp_path, monkeypatch)
    order = "utf-16-le" if bom == b"\xff\xfe" else "utf-16-be"
    (landing / "a.csv").write_bytes(bom + "id,code\n001,A1\n".encode(order))
    config = tmp_path / "config" / "sources"
    config.mkdir(parents=True)
    (config / "demo.yaml").write_text("encoding: utf-16\n")

    assert main(["demo"]) == 0

    frame = _bronze(tmp_path / "lake")
    assert frame.columns[:2] == ["id", "code"]  # the BOM is not part of the first column name
    assert frame["id"].to_list() == ["001"]
