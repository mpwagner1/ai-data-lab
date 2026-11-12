import logging
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from ingestion.bronze import ingest_file, main
from ingestion.json_reader import JsonReadError, read_json, read_ndjson
from ingestion.parquet_reader import ParquetReadError, read_parquet_as_strings
from ingestion.source_config import SourceConfig

RUN_AT = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)
SECRET = "SECRETVALUE"
SECRET_KEY = "SecretKeyName"
CONFIG = SourceConfig()


def _json(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _frame(parts: list) -> pl.DataFrame:
    [part] = parts
    assert part.sheet_name is None
    return part.frame.collect()


def _read_json(tmp_path: Path, text: str) -> pl.DataFrame:
    return _frame(read_json(_json(tmp_path / "d.json", text), CONFIG, None))


def _read_ndjson(tmp_path: Path, text: str) -> pl.DataFrame:
    return _frame(read_ndjson(_json(tmp_path / "d.ndjson", text), CONFIG, None))


def test_nested_values_arrive_as_json_strings(tmp_path: Path) -> None:
    text = '[{"id": 1, "o": {"b": 1.50, "a": [1, "é", null, true]}, "l": [{"x": 1e3}]}]'
    frame = _read_json(tmp_path, text)
    assert frame.schema == {"id": pl.String, "o": pl.String, "l": pl.String}
    assert frame.row(0) == ("1", '{"b":1.50,"a":[1,"é",null,true]}', '[{"x":1e3}]')


def test_numbers_keep_source_text_and_bools_and_nulls(tmp_path: Path) -> None:
    text = '[{"a": 1.50, "b": 1e3, "c": 12345678901234567890, "d": true, "e": false, "f": null}]'
    frame = _read_json(tmp_path, text)
    assert frame.row(0) == ("1.50", "1e3", "12345678901234567890", "true", "false", None)
    assert set(frame.schema.values()) == {pl.String}


def test_strings_are_kept_exactly(tmp_path: Path) -> None:
    frame = _read_json(tmp_path, '[{"a": "  007 ", "b": ""}]')
    assert frame.row(0) == ("  007 ", "")


def test_union_of_keys_in_first_seen_order(tmp_path: Path) -> None:
    frame = _read_json(tmp_path, '[{"a": "1"}, {"b": "2", "a": "3"}, {"c": "4"}]')
    assert frame.columns == ["a", "b", "c"]
    assert frame.rows() == [("1", None, None), ("3", "2", None), (None, None, "4")]


def test_duplicate_key_keeps_last(tmp_path: Path) -> None:
    assert _read_json(tmp_path, '[{"a": "1", "a": "2"}]').row(0) == ("2",)


def test_bom_is_not_part_of_first_key(tmp_path: Path) -> None:
    path = tmp_path / "d.json"
    path.write_bytes(b'\xef\xbb\xbf[{"id": "1"}]')
    assert _frame(read_json(path, CONFIG, None)).columns == ["id"]
    nd = tmp_path / "d.ndjson"
    nd.write_bytes(b'\xef\xbb\xbf{"id": "1"}\n')
    assert _frame(read_ndjson(nd, CONFIG, None)).columns == ["id"]


@pytest.mark.parametrize("text", ['[{"_file_name": "x"}]', '[{"ok": 1}, {"_x": 2}]'])
def test_underscore_key_rejected_without_key_text(tmp_path: Path, text: str) -> None:
    with pytest.raises(JsonReadError) as info:
        _read_json(tmp_path, text)
    assert "_file_name" not in str(info.value)
    assert "_x" not in str(info.value)


@pytest.mark.parametrize("text", ["[]", "", "  \n", "[\n]"])
def test_empty_json_raises(tmp_path: Path, text: str) -> None:
    with pytest.raises(JsonReadError, match="empty file"):
        _read_json(tmp_path, text)


def test_empty_ndjson_raises(tmp_path: Path) -> None:
    with pytest.raises(JsonReadError, match="empty file"):
        _read_ndjson(tmp_path, "\n\n")


@pytest.mark.parametrize("text", ['{"a": 1}', '"text"', "42", "[1, 2]", '[{"a": 1}, "x"]'])
def test_json_wrong_shape_fails(tmp_path: Path, text: str) -> None:
    with pytest.raises(JsonReadError):
        _read_json(tmp_path, text)


def test_ndjson_non_object_line_fails(tmp_path: Path) -> None:
    with pytest.raises(JsonReadError):
        _read_ndjson(tmp_path, '{"a": 1}\n[1, 2]\n')


def test_ndjson_blank_lines_skipped_and_union(tmp_path: Path) -> None:
    frame = _read_ndjson(tmp_path, '\n{"a": 1.0}\n\n  \n{"b": {"c": 2}}\n')
    assert frame.columns == ["a", "b"]
    assert frame.rows() == [("1.0", None), (None, '{"c":2}')]


def test_non_utf8_json_is_a_unicode_error(tmp_path: Path) -> None:
    path = tmp_path / "d.json"
    path.write_bytes('[{"a": "é"}]'.encode("cp1252"))
    with pytest.raises(UnicodeError):
        read_json(path, CONFIG, None)
    assert _frame(read_json(path, SourceConfig(encoding="cp1252"), None)).row(0) == ("é",)


def _bronze(lake: Path) -> pl.DataFrame:
    [file] = sorted(lake.rglob("*.parquet"))
    return pl.read_parquet(file)


def test_json_through_ingest_file_has_metadata(tmp_path: Path) -> None:
    path = _json(tmp_path / "d.json", '[{"id": "1"}, {"id": "2"}]')
    result = ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)
    frame = _bronze(tmp_path / "lake")
    assert result.rows == 2
    assert frame.columns == [
        "id",
        "_file_name",
        "_ingested_at",
        "_file_hash",
        "_row_number",
        "_sheet_name",
    ]
    assert frame["_row_number"].to_list() == [1, 2]
    assert frame["_sheet_name"].to_list() == [None, None]


def test_detection_picks_json_and_ndjson_by_shape(tmp_path: Path) -> None:
    arr = _json(tmp_path / "a.json", '[{"id": "1"}]')
    nd = _json(tmp_path / "b.json", '{"id": "1"}\n{"id": "2"}\n')
    assert ingest_file(arr, "demo", tmp_path / "lake", run_at=RUN_AT).rows == 1
    assert ingest_file(nd, "demo", tmp_path / "lake", run_at=RUN_AT).rows == 2


# Parquet


def _parquet(path: Path, frame: pl.DataFrame) -> Path:
    frame.write_parquet(path)
    return path


def test_parquet_types_become_documented_strings(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        {
            "d": [date(2020, 1, 2), None],
            "ts": [datetime(2020, 1, 2, 3, 4, 5, 123456), None],
            "tz": [datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC), None],
            "dec": [Decimal("1.50"), None],
            "f": [0.1, float("nan")],
            "big": [1e20, 1.0],
            "b": [True, False],
            "i": [42, None],
            "t": [time(1, 2, 3), None],
            "bin": [b"\x00ab", None],
            "st": [{"a": 1, "b": "x"}, None],
            "li": [[1, 2], None],
            "deep": [[{"d": date(2020, 1, 2), "n": Decimal("1.50")}], None],
        }
    )
    out = _frame(read_parquet_as_strings(_parquet(tmp_path / "d.parquet", frame), CONFIG, None))
    assert set(out.schema.values()) == {pl.String}
    assert out.row(0) == (
        "2020-01-02",
        "2020-01-02 03:04:05.123456",
        "2020-01-02 03:04:05.000000+00:00",
        "1.50",
        "0.1",
        "1e+20",
        "true",
        "42",
        "01:02:03",
        "006162",
        '{"a":1,"b":"x"}',
        "[1,2]",
        '[{"d":"2020-01-02","n":"1.50"}]',
    )
    assert out.row(1) == (
        None,
        None,
        None,
        None,
        "NaN",
        "1.0",
        "false",
        None,
        None,
        None,
        None,
        None,
        None,
    )


def test_nested_binary_fails_cleanly(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "d.parquet", pl.DataFrame({"x": [[b"ab"]]}))
    with pytest.raises(ParquetReadError):
        read_parquet_as_strings(path, CONFIG, None)


def test_zero_row_parquet_is_written_as_zero_row_file(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "d.parquet", pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)}))
    result = ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)
    frame = _bronze(tmp_path / "lake")
    assert result.rows == 0
    assert frame.height == 0
    assert frame.schema["a"] == pl.String


def test_underscore_parquet_column_rejected(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "d.parquet", pl.DataFrame({"_file_name": ["x"]}))
    with pytest.raises(ParquetReadError) as info:
        read_parquet_as_strings(path, CONFIG, None)
    assert "_file_name" not in str(info.value)


def test_bronze_parquet_as_input_is_rejected(tmp_path: Path) -> None:
    src = _json(tmp_path / "d.json", '[{"id": "1"}]')
    ingest_file(src, "demo", tmp_path / "lake", run_at=RUN_AT)
    [bronze] = tmp_path.joinpath("lake").rglob("*.parquet")
    with pytest.raises(ParquetReadError, match="starts with"):
        read_parquet_as_strings(bronze, CONFIG, None)


def test_zero_column_parquet_fails(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "d.parquet", pl.DataFrame())
    with pytest.raises(ParquetReadError):
        read_parquet_as_strings(path, CONFIG, None)


def test_parquet_through_ingest_file(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "d.parquet", pl.DataFrame({"a": [1, 2]}))
    ingest_file(path, "demo", tmp_path / "lake", run_at=RUN_AT)
    frame = _bronze(tmp_path / "lake")
    assert frame["a"].to_list() == ["1", "2"]
    assert frame["_row_number"].to_list() == [1, 2]


# Whole CLI


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "data" / "landing" / "demo").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_cli_loads_json_into_one_parquet(project: Path, caplog: pytest.LogCaptureFixture) -> None:
    landing = project / "data" / "landing" / "demo"
    _json(landing / "d.json", f'[{{"{SECRET_KEY}": "{SECRET}", "n": 1.50}}]')
    with caplog.at_level(logging.INFO):
        assert main(["demo"]) == 0
    frame = _bronze(project / "lake" / "bronze")
    assert frame.columns[-5:] == [
        "_file_name",
        "_ingested_at",
        "_file_hash",
        "_row_number",
        "_sheet_name",
    ]
    assert frame["_sheet_name"].to_list() == [None]
    assert frame["n"].to_list() == ["1.50"]
    assert SECRET not in caplog.text
    assert SECRET_KEY not in caplog.text


def test_bad_files_fail_alone_and_batch_continues(
    project: Path, caplog: pytest.LogCaptureFixture
) -> None:
    landing = project / "data" / "landing" / "demo"
    _json(landing / "a_good.json", '[{"id": "1"}]')
    _json(landing / "b_bad.json", f'[{{"{SECRET_KEY}": "{SECRET}", ')
    (landing / "c_trunc.parquet").write_bytes(b"PAR1" + SECRET.encode() + b"PAR1")
    _json(landing / "d_ndjson.ndjson", f'{{"a": "{SECRET}"}}\n[1]\n')
    with caplog.at_level(logging.INFO):
        assert main(["demo"]) == 1
    frame = _bronze(project / "lake" / "bronze")
    assert frame["id"].to_list() == ["1"]
    raw = sorted(p.name for p in (project / "lake" / "raw").rglob("*") if p.is_file())
    assert len(raw) == 4
    assert SECRET not in caplog.text
    assert SECRET_KEY not in caplog.text
    assert caplog.text.count("event=bronze_error") == 3
    assert "event=bronze_unparsed" not in caplog.text
