import codecs
import itertools
from pathlib import Path

import polars as pl
import pytest

from ingestion.csv_reader import _check_decoded, read_csv_as_strings

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_all_columns_are_strings() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert set(frame.dtypes) == {pl.String}


def test_leading_zeros_and_scientific_notation_survive() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert frame["id"].to_list() == ["001", "002", "003"]
    assert frame["amount"].to_list() == ["1e3", "007.50", "0"]


def test_empty_field_reads_as_null() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_leading_zeros.csv").collect()
    assert frame["code"].to_list() == ["A1", "B2", None]


def test_empty_file_raises() -> None:
    with pytest.raises(ValueError, match="empty file"):
        read_csv_as_strings(FIXTURES / "empty.csv")


def test_whitespace_only_file_raises_value_error() -> None:
    with pytest.raises(ValueError, match="empty file"):
        read_csv_as_strings(FIXTURES / "blank_lines_only.csv")


def test_header_only_file_gives_zero_rows() -> None:
    frame = read_csv_as_strings(FIXTURES / "orders_header_only.csv").collect()
    assert frame.height == 0
    assert frame.columns == ["id", "code", "amount"]


def test_underscore_prefixed_column_is_rejected_by_name() -> None:
    with pytest.raises(ValueError, match="_secret"):
        read_csv_as_strings(FIXTURES / "orders_underscore_column.csv")


# Correct UTF-16 text whose characters have a zero low byte (U+4E00, U+AC00) must load.
_CJK_ROWS = {"cjk": ("名,都", "一", "一一一一"), "hangul": ("이름,도시", "가", "가가가가")}


def _cjk_text(kind: str, lines: int) -> tuple[str, list[tuple[str, str]]]:
    header, first, second = _CJK_ROWS[kind]
    rows = [(first, second)] * lines
    return header + "\n" + "".join(f"{a},{b}\n" for a, b in rows), rows


def _read_rows(path: Path, encoding: str) -> list[tuple[str, ...]]:
    return read_csv_as_strings(path, encoding=encoding).collect().rows()


@pytest.mark.parametrize("kind", ["cjk", "hangul"])
@pytest.mark.parametrize("lines", [1, 3000], ids=["small", "large"])
@pytest.mark.parametrize(
    "encoding",
    [
        "utf-16-le",
        "utf-16-be",
        "utf-32-le",
        "utf-32-be",
    ],
)
def test_correct_explicit_endian_cjk_file_loads_unchanged(
    tmp_path: Path, encoding: str, lines: int, kind: str
) -> None:
    text, rows = _cjk_text(kind, lines)
    path = tmp_path / "a.csv"
    path.write_bytes(text.encode(encoding))

    assert _read_rows(path, encoding) == rows


@pytest.mark.parametrize("kind", ["cjk", "hangul"])
@pytest.mark.parametrize("lines", [1, 3000], ids=["small", "large"])
@pytest.mark.parametrize("encoding", ["utf-16", "utf-32"])
@pytest.mark.parametrize("big_endian", [False, True], ids=["bom_le", "bom_be"])
def test_cjk_file_with_bom_loads_unchanged_with_plain_codec(
    tmp_path: Path, encoding: str, big_endian: bool, lines: int, kind: str
) -> None:
    text, rows = _cjk_text(kind, lines)
    order = "-be" if big_endian else "-le"
    bom = {"utf-16": codecs.BOM_UTF16_BE, "utf-32": codecs.BOM_UTF32_BE}[encoding]
    le_bom = {"utf-16": codecs.BOM_UTF16_LE, "utf-32": codecs.BOM_UTF32_LE}[encoding]
    path = tmp_path / "a.csv"
    path.write_bytes((bom if big_endian else le_bom) + text.encode(encoding + order))

    assert _read_rows(path, encoding) == rows


_SCRIPTS = {
    "latin": ("nom,ville", "José,Zürich"),
    "cyrillic": ("имя,город", "Анна,Москва"),
    "greek": ("όνομα,πόλη", "Ελένη,Αθήνα"),
    "arabic": ("الاسم,المدينة", "علي,القاهرة"),
    "hebrew": ("שם,עיר", "דנה,חיפה"),
    "hangul": ("이름,도시", "가나,다라"),
    "cjk": ("名,都", "一一,三〇〇〇"),
    "cjk_ascii": ("name,名", "A1,一二"),
    "emoji": ("a,b", "😀,🎉"),
}
_ORDERS = [("utf-16-le", "utf-16-be"), ("utf-16-be", "utf-16-le")]
_ORDERS += [("utf-32-le", "utf-32-be"), ("utf-32-be", "utf-32-le")]


@pytest.mark.parametrize("script", sorted(_SCRIPTS))
@pytest.mark.parametrize(("written", "read_as"), _ORDERS)
def test_scripts_correct_order_loads_and_wrong_order_is_held(
    tmp_path: Path, script: str, written: str, read_as: str
) -> None:
    header, row = _SCRIPTS[script]
    path = tmp_path / "a.csv"
    path.write_bytes(f"{header}\n{row}\n".encode(written))

    assert _read_rows(path, written) == [tuple(row.split(","))]
    with pytest.raises(UnicodeError):
        read_csv_as_strings(path, encoding=read_as)


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be"])
@pytest.mark.parametrize("char", ["一", "　"])
def test_many_zero_low_byte_chars_with_one_newline_load(
    tmp_path: Path, encoding: str, char: str
) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes(f"h\n{char * 5000}\n".encode(encoding))

    assert _read_rows(path, encoding) == [(char * 5000,)]


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be"])
def test_file_with_only_a_newline_as_ascii_is_decided(tmp_path: Path, encoding: str) -> None:
    other = "utf-16-be" if encoding == "utf-16-le" else "utf-16-le"
    path = tmp_path / "a.csv"
    path.write_bytes("名前\n一二\n".encode(encoding))

    assert _read_rows(path, encoding) == [("一二",)]
    with pytest.raises(UnicodeError):
        read_csv_as_strings(path, encoding=other)


@pytest.mark.parametrize("encoding", ["utf-16-le", "utf-16-be"])
@pytest.mark.parametrize("text", ["名前", "一一一", "이름", "😀"])
def test_single_line_without_ascii_is_undecidable_and_held(
    tmp_path: Path, encoding: str, text: str
) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes(text.encode(encoding))

    with pytest.raises(UnicodeError):
        read_csv_as_strings(path, encoding=encoding)


@pytest.mark.parametrize(
    ("written", "bom"), [("utf-16-le", codecs.BOM_UTF16_BE), ("utf-16-be", codecs.BOM_UTF16_LE)]
)
def test_bom_contradicting_the_codec_is_held(tmp_path: Path, written: str, bom: bytes) -> None:
    path = tmp_path / "a.csv"
    path.write_bytes(bom + "id,name\n1,x\n".encode(written))

    with pytest.raises(UnicodeError):
        read_csv_as_strings(path, encoding=written)


def _held(raw: bytes, encoding: str) -> bool:
    try:
        _check_decoded(raw, raw.decode(encoding), encoding)
    except UnicodeError:
        return True
    return False


_SWEEP_ALPHABET = ["a", ",", "\n", "\u4e00", "\u2c00", "\u0a00", "\u3000", "名", "가"]
_SWEEP = ["".join(t) for k in (1, 2, 3) for t in itertools.product(_SWEEP_ALPHABET, repeat=k)]


def _may_load_wrong_endian(text: str) -> bool:
    """The documented undecidable class: a wrong-endian read of these may load."""
    if not any(char in "a,\n" for char in text):
        return True  # no ASCII at all
    if any(char in "\u2c00\u0a00" for char in text):
        return True  # look-alike comma/newline outvotes the real ones
    no_structural = "," not in text and "\n" not in text
    return no_structural and any(char in "\u4e00\u3000" for char in text)  # look-alike letters


@pytest.mark.parametrize(("written", "read_as"), _ORDERS[:2])
def test_short_string_sweep_wrong_endian_holds_and_correct_never_alters(
    written: str, read_as: str
) -> None:
    """Wrong-endian reads hold except in `_may_load_wrong_endian`; correct reads never alter."""
    loaded_wrong = []
    for text in _SWEEP:
        raw = text.encode(written)
        if not _held(raw, written):
            assert raw.decode(written) == text
        if not _held(raw, read_as) and not _may_load_wrong_endian(text):
            loaded_wrong.append(text)
    assert loaded_wrong == []


def test_ties_hold_wrong_endian_file() -> None:
    assert _held("一a".encode("utf-16-le"), "utf-16-be")


def test_control_code_units_do_not_vote_in_the_letter_fallback() -> None:
    raw = "a\u0100\u0100".encode("utf-16-le")  # U+0100 read swapped is the control code 0x01

    assert not _held(raw, "utf-16-le")
