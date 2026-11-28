"""Every suppression has a reason, an owner and an expiry date (docs/risk-register.toml)."""

from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from risk_register import (
    check_register,
    find_suppressions,
    is_code,
    load_entries,
    scan_text,
    tracked_files,
)

ROOT = Path(__file__).resolve().parents[1]
REGISTER = ROOT / "docs" / "risk-register.toml"
TODAY = date(2030, 1, 15)
KEY = ("src/a.py", "noqa", "X001")


def _entry(**changes: object) -> dict[str, Any]:
    entry = {
        "file": KEY[0],
        "kind": KEY[1],
        "rule": KEY[2],
        "count": 1,
        "reason": "synthetic reason",
        "owner": "synthetic-owner",
        "expires": TODAY + timedelta(days=30),
    }
    return {**entry, **changes}


def _without(field: str) -> dict[str, Any]:
    return {name: value for name, value in _entry().items() if name != field}


def test_register_covers_every_suppression_and_nothing_has_expired() -> None:
    found = find_suppressions(ROOT, tracked_files(ROOT))

    assert check_register(load_entries(REGISTER), found, date.today()) == []


def test_a_complete_entry_passes() -> None:
    assert check_register([_entry()], Counter({KEY: 1}), TODAY) == []


def test_an_entry_is_still_valid_on_its_expiry_date() -> None:
    assert check_register([_entry(expires=TODAY)], Counter({KEY: 1}), TODAY) == []


def test_an_expired_entry_fails() -> None:
    entry = _entry(expires=TODAY - timedelta(days=1))

    [problem] = check_register([entry], Counter({KEY: 1}), TODAY)

    assert "expired on 2030-01-14" in problem


@pytest.mark.parametrize("field", ["reason", "owner", "expires"])
def test_an_entry_missing_a_required_field_fails(field: str) -> None:
    [problem] = check_register([_without(field)], Counter({KEY: 1}), TODAY)

    assert field in problem


@pytest.mark.parametrize(
    "changes",
    [
        {"reason": "  "},
        {"owner": 7},
        {"expires": "2031-01-01"},
        {"expires": datetime(2031, 1, 1, 12, 0)},
        {"count": 0},
        {"count": True},
        {"max_count": 2},
    ],
)
def test_an_entry_with_an_invalid_value_fails(changes: dict[str, object]) -> None:
    assert check_register([_entry(**changes)], Counter({KEY: 1}), TODAY) != []


def test_an_unlisted_suppression_fails() -> None:
    [problem] = check_register([], Counter({KEY: 1}), TODAY)

    assert "not listed" in problem


@pytest.mark.parametrize("found", [0, 2])
def test_a_count_that_does_not_match_fails(found: int) -> None:
    [problem] = check_register([_entry()], Counter({KEY: found}), TODAY)

    assert f"found {found}" in problem


@pytest.mark.parametrize(("found", "passes"), [(0, True), (1, True), (2, False)])
def test_max_count_allows_up_to_the_maximum(found: int, passes: bool) -> None:
    entry = {**_without("count"), "max_count": 1}

    assert (check_register([entry], Counter({KEY: found}), TODAY) == []) is passes


def test_a_duplicate_entry_fails() -> None:
    [problem] = check_register([_entry(), _entry()], Counter({KEY: 1}), TODAY)

    assert "duplicate" in problem


@pytest.mark.parametrize(
    ("name", "text", "kind", "rule"),
    [
        ("a.py", "x = 1  # noqa\n", "noqa", "*"),
        ("a.py", "x = 1  # NOQA: E501 - why\n", "noqa", "E501"),
        ("a.py", "# ruff: noqa: E501\n", "noqa-file", "E501"),
        ("a.py", "# ruff: disable[E501]\n", "ruff-comment", "E501"),
        ("a.py", "x = 1  # type: ignore\n", "type-ignore", "*"),
        ("a.py", "x = 1  # type: ignore[index]\n", "type-ignore", "index"),
        ("a.py", "x = 1  # pyright: ignore[reportX]\n", "type-ignore", "reportX"),
        ("a.py", "x = 1  # nosec B101\n", "nosec", "B101"),
        ("a.py", "pytest.skip('no')\n", "pytest-skip", "skip"),
        ("a.py", "pytest.importorskip('x')\n", "pytest-skip", "importorskip"),
        ("a.py", "@pytest.mark.skip\ndef f(): ...\n", "pytest-skip", "mark.skip"),
        ("a.py", "@pytest.mark.skipif(True)\ndef f(): ...\n", "pytest-skip", "mark.skipif"),
        ("a.py", "pytestmark = pytest.mark.xfail(strict=True)\n", "pytest-skip", "mark.xfail"),
        ("a.py", "from pytest import skip\n", "pytest-import", "skip"),
        ("a.py", "import pytest as pt\n", "pytest-import", "alias"),
        ("a.sql", "select 1 -- noqa: LT01\n", "noqa", "LT01"),
        ("a.sql", "-- noqa: disable=all\n", "noqa", "all"),
        ("w.yml", "on: # zizmor: ignore[some-audit]\n", "zizmor-ignore", "some-audit"),
        ("w.yml", "run: pip-audit --ignore-vuln GHSA-aaaa\n", "ignore-vuln", "GHSA-aaaa"),
        ("w.yml", "ignore-vulns: |\n  PYSEC-1\n", "ignore-vuln", "PYSEC-1"),
        (".sqlfluff", "[sqlfluff]\nexclude_rules = LT05\n", "sqlfluff-exclude-rules", "LT05"),
        (
            "pyproject.toml",
            "[tool.sqlfluff.core]\nexclude_rules = 'LT05'\n",
            "sqlfluff-exclude-rules",
            "LT05",
        ),
        ("pyproject.toml", "[tool.ruff.lint]\nignore = ['E501']\n", "ruff-ignore", "E501"),
        ("ruff.toml", "extend-ignore = ['E501']\n", "ruff-ignore", "E501"),
        (
            "pyproject.toml",
            "[tool.ruff.lint.per-file-ignores]\n'tests/*' = ['B017']\n",
            "ruff-per-file-ignore",
            "tests/*:B017",
        ),
    ],
)
def test_detects_each_kind_of_suppression(name: str, text: str, kind: str, rule: str) -> None:
    assert scan_text(name, text) == Counter({(name, kind, rule): 1})


def test_each_rule_in_one_comment_is_counted() -> None:
    found = scan_text("a.py", "x = 1  # type: ignore[arg-type,return-value]  # noqa: E501, B017\n")

    assert found == Counter(
        {
            ("a.py", "type-ignore", "arg-type"): 1,
            ("a.py", "type-ignore", "return-value"): 1,
            ("a.py", "noqa", "E501"): 1,
            ("a.py", "noqa", "B017"): 1,
        }
    )


def test_a_marker_inside_a_python_string_is_not_a_suppression() -> None:
    text = 'x = "# noqa and # type: ignore"\ny = "pytest.skip --ignore-vuln"\n'

    assert scan_text("a.py", text) == Counter()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("docs/plans/x.md", False),
        ("docs/risk-register.toml", False),
        ("README.md", False),
        ("src/a.py", True),
        (".sqlfluff", True),
        (".github/workflows/w.yml", True),
    ],
)
def test_docs_and_markdown_are_not_code(name: str, expected: bool) -> None:
    assert is_code(name) is expected


def test_a_new_suppression_in_a_covered_file_fails(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("a = 1  # noqa: X001\nb = 2  # noqa: X001\n")

    found = find_suppressions(tmp_path, ["src/a.py", "src/deleted.py"])

    [problem] = check_register([_entry()], found, TODAY)
    assert "found 2" in problem
