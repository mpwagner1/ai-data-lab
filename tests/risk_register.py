"""Find suppressions in tracked files and compare them with docs/risk-register.toml."""

import ast
import configparser
import io
import re
import subprocess
import tokenize
import tomllib
from collections import Counter
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

# (file, kind, rule)
Key = tuple[str, str, str]
# (kind, rule)
Hit = tuple[str, str]

ANY_RULE = "*"
_RULE_LIST = r"[A-Z]+\d*(?:\s*,\s*[A-Z]+\d*|\s+[A-Z]+\d+)*"
_COMMENT_PATTERNS: dict[str, re.Pattern[str]] = {
    "noqa": re.compile(rf"(?:#|--)\s*(?i:noqa)\b(?:\s*:\s*(?:disable=)?({_RULE_LIST}|all))?"),
    "noqa-file": re.compile(
        rf"(?:#|--)\s*(?:ruff|flake8)\s*:\s*(?i:noqa)\b(?:\s*:\s*({_RULE_LIST}))?"
    ),
    "ruff-comment": re.compile(r"#\s*ruff\s*:\s*(?:disable|ignore|file-ignore)\s*\[([^\]]*)\]"),
    "type-ignore": re.compile(r"#\s*(?:type|pyright|ty)\s*:\s*ignore\b(?:\s*\[([^\]]*)\])?"),
    "nosec": re.compile(r"#\s*nosec\b(?:\s*:?\s*(B\d+(?:[\s,]+B\d+)*))?"),
    "zizmor-ignore": re.compile(r"#\s*zizmor\s*:\s*ignore\s*\[([^\]]*)\]"),
}
# Command-line and CI inputs, so they are not comments. Looked for outside Python only.
_IGNORE_VULN = re.compile(r"ignore-vulns?\b(?:[\s=:,'\"|-]*((?:GHSA|PYSEC|CVE)-[A-Za-z0-9-]+))?")
_PYTEST_SKIPS = re.compile(r"pytest\.((?:mark\.)?(?:skip|skipif|xfail)|importorskip)")
_INI_NAMES = {".sqlfluff", "setup.cfg", "tox.ini", "pep8.ini"}
_RUFF_TOML_NAMES = {"ruff.toml", ".ruff.toml"}
_SQLFLUFF_KEYS = ("exclude_rules", "ignore", "warnings")


def tracked_files(root: Path) -> list[str]:
    """Tracked files that count as code: everything except docs/ and Markdown."""
    command = ["git", "-C", str(root), "ls-files", "-z"]
    output = subprocess.run(command, capture_output=True, check=True).stdout.decode()
    return [name for name in output.split("\0") if name and is_code(name)]


def is_code(name: str) -> bool:
    return not name.startswith("docs/") and not name.endswith(".md")


def _split_rules(rules: str | None) -> list[str]:
    return [rule for rule in re.split(r"[\s,]+", rules or "") if rule] or [ANY_RULE]


def _comment_hits(text: str) -> Iterator[Hit]:
    for kind, pattern in _COMMENT_PATTERNS.items():
        for match in pattern.finditer(text):
            yield from ((kind, rule) for rule in _split_rules(match[1]))


def _python_comments(text: str) -> Iterator[str]:
    """Comment tokens only, so that a string which mentions a marker does not count."""
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type == tokenize.COMMENT:
            yield token.string


def _dotted_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_dotted_name(node.value)}.{node.attr}"
    return ""


def _pytest_import_hits(node: ast.AST) -> Iterator[Hit]:
    """Imports that would let a skip hide from the attribute check."""
    if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "pytest":
        hidden = {"skip", "xfail", "importorskip", "mark"}
        yield from (("pytest-import", a.name) for a in node.names if a.name in hidden)
    if isinstance(node, ast.Import):
        yield from (
            ("pytest-import", "alias") for a in node.names if a.name == "pytest" and a.asname
        )


def _pytest_hits(text: str) -> Iterator[Hit]:
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.Attribute):
            match = _PYTEST_SKIPS.fullmatch(_dotted_name(node))
            if match:
                yield "pytest-skip", match[1]
        yield from _pytest_import_hits(node)


def _as_list(value: str | list[str]) -> list[str]:
    items = value.split(",") if isinstance(value, str) else value
    return [item.strip() for item in items if item.strip()]


def _ini_hits(text: str) -> Iterator[Hit]:
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(text)
    for section in parser.sections():
        if not section.startswith("sqlfluff"):
            continue
        for key in _SQLFLUFF_KEYS:
            for rule in _as_list(parser[section].get(key, "")):
                yield f"sqlfluff-{key.replace('_', '-')}", rule


def _ruff_hits(table: dict[str, Any]) -> Iterator[Hit]:
    for section in (table, table.get("lint", {})):
        for key in ("ignore", "extend-ignore"):
            yield from (("ruff-ignore", rule) for rule in section.get(key, []))
        for key in ("per-file-ignores", "extend-per-file-ignores"):
            for glob, rules in section.get(key, {}).items():
                yield from (("ruff-per-file-ignore", f"{glob}:{rule}") for rule in rules)


def _sqlfluff_toml_hits(table: dict[str, Any]) -> Iterator[Hit]:
    for section in (table, table.get("core", {})):
        for key in _SQLFLUFF_KEYS:
            for rule in _as_list(section.get(key, [])):
                yield f"sqlfluff-{key.replace('_', '-')}", rule


def _config_hits(name: str, text: str) -> Iterator[Hit]:
    if name in _INI_NAMES:
        yield from _ini_hits(text)
    if name in _RUFF_TOML_NAMES:
        yield from _ruff_hits(tomllib.loads(text))
    if name == "pyproject.toml":
        tool = tomllib.loads(text).get("tool", {})
        yield from _ruff_hits(tool.get("ruff", {}))
        yield from _sqlfluff_toml_hits(tool.get("sqlfluff", {}))


def scan_text(name: str, text: str) -> Counter[Key]:
    """Count the suppressions in one file; `name` is its repository path."""
    hits: list[Hit] = []
    if name.endswith((".py", ".pyi")):
        for comment in _python_comments(text):
            hits.extend(_comment_hits(comment))
        hits.extend(_pytest_hits(text))
    else:
        hits.extend(_comment_hits(text))
        hits.extend(("ignore-vuln", m[1] or ANY_RULE) for m in _IGNORE_VULN.finditer(text))
        hits.extend(_config_hits(Path(name).name, text))
    return Counter((name, kind, rule) for kind, rule in hits)


def find_suppressions(root: Path, names: list[str]) -> Counter[Key]:
    found: Counter[Key] = Counter()
    for name in names:
        path = root / name
        # A file deleted but not yet staged has nothing to suppress.
        if path.is_file():
            found.update(scan_text(name, path.read_text(encoding="utf-8", errors="replace")))
    return found


def load_entries(path: Path) -> list[dict[str, Any]]:
    return tomllib.loads(path.read_text(encoding="utf-8")).get("entry", [])


def _entry_key(entry: dict[str, Any]) -> Key:
    return (str(entry.get("file")), str(entry.get("kind")), str(entry.get("rule")))


def _field_problems(entry: dict[str, Any], today: date) -> Iterator[str]:
    for field in ("file", "kind", "rule", "reason", "owner"):
        value = entry.get(field)
        if not isinstance(value, str) or not value.strip():
            yield f"missing {field}"
    expires = entry.get("expires")
    # An exact type check: a TOML date-time or a quoted string is not accepted.
    if type(expires) is not date:
        yield "missing or invalid expires (use an unquoted YYYY-MM-DD)"
    elif expires < today:
        yield f"expired on {expires.isoformat()}"
    limits = [entry[field] for field in ("count", "max_count") if field in entry]
    if len(limits) != 1 or type(limits[0]) is not int or limits[0] < 1:
        yield "needs exactly one of count or max_count, as a positive integer"


def _count_problem(entry: dict[str, Any], found: int) -> str | None:
    """`count` must match exactly, so a removed suppression cannot leave room for a new one."""
    if type(entry.get("count")) is int and found != entry["count"]:
        return f"found {found}, register says count = {entry['count']}"
    if type(entry.get("max_count")) is int and found > entry["max_count"]:
        return f"found {found}, register says max_count = {entry['max_count']}"
    return None


def check_register(entries: list[dict[str, Any]], found: Counter[Key], today: date) -> list[str]:
    """Return one line per problem; an empty list means the register is in order."""
    problems: list[str] = []
    listed: set[Key] = set()
    for entry in entries:
        key = _entry_key(entry)
        label = " | ".join(key)
        if key in listed:
            problems.append(f"{label}: duplicate entry")
        listed.add(key)
        problems.extend(f"{label}: {problem}" for problem in _field_problems(entry, today))
        mismatch = _count_problem(entry, found[key])
        if mismatch:
            problems.append(f"{label}: {mismatch}")
    problems.extend(
        f"{' | '.join(key)}: {found[key]} not listed in the register"
        for key in sorted(set(found) - listed)
    )
    return problems
