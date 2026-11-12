"""Per-source ingestion settings, loaded from `config/sources/<source>.yaml`.

Every key is optional. Unknown keys raise so a typo cannot be silently ignored.
"""

from dataclasses import dataclass
from pathlib import Path

import yaml

from ingestion.csv_reader import DEFAULT_ENCODING

CONFIG_ROOT = Path("config/sources")
_FORBIDDEN_DELIMITERS = frozenset({'"', "\n", "\r"})
_KNOWN_KEYS = frozenset({"delimiter", "encoding", "format", "sheets"})


@dataclass(frozen=True)
class SourceConfig:
    """Settings for one source. `format` and `sheets` are stored now and used by later readers."""

    delimiter: str | None = None
    encoding: str = DEFAULT_ENCODING
    format: str | None = None
    sheets: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.delimiter is not None:
            validate_delimiter(self.delimiter)
        validate_encoding(self.encoding)


def validate_delimiter(delimiter: object) -> None:
    """Raise ValueError unless `delimiter` is one byte in UTF-8 and not a quote or newline.

    Polars needs a single-byte separator. Messages never echo the value.
    """
    if not isinstance(delimiter, str):
        raise ValueError("delimiter must be a string")
    if delimiter in _FORBIDDEN_DELIMITERS:
        raise ValueError("delimiter must not be a quote or a newline")
    if len(delimiter.encode("utf-8")) != 1:
        raise ValueError("delimiter must be exactly one byte in UTF-8")


def validate_encoding(encoding: object) -> None:
    """Raise ValueError unless `encoding` is a text codec (so not `hex`, `base64`, `undefined`...).

    Trial round trip of a short ASCII string. Fails closed: anything that does not give the
    same string back is rejected. Messages never echo the value.
    """
    if not isinstance(encoding, str):
        raise ValueError("encoding must be a string")
    sample = "abc"
    try:
        ok = sample.encode(encoding).decode(encoding) == sample
    except (LookupError, UnicodeError, TypeError, ValueError, AttributeError):
        raise ValueError("encoding is not a usable text encoding") from None
    if not ok:
        raise ValueError("encoding is not a usable text encoding")


def _parse_sheets(sheets: object) -> tuple[str, ...] | None:
    if sheets is None:
        return None
    if not isinstance(sheets, list) or not all(isinstance(name, str) for name in sheets):
        raise ValueError("sheets must be a list of strings")
    return tuple(sheets)


def parse_source_config(raw: object) -> SourceConfig:
    """Build a SourceConfig from parsed YAML. Raises ValueError on bad shape or unknown keys."""
    if raw is None:
        return SourceConfig()
    if not isinstance(raw, dict):
        raise ValueError("source config must be a mapping")
    if set(raw) - _KNOWN_KEYS:
        # Count only: config text (even key names) never goes into an error message.
        raise ValueError("unknown source config keys")
    sheets = _parse_sheets(raw.get("sheets"))
    values = {key: value for key, value in raw.items() if key != "sheets" and value is not None}
    return SourceConfig(sheets=sheets, **values)


def load_source_config(source: str, config_root: Path = CONFIG_ROOT) -> SourceConfig:
    """Load `<config_root>/<source>.yaml`; a missing file means all defaults.

    Raises ValueError for unreadable or invalid config. The YAML parser's message is dropped
    because it can quote config text.
    """
    path = config_root / f"{source}.yaml"
    if not path.is_file():
        return SourceConfig()
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (yaml.YAMLError, UnicodeDecodeError) as error:
        raise ValueError(f"invalid config file {path.name}: {type(error).__name__}") from None
    return parse_source_config(raw)
