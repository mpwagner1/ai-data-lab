"""Per-source ingestion settings, loaded from `config/sources/<source>.yaml`.

Every key is optional. Unknown keys raise so a typo cannot be silently ignored.
"""

import codecs
from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_ROOT = Path("config/sources")
_KNOWN_KEYS = frozenset({"delimiter", "encoding", "format", "sheets"})


@dataclass(frozen=True)
class SourceConfig:
    """Settings for one source. `format` and `sheets` are stored now and used by later readers."""

    delimiter: str | None = None
    encoding: str = "utf-8"
    format: str | None = None
    sheets: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.delimiter is not None and len(self.delimiter) != 1:
            raise ValueError("delimiter must be a single character")
        try:
            codecs.lookup(self.encoding)
        except LookupError as error:
            raise ValueError(f"unknown encoding: {self.encoding!r}") from error


def parse_source_config(raw: object) -> SourceConfig:
    """Build a SourceConfig from parsed YAML. Raises ValueError on bad shape or unknown keys."""
    if raw is None:
        return SourceConfig()
    if not isinstance(raw, dict):
        raise ValueError("source config must be a mapping")
    unknown = sorted(set(raw) - _KNOWN_KEYS)
    if unknown:
        raise ValueError(f"unknown source config keys: {unknown}")
    sheets = raw.get("sheets")
    if sheets is not None:
        sheets = tuple(str(name) for name in sheets)
    values = {key: value for key, value in raw.items() if key != "sheets" and value is not None}
    return SourceConfig(sheets=sheets, **values)


def load_source_config(source: str, config_root: Path = CONFIG_ROOT) -> SourceConfig:
    """Load `<config_root>/<source>.yaml`; a missing file means all defaults."""
    path = config_root / f"{source}.yaml"
    if not path.is_file():
        return SourceConfig()
    return parse_source_config(yaml.safe_load(path.read_text(encoding="utf-8")))
