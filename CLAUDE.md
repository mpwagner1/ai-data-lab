# CLAUDE.md

Placeholder. Alfred's full instructions will replace this.

- Python 3.12 via `uv` only (`uv add`, `uv run`). Never pip.
- Polars for dataframes, DuckDB for SQL on files.
- Lint/format with ruff; SQL with sqlfluff.
- Type hints everywhere; tests in `tests/` with pytest.
- Never log secrets or row-level data. Work on branches, open PRs.
