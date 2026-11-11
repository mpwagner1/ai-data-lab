# Code style

## Tooling

- Python 3.12, managed with uv: `uv add`, `uv run`. Never `pip` or global installs.
- Polars for dataframes, never pandas. DuckDB for SQL over files.
- `ruff` lints and formats (line length 100; rules E, F, I, UP, B, SIM).
  `sqlfluff` lints SQL (dialect duckdb, line length 100).
- Fix before committing: `uv run ruff check --fix . && uv run ruff format .`

## Python

- Type hints on every function, parameters and return. Use `X | None`, `list[str]`
  (no `typing.List`/`Optional`).
- Small, focused functions: one job, ideally under ~30 lines.
- Early returns and guard clauses. No more than two levels of nesting.
- Files stay under 500 lines. Split by responsibility when they grow.
- Prefer Polars lazy frames (`scan_*`, `.lazy()`) and expressions over row loops.
  No `.apply`/`map_elements` unless there is no expression equivalent.
- Pure functions for transforms; keep I/O (reading, writing, network) at the edges.
- No mutable default args, no wildcard imports, no `print` in library code.
- Comments explain why, not what. Docstrings on public functions: one line, plus
  detail only when the behavior is non-obvious.

## Naming

| Thing | Style | Example |
| --- | --- | --- |
| Modules, functions, variables | `snake_case` | `add_bronze_metadata` |
| Classes | `PascalCase` | `FileManifest` |
| Constants | `UPPER_SNAKE` | `BRONZE_META_COLUMNS` |
| Private | leading underscore | `_hash_file` |
| Tests | `test_<behavior>` | `test_rejects_empty_file` |

- Names say what a thing is: `rows_ingested`, not `n` or `data`.
- Boolean names read as questions: `is_valid`, `has_header`.

## SQL (sqlfluff)

- Lowercase keywords and identifiers, `snake_case` names, trailing commas off.
- One CTE per logical step, named for what it holds (`deduped_orders`).
- No `select *` outside a CTE that immediately narrows it.
- Explicit joins with `on`; qualify columns when more than one table is in scope.
- Format: `uv run sqlfluff fix <path>`, check: `uv run sqlfluff lint <path>`.
