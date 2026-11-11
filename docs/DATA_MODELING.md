# Data modeling

Medallion layers: bronze (raw) → silver (typed, contracted) → gold (modeled for use).

## Bronze

- Append-only. Never update or delete rows; reruns append new rows.
- Every row carries these metadata columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `_file_name` | string | Source file name (not full path) |
| `_ingested_at` | timestamp (UTC) | When this load wrote the row |
| `_file_hash` | string | SHA-256 of the source file bytes |
| `_row_number` | int | 1-based row position within the source file |

- No type casting in bronze. Source columns stay as strings exactly as read.
  Read CSVs with Polars `infer_schema=False` (or DuckDB `all_varchar=true`).
- No cleaning, renaming of values, or filtering. Fix problems in silver.
- Idempotency: `_file_hash` lets silver detect a file loaded more than once.

## Silver

- Typed, deduplicated and cleaned. Cast here, not in bronze.
- Every silver table has a contract in `contracts/silver/<table>.yaml` and must match it:
  column names, types, nullability, keys.
- Contracts mark PII columns (see [SECURITY](SECURITY.md)).
- Rows that fail casting or validation go to a quarantine table with the reason, not
  silently dropped. Log counts only, never row contents.
- Keep lineage: carry `_file_name` and `_file_hash` through, so any row traces to its file.
- Change a contract first, then the code, in the same PR.

## Gold

- Gold is dbt models (dbt-duckdb) built from silver, never from bronze directly.
- Naming: `dim_<entity>`, `fact_<event>`, `agg_<subject>_<grain>`.
- Each model has a description and tests (`unique`, `not_null`, `relationships`)
  in its schema YAML.
- Use `ref()` and `source()`, never hardcoded table names.

## Naming (all layers)

- Tables and columns are `snake_case`, lowercase, no spaces or reserved words.
- Singular entity names (`customer`, not `customers`); timestamps end `_at`,
  dates end `_date`, booleans start `is_`/`has_`, surrogate keys end `_key`.
- Only bronze metadata columns start with `_`.
