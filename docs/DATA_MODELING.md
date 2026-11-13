# Data modeling

Medallion layers: bronze (raw) → silver (typed, contracted) → gold (modeled for use).

## Bronze

- Append-only. Never update or delete rows; reruns append new rows.
- Every row carries these five metadata columns (defined in
  [`contracts/bronze/_metadata.yaml`](../contracts/bronze/_metadata.yaml)):

| Column | Type | Meaning |
| --- | --- | --- |
| `_file_name` | string | Landing file name (not full path). An archive member is `<archive>!<member>`, for example `orders.zip!orders_2026.csv` |
| `_ingested_at` | timestamp (UTC) | When the run started; the same for every file in one run |
| `_file_hash` | string | SHA-256 of the file bytes (of the member's bytes for an archive member) |
| `_row_number` | int | Row position; its meaning depends on the format (below) |
| `_sheet_name` | string | The workbook sheet a row came from; null for files that are not workbooks |

- `_row_number` by format:
  - Delimited text, JSON, NDJSON and Parquet: 1-based position among data rows. The header is not
    counted.
  - Workbooks: the row number as Excel shows it, counted per sheet. With `header_row: 1` the first
    data row is 2. Empty rows inside the data keep their numbers.
  - Archive members: each member is its own file, so the numbering starts again (at 1, or at the
    sheet row for a workbook member).
- Source columns are all strings and keep the source text. Bronze does not cast, trim, round,
  rename values or filter rows; typing happens in silver. Parquet is the one format with its own
  types, so its values are written as text in fixed forms (see [BRONZE](BRONZE.md), section 4). A
  source column whose name starts with `_` fails the file, so it cannot clash with a metadata column.
- The raw zone keeps every received file unchanged, so bronze can be rebuilt from it.
- A file that cannot be read yet is held, not dropped: nothing is written for it, and it is read
  again on every run, so a new reader or config replays it. A corrupt file fails the run instead.
- Idempotency: a file whose `_file_hash` is already in bronze is skipped. A forced reload writes its
  rows again, so `_file_hash` lets silver detect and dedupe a file loaded more than once.
- Details (formats, config, exit codes, log events): [BRONZE](BRONZE.md). Why: ADR
  [0002](decisions/0002-bronze-multi-format.md).

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
