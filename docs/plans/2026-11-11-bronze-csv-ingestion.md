# Plan: Bronze ingestion for CSV files

Date: 2026-11-11
Status: Done (PRs #11, #14, #17 and #20; ADR in #15)

## Goal

Load CSVs into append-only bronze Parquet files, with every column kept as a string and the four
metadata columns (`_file_name`, `_ingested_at`, `_file_hash`, `_row_number`) added to every row.

## Assumptions

- Agents come from the design: `builder`, `code-reviewer`, `qa` (all Sonnet). They are drafted in
  PR #8 and no task runs until it is merged.
- Storage: Parquet at `lake/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<rand>.parquet`
  (`run_ts` is a UTC timestamp like `20261111T163300Z`, `rand` is 8 random hex characters), queried
  through DuckDB. `lake/` and `data/` are git-ignored. See `docs/decisions/0001-bronze-parquet-storage.md`.
- Source CSVs come from `data/landing/<source>/`. Sample files go in `tests/fixtures/`
  (synthetic only).
- Reads use Polars `scan_csv(..., infer_schema=False)`; values are never trimmed, renamed or cast.
- Reruns skip a file when any existing file for the source starts with its `<file_hash>`.
  `--force` writes a new file and never overwrites. The hash and timestamp are concatenated with no
  separator (the hash is always 64 hex characters), followed by `_<rand>`. Writes go to a temp file
  and are published without replacing. In a batch, one failing file does not stop the rest; the CLI
  exits 1 at the end if any failed.
- Column changes between files do not fail bronze (`union_by_name` at read time). Silver contracts
  handle them. A source column starting with `_` would collide with metadata, so it is rejected.
- An empty file (no header) raises. A header-only file is written as a zero-row Parquet file (so it is skipped on later runs) and logs `rows_ingested=0`.
- Out of scope: silver, gold, Dagster wiring, other file formats, cloud storage, compaction.
- No existing helpers: `src/ingestion/` is an empty package.

## Tasks

| # | Task | Agent | Model | Acceptance criteria | Depends on |
|---|------|-------|-------|---------------------|------------|
| 1 | Builder PR 1, core library. Add `.gitignore` entries for `lake/` and `data/`; `contracts/bronze/_metadata.yaml` (four columns and types); `src/ingestion/metadata.py` (`hash_file` streaming SHA-256, `add_bronze_metadata`, `BRONZE_META_COLUMNS`); `src/ingestion/csv_reader.py` (`read_csv_as_strings`: all columns `pl.String`, rejects empty file and `_`-prefixed columns). Tests in `tests/ingestion/`. | builder | Sonnet | `uv run pytest tests/ingestion` passes and covers: hash matches `hashlib`; `_row_number` is 1-based and contiguous; `_ingested_at` is UTC and identical within one load; leading zeros and `"1e3"` survive; empty file raises; header-only gives 0 rows; `_` column rejected. `git check-ignore lake data` lists both. `uv run ruff check .` and `uv run ruff format --check .` are clean. | ADR + agents PR merged |
| 2 | Builder PR 2, write path and CLI. `src/ingestion/bronze.py`: `ingest_csv(path, source, lake_root, force=False) -> IngestResult` (frozen dataclass: file name, hash, rows, skipped). It writes the partitioned Parquet path, skips when a file starting with the hash exists unless forced, and never overwrites an existing file. Logs are key=value: counts, file name, hash, duration, never row data. CLI: `uv run python -m ingestion.bronze <source> [--force]` over `data/landing/<source>/*.csv`. Plus a DuckDB read helper and a short README usage section. | builder | Sonnet | `tests/ingestion/test_bronze.py` passes and covers: the four metadata columns exist; values are never cast; the path matches the layout, including the `<file_hash><run_ts>_<rand>.parquet` name; a rerun skips (file count and rows unchanged); `--force` adds a new file with a different `run_ts` and keeps the old one; a name collision fails instead of overwriting; files with different columns both load and read back via DuckDB; empty file raises; captured logs contain none of the fixture's cell values. CLI run on a `tmp_path` landing folder creates the Parquet file. `uv run pytest` passes. | 1 |
| 3 | QA pass on the bronze rules in `docs/TESTING.md`: gaps in edge cases (BOM, quoted fields with embedded newlines, duplicate rows, nulls, two `--force` runs on the same day). Edits `tests/` only. | qa | Sonnet | New tests are added under `tests/` only (`git diff --stat` shows no other paths); `uv run pytest` passes; any code bug found is reported to Alfred and is not fixed by QA. | 2 |
| 4 | Code review of PR 1 and PR 2 against `AGENTS.md`, `DATA_MODELING`, `SECURITY` and `TESTING`. Comments only. (Reviews run right after each builder PR; the QA PR gets one too.) | code-reviewer | Sonnet | PR comments exist, each finding has a file and line; the report lists Passed, Failed and Not run for ruff check, ruff format and pytest (sqlfluff not applicable: no SQL). No bronze-rule violations remain open. | 1, 2, 3 |

Each task gets its own issue and a branch `agent/<issue>-<slug>`, one PR per task, dispatched one at a
time. Review (task 4) runs after each PR, before the next one starts.

Removed from the earlier draft: the separate metadata, reader, and CLI tasks (merged into tasks 1
and 2), the summary doc task, and the docs-writer agent. The ADR is written by Alfred and is not a
task.

## Risks

- Two loads in the same second — the random suffix makes names unique, and the write still fails
  rather than overwrite; tested in task 2.
- A crash mid-write — atomic temp-file write and no-replace publish; tested in task 2.
- Header-only files would be re-read on every run — they are written as zero-row Parquet files so the skip check finds them (#19).
- Schema drift between files produces nulls at read time — accepted by design; silver contracts
  must validate and quarantine.
- Polars `scan_csv` quirks (quoting, BOM, encoding) altering raw values — tests in tasks 1 and 3;
  non-UTF-8 files are rejected rather than altered.
- Hash-skip uses a file-name prefix match, so a deleted or moved Parquet file would be re-ingested — acceptable
  and consistent with `lake/` being rebuildable from `data/landing/`.
- Row-level data leaking into logs or errors — log-capture test in task 2; error messages carry
  column names, never values.
- The `qa` tests-only and `code-reviewer` read-only limits rely on instructions plus tool lists; they
  are not hard-enforced. Task 3 checks `git diff --stat`.

## Open questions

- None outstanding.
