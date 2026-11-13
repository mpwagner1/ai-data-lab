# Plan: Bronze ingestion for any file a customer sends

Date: 2026-11-12
Status: Done 2026-11-12 (all seven tasks merged). Delivered by PRs #36 (task 1), #42 (2), #49 (3),
#55 (4), #58 (5), #64 (6) and #74 (7), with follow-ups #40, #46, #53, #67, #70 and #77. Reviews changed
the design along the way; ADR 0002 describes the behavior as built, and `docs/BRONZE.md` is the guide.
Left over, by choice: a one-test follow-up for the structural-vote tie in the UTF-16 byte-order check
(PR #70 review), and an optional `landing.py` split of `bronze.py` (430 lines, limit 450).

## Goal

Never turn a customer away over a file type. Every file is accepted and kept. If it cannot be parsed
yet, it is held and reported. Adding a format means one reader function plus a fixture.

## Decisions already made

- Delimiters: extension defaults plus a per-source YAML override. No sniffing.
- Excel: load every sheet and add a `_sheet_name` metadata column.
- Scope: full foundation (raw zone, registry, detection, config, text, Excel, JSON/NDJSON, Parquet,
  gz/zip). Fixed-width is a follow-up.

## Assumptions

- The bronze rules stay: append-only, all source columns strings, no casting or cleaning, never log
  row data.
- `ingest_csv` stays as a thin wrapper over the new `ingest_file`, so the existing tests run unchanged.
- Out of scope: silver, gold, Dagster wiring, cloud storage, compaction, fixed-width, deleting or moving
  landing files.

## Design summary

Full rationale is in `docs/decisions/0002-bronze-multi-format.md`.

- **Raw zone:** `lake/raw/<source>/receive_date=YYYY-MM-DD/<sha256>.<orig-ext>` holds the exact bytes.
- **Pending is derived:** a raw file is pending when no bronze Parquet starts with its hash. Every run
  parses everything pending, so replay is automatic.
- **Detection:** `format:` config, then file signature, then text heuristics. Delimited files get their
  delimiter from the flag (no-default extensions only), then config, then extension. An unknown `.txt` or `.dat` is held, not guessed.
- **Readers:** `READERS: dict[FileFormat, Reader]`, each returning `list[ParsedPart]` with all-string
  frames. Shared metadata, hashing, re-hash check, atomic write and logging stay as they are.
- **Workbooks:** one Parquet file per workbook, all sheets combined, with `_sheet_name` on every row.
- **`--delimiter` flag:** supplies a single-character delimiter for files with no extension default
  (`.txt`, `.dat`). It wins over config `delimiter:` for those files and never touches `.csv`, `.tsv`
  or `.psv`. With neither flag nor config, the file is held (`needs_config`) and the run exits 3.
- **Config:** `config/sources/<source>.yaml` with `delimiter`, `encoding`, `format`, `sheets`. Unknown
  keys raise.
- **Exit codes:** 0 ok, 1 a file failed, 2 bad source or folder, 3 nothing failed but files are held.

## Tasks

| # | Task | Agent | Model | Acceptance criteria | Depends on |
|---|------|-------|-------|---------------------|------------|
| 0 | This plan and ADR 0002. | Alfred | Opus | Michael approves both. | - |
| 1 | Reader registry, `SourceConfig`, delimited reader (comma, tab, pipe, config delimiter, `--delimiter` flag, encoding), `ingest_file`, `ingest_csv` wrapper. | builder | Sonnet | All existing tests pass unchanged. New tests: `.psv`, `.tsv` and `.txt` with a config delimiter or `--delimiter '|'` load as strings; a multi-character `--delimiter` exits 2; the flag beats config for a `.txt`; the flag does not change a `.csv`; a cp1252 file with config keeps `é`; an unknown config key raises. Ruff clean. | 0 |
| 2 | Raw zone, signature detection, derived pending, `bronze_unparsed`, exit code 3, `--pending`. | builder | Sonnet | Raw bytes hash-equal the landing bytes. A `.txt` with no config and an unknown `.bin` stay pending (exit 3). Adding the config and rerunning parses the `.txt` (exit 0). An xlsx saved as `.csv` is detected as xlsx. Logs hold no cell values. | 1 |
| 3 | Excel reader (`uv add fastexcel`), `_sheet_name` metadata and the `header_row` config key. | builder | Sonnet | A 2-sheet xlsx gives one Parquet file with both sheet names and `_row_number` equal to the sheet row (first data row 2 with `header_row: 1`). A sheet with 5 blank rows then headers on row 6 loads with `header_row: 6` (first data row has `_row_number` 7), logs the skipped-row count and never their content; without it the headers come out as `unnamed_<n>`. A `header_row` past the end of a sheet fails that file with a clear error. `header_row` must be a positive integer. Empty rows inside data are kept. A blank header becomes `unnamed_<n>`. Older bronze files read back with null `_sheet_name`. The `sheets:` filter works. | 2 |
| 4 | JSON, NDJSON and Parquet readers. | builder | Sonnet | Nested values arrive as JSON strings. Every source column is `pl.String`. An empty array raises like an empty file. | 2 |
| 5 | Archives (gz, zip). | builder | Sonnet | A zip with a csv and an xlsx gives two units with member hashes and `!` names. Size and member caps raise. A member named `../x` cannot escape the lake. A rerun skips both. | 3, 4 |
| 6 | QA pass on edge cases: merged cells, Excel number and date cells, UTF-16, gzipped csv, corrupt xlsx. | qa | Sonnet | `tests/` only. A corrupt xlsx fails, keeps its raw copy, and other files continue. Bugs are reported, not fixed. | 5 |
| 7 | Update `docs/BRONZE.md`. | docs-writer | Haiku | Flow, formats, config keys, the `--delimiter` flag, exit codes, events, the `_sheet_name` column and limits match the code. Includes a "how to add a reader" section. | 6 |

All documentation tasks go to the `docs-writer` agent (it edits only `README.md` and `docs/`). Task 0
was written by Alfred before this routing was set. The `_sheet_name` contract change
(`contracts/bronze/_metadata.yaml`) is code-side, so it stays in builder task 3.

Each task gets its own issue and branch `agent/<issue>-<slug>`, one PR per task, dispatched one at a
time, with a code-reviewer pass before the next task starts.

## Risks

- Excel stores `001` as a number, so leading zeros cannot be recovered from the file. The raw zone keeps
  the original bytes.
- Calamine returns numbers in its own string form (for example `1.0`). Documented, not cleaned.
- Zip bombs and path traversal in archives. Caps on size and member count; member names are never used
  as paths; nested archives are not expanded.
- Excel and JSON are read into memory. Fine at this scale; revisit for large files.
- The raw zone roughly doubles storage. Accepted for recoverability.
- Fixed-width, EBCDIC, PDF and proprietary binaries need a spec from the customer. They stay pending
  until a reader exists.
- Single writer per source, unchanged from ADR 0001.

## Verification

1. `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest`.
2. End to end with a demo source holding csv, psv, tsv, a pipe `.txt`, a 2-sheet xlsx, json, ndjson,
   parquet, a zip, and a `.bin`: the first run exits 3 with the `.txt` and `.bin` pending; adding
   `config/sources/demo.yaml` with `delimiter: "|"` leaves only the `.bin`; `read_bronze` returns every
   unit with string columns; a third run skips everything.
3. Grep the logs for fixture cell values; there must be none.
