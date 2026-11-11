# Bronze ingestion

How CSV files get from a landing folder into the bronze layer, how the code is built, and how
the work was planned and reviewed. Decisions live in
[`decisions/0001-bronze-parquet-storage.md`](decisions/0001-bronze-parquet-storage.md); the plan
and its dispatch log are in [`plans/`](plans/).

## What bronze is

Bronze is the raw layer of the medallion platform (bronze, silver, gold). Its job is to keep
exactly what the source sent, plus enough metadata to trace every row back to its file.

- **Append-only.** Nothing is updated, overwritten or deleted. A reload adds a new file.
- **No casting, cleaning or filtering.** Every source column is a string, as read. Typing and
  validation happen in silver, where a contract defines what is valid.
- **Four metadata columns on every row** (defined in `contracts/bronze/_metadata.yaml`):

| Column | Type | Meaning |
| --- | --- | --- |
| `_file_name` | string | Source file name, not the full path |
| `_ingested_at` | timestamp (UTC) | When the load ran; the same for every row in one load |
| `_file_hash` | string | SHA-256 of the source file's bytes |
| `_row_number` | int | 1-based row position within the source file |

Why these four: `_file_hash` lets silver spot a file that was loaded twice, and
`_file_name` plus `_row_number` point back to the exact row in the source.

## How to run it

```bash
mkdir -p data/landing/<source>          # put the CSV files here (git-ignored)
uv run python -m ingestion.bronze <source>            # load new files, skip known ones
uv run python -m ingestion.bronze <source> --force    # load again; keeps the earlier copy
```

`<source>` is a name you choose, for example `orders`. It must start with a letter or digit and
use only letters, digits, `_` and `-`, because it becomes a folder name and must not escape the
lake.

Read the data back with DuckDB:

```python
from pathlib import Path
from ingestion.bronze import read_bronze

read_bronze("<source>", Path("lake")).show()
```

Files with different columns read together; a column missing from a file comes back as null.

## Where files go

```
data/landing/<source>/*.csv                                  input (git-ignored)
lake/bronze/<source>/ingest_date=YYYY-MM-DD/
    <file_hash><run_ts>_<8 hex>.parquet                      output (git-ignored)
```

- `<file_hash>` is 64 hex characters, so the name is unambiguous even though the hash and
  `<run_ts>` (UTC, like `20261111T163300Z`) are joined with no separator.
- The random 8-hex suffix makes every file name unique, so two loads in the same second never
  collide.
- One Parquet file is written per load of a source file.

## What happens to one file

`ingest_csv` in `src/ingestion/bronze.py`:

1. **Validate the source name.** Anything unsafe raises `ValueError`.
2. **Hash the file** (SHA-256, streamed in chunks by `hash_file`).
3. **Skip check.** If any existing `*.parquet` for the source starts with this hash, the file was
   already loaded and is skipped. `--force` bypasses the check.
4. **Read it as strings** (`read_csv_as_strings`: Polars `scan_csv` with `infer_schema=False`).
   A zero-byte or whitespace-only file raises `ValueError`, as does a source column starting
   with `_`, which would collide with the metadata columns.
5. **Add the metadata columns** (`add_bronze_metadata`) and collect the frame.
6. **Re-hash the file.** If the hash changed, the file was edited mid-read, so the run fails
   instead of writing rows that do not match `_file_hash`.
7. **Write atomically and never replace.** The frame goes to `<name>.parquet.tmp`, is published
   to its final name with `os.link` (which fails if the name exists), and the temp file is
   removed either way. A crash can leave a `.tmp` file but never a half-written Parquet file.

A header-only file (header, no rows) is written as a zero-row Parquet file. That records the
load, so the skip check skips it next time and it is not read again.

## Running over a folder

`python -m ingestion.bronze <source>` loads every `*.csv` in `data/landing/<source>/`, in name
order. One bad file does not stop the rest: its failure is logged and the run continues.

| Exit code | Meaning |
| --- | --- |
| 0 | Every file was loaded or skipped (also when the folder has no CSVs, with a warning) |
| 1 | At least one file failed |
| 2 | Invalid source name, or the landing folder does not exist |

Stale `.parquet.tmp` files from a killed run are reported with a warning. They are never
deleted automatically.

## Logs

Logs are `key=value` lines. String values are JSON-quoted, so a file name with a space or
newline cannot break the format. They carry counts, file names, hashes, durations and error
types, never row contents or error messages (messages can include values).

| Event | When |
| --- | --- |
| `bronze_write` | A file was written (`rows_ingested`, `duration_s`) |
| `bronze_skip` | A file was already loaded |
| `bronze_error` | A file failed, or the source name or folder was invalid |
| `bronze_run` | Summary of a folder run: files, written, skipped, failed |
| `bronze_no_files` | The landing folder had no CSVs |
| `bronze_stale_temp` | Leftover `.parquet.tmp` files were found |

## Assumptions and limits

- **One writer per source at a time.** Two concurrent runs can both pass the skip check and write
  the same file, so its rows would appear twice. Nothing is overwritten, and silver dedupes on
  `_file_hash`, but bronze does not prevent it.
- **Local filesystem with hard links.** `os.link` is how a write avoids replacing a file. Azure
  Blob Storage has no equivalent, so a move there needs a conditional create-if-not-exists
  write.
- **A forced reload duplicates that file's rows by design.** Silver dedupes on `_file_hash`.
- **Many small files.** Fine for this project; compaction would be a separate job that must not
  alter bronze history.

## Code map

| Path | Role |
| --- | --- |
| `src/ingestion/metadata.py` | `hash_file`, `add_bronze_metadata`, `BRONZE_META_COLUMNS` |
| `src/ingestion/csv_reader.py` | `read_csv_as_strings` |
| `src/ingestion/bronze.py` | `ingest_csv`, `ingest_landing`, `read_bronze`, the CLI |
| `contracts/bronze/_metadata.yaml` | The four metadata columns |
| `tests/ingestion/` | pytest tests, with synthetic fixtures in `tests/fixtures/` |

Check everything with `uv run ruff check .`, `uv run ruff format --check .` and
`uv run pytest`.

## How it was built

The work followed the project's agent workflow, one task at a time:

1. **Plan.** Alfred (the head agent) wrote
   [`plans/2026-11-11-bronze-csv-ingestion.md`](plans/2026-11-11-bronze-csv-ingestion.md) and
   the decision record, and Michael approved them.
2. **Issue.** Each task got a GitHub issue with acceptance criteria.
3. **Build.** The `builder` agent implemented one issue on an `agent/<issue>-<slug>` branch and
   opened a PR.
4. **Review.** The `code-reviewer` agent commented on the PR against `AGENTS.md` and `docs/`.
   It never edits code. The builder then fixed the findings on the same PR.
5. **Test.** The `qa` agent added tests under `tests/` only.
6. **Merge.** Michael merged every PR. No agent pushes to `main` or merges.
7. **Log.** Every dispatch and result is in [`plans/log.md`](plans/log.md).

Review changed the design along the way: the first write was not atomic and two loads in the
same second could collide, so the file names got a random suffix and writes became
temp-file-then-link. Header-only files were first skipped without a record, so they were re-read
every run, which led to writing them as zero-row Parquet files.

## What comes next

Silver is not built yet. It reads bronze, casts to the types in `contracts/silver/<table>.yaml`,
quarantines rows that fail, dedupes on `_file_hash`, and carries `_file_name` and `_file_hash`
through for lineage.
