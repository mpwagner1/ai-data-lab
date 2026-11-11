# 0001: Bronze storage as partitioned Parquet, queried with DuckDB

Date: 2026-11-11
Status: Proposed (revised 2026-11-11: unique file names, atomic writes, batch behavior)

## Context

Bronze must be append-only, keep every source value as a string, and carry the four
metadata columns (`_file_name`, `_ingested_at`, `_file_hash`, `_row_number`). We need a
storage layout that makes appends safe, makes "was this file already loaded?" cheap, and
moves to cloud storage later without a rewrite.

## Decision

- Write one Parquet file per load of a source file:
  `lake/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<rand>.parquet`.
- `<source>` is passed in by the caller. Source CSVs are read from `data/landing/<source>/`.
- Query bronze through DuckDB (`read_parquet('lake/bronze/<source>/**/*.parquet',
  hive_partitioning=true, union_by_name=true)`).
- `lake/` and `data/` are git-ignored.
- All source columns are stored as strings. Column changes between files are not an error in
  bronze: `union_by_name` fills missing columns with null. Silver contracts decide what is valid.
- `<file_hash>` is the 64-character SHA-256 hex of the source file bytes. `<run_ts>` is the UTC
  start time of the run, formatted `YYYYMMDDTHHMMSSZ` (for example `20261111T163300Z`).
  `<rand>` is 8 random hex characters. The hash and timestamp are concatenated with no
  separator, and an underscore precedes `<rand>`, so the full name is
  `<64 hex chars>20261111T163300Z_1a2b3c4d.parquet`. The fixed hash length keeps the name
  unambiguous. The random suffix makes every name unique, so two loads in the same second
  (for example `--force` over identical files) never collide.
- Skip a file when any existing `*.parquet` file under the source (any `ingest_date`) has a name
  starting with its `<file_hash>`. `--force` skips that check and writes a new file.
  Temp files are ignored by the skip check and by readers.
- Writes are atomic and never replace: the frame is written to `<name>.parquet.tmp` in the target
  folder, then published to the final name with a no-replace operation (`os.link`), and the temp
  file is removed whether or not that succeeds. A crash cannot leave a truncated file at a final
  name. If the final name somehow exists, the write fails.
- Batch behavior (CLI over `data/landing/<source>/*.csv`): a failure on one file is logged
  (error type and file name only, never row data) and the run continues with the remaining files.
  The run exits 1 if any file failed, so one bad file never hides the others and never passes
  silently. A missing landing folder exits 2 with an error. A folder with no CSVs logs a warning
  and exits 0.
- Log values are JSON-quoted strings, so file names with spaces or newlines cannot break the
  `key=value` format.

## Consequences

- Append-only is natural: a load only ever adds a new file and never edits an existing one.
- Idempotency is a file-existence check on the hash, with no table scan.
- Parquet and the layout move to Azure storage as-is.
- Schema drift is tolerated at read time rather than blocked at write time. Silver must
  validate columns and quarantine, as `docs/DATA_MODELING.md` already requires.
- Every load writes a new, uniquely named file, so a forced rerun keeps the earlier copy and
  strict append-only holds, including on the same day and within the same second.
- A forced rerun duplicates rows for that file by design. Silver dedupes on `_file_hash`.
- Many small files for many small CSVs. Acceptable for a learning project; compaction would be
  a later, separate job that must not alter bronze history.

## Alternatives considered

- **DuckDB table per source:** simpler to append to, but couples storage to one engine and
  one file, and it makes schema drift a write-time problem.
- **Single Parquet file per source, rewritten on append:** violates append-only.
- **`<file_hash>.parquet` only:** a forced same-day rerun would overwrite the file.
- **`<file_hash><run_ts>.parquet` (no random suffix):** two forced loads of identical files in the
  same second collide, and one collision aborted the rest of a batch.
- **Header-only files:** they write nothing and are re-read on every run. Recording them is left
  as a later decision.
