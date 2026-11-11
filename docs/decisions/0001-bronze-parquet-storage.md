# 0001: Bronze storage as partitioned Parquet, queried with DuckDB

Date: 2026-11-11
Status: Proposed (revised 2026-11-11: timestamped file names)

## Context

Bronze must be append-only, keep every source value as a string, and carry the four
metadata columns (`_file_name`, `_ingested_at`, `_file_hash`, `_row_number`). We need a
storage layout that makes appends safe, makes "was this file already loaded?" cheap, and
moves to cloud storage later without a rewrite.

## Decision

- Write one Parquet file per load of a source file:
  `lake/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>.parquet`.
- `<source>` is passed in by the caller. Source CSVs are read from `data/landing/<source>/`.
- Query bronze through DuckDB (`read_parquet('lake/bronze/<source>/**/*.parquet',
  hive_partitioning=true, union_by_name=true)`).
- `lake/` and `data/` are git-ignored.
- All source columns are stored as strings. Column changes between files are not an error in
  bronze: `union_by_name` fills missing columns with null. Silver contracts decide what is valid.
- `<file_hash>` is the 64-character SHA-256 hex of the source file bytes. `<run_ts>` is the UTC
  start time of the run, formatted `YYYYMMDDTHHMMSSZ` (for example `20261111T163300Z`). The two are
  concatenated with no separator, so the full name is
  `<64 hex chars>20261111T163300Z.parquet`. The fixed hash length keeps the name unambiguous.
- Skip a file when any existing file under the source (any `ingest_date`) has a name starting with
  its `<file_hash>`. `--force` skips that check and writes a new file with a new `run_ts`.
  Existing files are never overwritten: if the target name already exists (two forced runs in the
  same second), the run fails instead of replacing it.

## Consequences

- Append-only is natural: a load only ever adds a new file and never edits an existing one.
- Idempotency is a file-existence check on the hash, with no table scan.
- Parquet and the layout move to Azure storage as-is.
- Schema drift is tolerated at read time rather than blocked at write time. Silver must
  validate columns and quarantine, as `docs/DATA_MODELING.md` already requires.
- Every load writes a new, uniquely named file, so a forced rerun keeps the earlier copy and
  strict append-only holds, including on the same day.
- A forced rerun duplicates rows for that file by design. Silver dedupes on `_file_hash`.
- Many small files for many small CSVs. Acceptable for a learning project; compaction would be
  a later, separate job that must not alter bronze history.

## Alternatives considered

- **DuckDB table per source:** simpler to append to, but couples storage to one engine and
  one file, and it makes schema drift a write-time problem.
- **Single Parquet file per source, rewritten on append:** violates append-only.
- **`<file_hash>.parquet` only:** a forced same-day rerun would overwrite the file.
