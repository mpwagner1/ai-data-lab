# 0002: Bronze accepts any file, parses by format, and keeps the raw bytes

Date: 2026-11-12
Status: Accepted (approved by Michael 2026-11-12, with the `_sheet_name` column and `--delimiter` flag)

## Context

Bronze reads only comma-delimited `*.csv` (ADR 0001). Customers send Excel, pipe- and tab-delimited
files, JSON and more. We do not want to refuse a customer's data because we have no reader yet, and we
want a new format to cost one function, not a redesign. The bronze rules do not change: append-only,
every source column a string, no casting or cleaning, no row data in logs.

## Decision

- **Raw zone.** Every file in `data/landing/<source>/` is copied unchanged to
  `lake/raw/<source>/receive_date=YYYY-MM-DD/<sha256>.<orig-ext>` using the same temp-file-then-`os.link`
  publish as bronze. Landing files are never moved or deleted.
- **Pending is derived.** A raw file is pending when no bronze Parquet name starts with its hash (the
  ADR 0001 skip check). There is no manifest. Every run parses all pending files, so adding a reader or
  config replays held files with no extra command.
- **Held, not rejected.** A file with no reader, missing config or an unknown encoding stays pending and
  logs `bronze_unparsed` with a `reason`. The run exits 3 when nothing failed but files are held. A
  `--pending` flag lists them.
- **Reader registry.** `READERS: dict[FileFormat, Reader]`. A reader takes a path and a `SourceConfig`
  and returns `list[ParsedPart]`, each with an optional sheet name and an all-string `LazyFrame`.
  Metadata, hashing, the re-hash check, the atomic write and logging are shared.
- **Detection.** Order: `format:` in config, then file signature (`PK` zip container → xlsx or ods or
  archive, `D0CF11E0` xls, `PAR1` Parquet, `1F8B` gzip), then text heuristics (JSON array, NDJSON,
  else delimited). A delimited file's delimiter comes from config, then extension (`.csv` comma,
  `.tsv` tab, `.psv` pipe). There is **no delimiter sniffing**: an unknown `.txt` or `.dat` is held with
  `reason=needs_config`.
- **`--delimiter` flag.** `python -m ingestion.bronze <source> --delimiter '|'` supplies the delimiter
  for delimited files that have no extension default (`.txt`, `.dat`). It must be a single character.
  A `delimiter:` in the source config takes precedence, and the flag never overrides an extension
  default (a `.csv` stays comma). With no flag and no config, such a file is still held with
  `reason=needs_config` and the run exits 3. The flag is not stored, so a later run without it holds the
  file again only if it is still pending.
- **Per-source config.** Optional `config/sources/<source>.yaml` with `delimiter`, `encoding`, `format`
  and `sheets`. Unknown keys raise.
- **Workbooks.** One Parquet file per workbook with all sheets combined (diagonal concat) and a new
  `_sheet_name` metadata column (null for non-workbooks). `_row_number` restarts at 1 in each sheet.
  Excel is read with `drop_empty_rows=False` and `drop_empty_cols=False` so no rows are silently
  dropped. A blank header cell becomes `unnamed_<n>`, the one documented rename, because the source
  supplied no name and `_`-prefixed names are reserved.
- **Archives.** `.gz` and `.zip` are expanded; each member is its own unit with its own hash and
  `_file_name = "<archive>!<member>"`. Caps on total uncompressed size and member count, member names
  never used as paths, nested archives not expanded.
- **Metadata contract.** `_sheet_name` (string, the sheet a row came from, null for files that are not
  workbooks) is a fifth metadata column, added to `contracts/bronze/_metadata.yaml`. Older bronze files
  read back with null `_sheet_name` via `union_by_name`, so no migration.

## Consequences

- A customer's file is always safe once it lands, whether or not we can read it yet.
- New format = one reader function, one fixture, one registry line.
- Most variations (delimiter, encoding, sheets) need config, not code.
- Parquet can be rebuilt from the raw zone if a reader has a bug.
- The raw zone roughly doubles storage.
- Excel stores `001` as the number 1, so the leading zeros are already gone in the file. Numbers come
  back in calamine's string form (for example `1.0`). The raw zone keeps the original bytes.
- Excel and JSON are read into memory, not streamed.
- Fixed-width, EBCDIC, PDF and proprietary binaries need a spec from the customer and stay pending
  until a reader exists. Fixed-width is the planned follow-up, configured with column positions.
- The single-writer and hard-link assumptions from ADR 0001 still apply, including to the raw zone.

## Alternatives considered

- **Reject unknown formats:** simple, but it is exactly the "we cannot take your data" answer.
- **Sniff delimiters:** zero config, but it can silently mis-split single-column or oddly quoted files.
  Explicit config fails loudly instead.
- **One Parquet file per sheet:** the skip check is per hash, so a crash after sheet 1 could mark a
  half-loaded workbook as done. One file per workbook is atomic.
- **A manifest of pending files:** a second source of truth that can drift from the lake. Deriving
  pending from file names cannot.
- **First sheet only:** silently drops data in the other sheets.
