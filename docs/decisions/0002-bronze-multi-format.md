# 0002: Bronze accepts any file, parses by format, and keeps the raw bytes

Date: 2026-11-12
Status: Accepted (approved by Michael 2026-11-12, with the `_sheet_name` column and `--delimiter` flag).
Amended 2026-11-12: `header_row` config key and sheet-row `_row_number` for workbooks (below); detection wording
matched to the code.

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
  or delimited when the extension is `.csv`, `.tsv`, `.psv` or the config says so). A delimited file's delimiter comes from the `--delimiter` flag (only for files with
  no extension default), then config, then extension (`.csv` comma, `.tsv` tab, `.psv` pipe). There is
  **no delimiter sniffing**: an unknown `.txt` or `.dat` with none of those is held with
  `reason=needs_config`.
- **`--delimiter` flag.** `python -m ingestion.bronze <source> --delimiter '|'` supplies the delimiter
  for delimited files that have no extension default (`.txt`, `.dat`). It must be a single character.
  The flag wins over a `delimiter:` in the source config (the usual rule: command line beats config),
  but it only applies to those files, so a `.csv` in the same folder stays comma. With no flag and no
  config, such a file is still held with
  `reason=needs_config` and the run exits 3. The flag is not stored, so a later run without it holds the
  file again only if it is still pending.
- **Per-source config.** Optional `config/sources/<source>.yaml` with `delimiter`, `encoding`, `format`,
  `sheets` and `header_row`. Unknown keys raise.
- **Workbooks.** One Parquet file per workbook with all sheets combined (diagonal concat) and a new
  `_sheet_name` metadata column (null for non-workbooks). `_row_number` is the row's position in the
  sheet as Excel shows it (see Header row). The `fastexcel` reader used here (calamine) has no
  `drop_empty_rows` or `drop_empty_cols` options, and its own `header_row=0` silently skips leading
  blank rows, which would break sheet-row numbering. So the reader loads each sheet with
  `header_row=None, skip_rows=0` (row `i` of the frame is sheet row `i + 1`, nothing is dropped) and
  applies the header row itself.
  - **Headers.** A blank header cell becomes `unnamed_<n>` (`n` is the 1-based position among the
    columns read), the one documented rename, because the source supplied no name and `_`-prefixed
    names are reserved. A header that starts with `_`, or two headers that are equal, **fail the
    file** with an error naming the error type and file only; they are never renamed. This is
    stricter than the delimited reader, which lets Polars suffix duplicates, because renaming
    would change source names. There is no config key to work around it; fix the source or add a
    reader option in a later change. Case-only duplicates pass the check and still read back
    through DuckDB.
  - **Value forms.** Cells arrive as strings in calamine's form, not always the text Excel shows.
    A stored `1.234567890123457e+19` arrives as `'12345678901234569216'` (the same double written
    out in full), dates and datetimes as `YYYY-MM-DD HH:MM:SS` (date versus datetime is lost),
    and formulas as their cached value or null when none was stored. Our code does not cast,
    round or reformat. The original workbook stays in the raw zone.
  - **Limits.** Calamine drops leading blank columns, so `unnamed_<n>` is relative to the first
    used column, not column A, and trailing blank rows are not emitted. Hidden sheets are read.
    Merged ranges fill only their top-left cell. Every sheet is loaded and collected in memory
    with no row or size cap (caps come with archives in task 5). A sheet with no cells is skipped,
    and a workbook with no sheet that has cells fails.
  - **Failures.** A corrupt, truncated or password-protected workbook, a `sheets:` name that is
    not in the workbook, and a `header_row` past the end of a sheet fail that file (exit 1, raw copy
    kept, the rest of the batch continues). They are not held. Messages and logs never carry
    sheet names, headers or cell values; the per-sheet `bronze_sheet` log line carries the
    1-based sheet index, `header_row`, the skipped-row count and the row count.
  - **Detection.** A zip that is broken but has extension `.xlsx`, `.xlsm` or `.ods` is detected
    as that workbook (so it fails instead of being held as an archive). A non-Excel OLE file now fails
    instead of being held as `no_reader`.
- **Header row (workbooks).** Real sheets often start with blank or title rows above the headers.
  `header_row: <n>` in the source config (1-based, default 1, applies to every sheet read) names the
  sheet row that holds the column headers; the rows above it are not read into bronze. The skipped
  rows stay in the raw zone, so this is a parsing instruction, not a deletion, and a wrong value is
  fixed by changing the config and reprocessing. A bronze run logs `header_row` and the count of
  skipped rows per sheet, never their content. A `header_row` beyond the end of a sheet raises a
  clear error for that file (it is not held) and exits 1. There is no auto-detection of the header
  row: a title or note above the headers can carry information, and a guess would be silent. Applying
  `header_row` to delimited files (preamble lines) is not part of this change.
- **`_row_number` for workbooks.** For a workbook row it is the row number as shown in the sheet, so
  a person can open the original file and find the row. With `header_row: 1` the first data row is
  2; with `header_row: 6` it is 7. It still counts per sheet (each sheet has its own numbering) and
  empty rows inside the data keep their numbers. This differs from delimited files, where
  `_row_number` is the 1-based position among data rows (the header is not counted). Both are
  documented in `contracts/bronze/_metadata.yaml`; bronze files already written are not changed.
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
