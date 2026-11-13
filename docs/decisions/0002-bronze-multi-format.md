# 0002: Bronze accepts any file, parses by format, and keeps the raw bytes

Date: 2026-11-12
Status: Accepted (approved by Michael 2026-11-12, with the `_sheet_name` column and `--delimiter` flag).
Amended 2026-11-12: `header_row` config key and sheet-row `_row_number` for workbooks, detection wording
matched to the code, archive handling (caps, failure rules, member safety), the JSON, NDJSON, Parquet and
UTF-16/32 encoding rules, and the final CLI behavior (real `--pending`, log levels, error text), all as built.

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
  logs `bronze_unparsed` with a `reason` (at WARNING). The run exits 3 when nothing failed but files
  are held. A `--pending` flag lists every file that has no bronze rows yet, with the outcome a real
  run would give. It runs the same read path as a real run (detect, the readable check, then the
  reader and the metadata step) and discards the frame, so the two cannot disagree: `needs_config`,
  `no_reader` or `encoding` for a file a run would hold, `failed` with the error type only for a file
  a run would fail (corrupt content, a bad header, an exceeded cap), and `ready` for a file that would
  load. A `ready` file still counts as not loaded, so `--pending` exits 3 when there is anything to
  load. The cost is that `--pending` parses every pending file, about as long as a real run. It writes
  nothing.
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
    kept, the rest of the batch continues). They are not held. The error is `ExcelReadError`, a
    `ValueError` like the other reader errors. Messages and logs never carry
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
- **JSON, NDJSON and Parquet.** Registered readers for a top-level JSON array of objects, NDJSON (one
  object per line) and Parquet. Every source column is `pl.String`.
  - **JSON and NDJSON keep the source text of scalars.** Parsing uses `parse_float=str`,
    `parse_int=str` and `parse_constant`, so `1.50`, `1e3`, `1E+3`, `-0`, a 24-digit integer, `NaN`
    and `Infinity` arrive exactly as written. `true` and `false` arrive as `"true"` and `"false"`,
    `null` and a missing key as null, strings as they are. Nested objects and arrays are encoded to
    a compact JSON string by our own encoder (key order kept, no ASCII escaping, numbers inside
    keep their source text), because `json.dumps` cannot keep number text. A `NaN` or `Infinity`
    inside a nested value is written as a bare token, so that cell is not strict JSON; bronze
    keeps what the source sent. Columns are the union of keys in first-seen order. A `_`-prefixed
    key, an empty array or file, `[{}]`, a non-array top level, a non-object element or line, and a
    trailing comma fail the file. Duplicate keys in one object: the last one wins. A UTF-8 BOM is
    stripped. JSON is decoded with the source config `encoding` (default UTF-8); text that does not
    decode is held as `encoding` like CSV, while a lone surrogate (valid JSON, not encodable) fails
    the file. NDJSON is parsed line by line (the file is not read into one string) and blank lines are skipped; the rows are all kept in memory like every format. A single-line `.json`
    file holding one object (`{"data":[...]}`) is detected as NDJSON and read as one row with the
    nested value JSON-encoded; detection is not changed for it.
  - **Parquet is the one source format with its own types**, so string conversion is a real
    conversion, with the original file kept in the raw zone. The file is read eagerly. The forms
    (Polars 2.1): integers as digits, booleans `true` and `false`, floats as the shortest round-trip
    text (`1.0`, `0.1`, `1e+20`, `NaN`, `inf`), decimals as exact digits at the column scale
    (`1.50`), dates `YYYY-MM-DD`, times `HH:MM:SS`, timestamps `YYYY-MM-DD HH:MM:SS.<fraction>`
    with a fraction that follows the column's time unit (3, 6 or 9 digits) and an offset when the
    column has a time zone, durations as ISO-8601 text (`PT1M30S`, `-P1D`), and binary as lowercase
    hex. Structs, lists and arrays become compact JSON, where dates and decimals inside become JSON
    strings and NaN and inf are bare tokens (not strict JSON). Binary nested inside a struct or
    list fails the file. A `_`-prefixed column, a file with no columns, and a bronze Parquet fed
    back in fail the file; a zero-row file with columns is written as a zero-row file.
  - **Parquet MAP columns** become a JSON list of key and value pairs, for example
    `[{"key":"a","value":"x"}]`, with an empty map as `[]` and a null map as null. The list form was
    chosen over a JSON object because map keys need not be strings, so one form covers every key
    type without a key-stringification rule. Maps inside structs, lists, arrays and other maps are
    rewritten recursively. The cost for silver is that looking a key up needs extra work and
    duplicate keys are kept.
  - **Failures.** Reader errors (`JsonReadError`, `ParquetReadError`, both `ValueError`, like `ExcelReadError` and `ArchiveReadError`) name the
    error type and file only, are raised outside the `except` block so `__context__` and
    `__cause__` are `None`, and never carry keys, values, record text or library messages. The
    file fails (exit 1, raw copy kept, the batch continues); it is not held.
- **Text encodings and UTF-16/32.** The source config `encoding` is applied before detection, so
  NUL bytes in UTF-16/32 text do not hide it. A file that could be loaded wrongly is held as
  `encoding` (not written, exit 3, raw copy kept) rather than loaded as garbage, because a wrong
  load is silent and permanent while a hold is visible in `bronze_unparsed` and `--pending`.
  - **Plain `utf-16` and `utf-32` need a BOM.** The explicit-endian codecs (`utf-16-le`,
    `utf-16-be`, `utf-32-le`, `utf-32-be`) do not. A UTF-8 BOM read with any codec other than
    `utf-8-sig` is held. UTF-8 data read with a UTF-16/32 config holds (genuine multi-line
    UTF-16/32 text always contains NUL bytes), and so does text whose decoded header contains
    U+FFFD, a control character, or an unassigned, private-use or surrogate character.
  - **The NUL rule is narrow.** A stray NUL inside otherwise valid UTF-8 loads unchanged at any
    offset. A delimited file is held only when its NUL pattern looks like UTF-16/32 or strict UTF-8
    decoding fails. A binary file named `.csv` that is not valid UTF-8 stays `no_reader`.
  - **Byte order is decided by ASCII code units only** (`_wrong_byte_order`). An ASCII character
    has exactly one non-zero byte, and which byte tells the order; other characters are ignored
    because a correct CJK file contains characters that look swapped (U+4E00 and U+3000 read in the
    other order are `N` and `0`). Structural ASCII (tab, newline, CR, comma, semicolon, pipe,
    quote) votes first; only a sample with none falls back to structural plus letters and digits.
    The opposite order must lose strictly, so **a tie holds**. Only the first 64 KB is looked at.
  - **Held although correct** (a visible cost): a one-line file with no ASCII, a one-line file of
    look-alike characters only, a file like `名前　都市　東京` (ideographic spaces and no
    structural ASCII, letters or digits), a file whose votes tie, and a file whose first line is
    over about 32K non-ASCII characters. **Can still load although wrong-endian** (the evidence is
    ambiguous or outweighed): a one-line file made only of look-alike characters (`"一"` written
    little-endian and read as big-endian gives the header `N`), and a file whose structural ASCII
    is outvoted by look-alike structural characters such as U+2C00, U+0A00, U+7C00 or U+2200. In a
    seeded differential test of about 3000 random texts, no wrong-endian file containing real
    structural ASCII loaded unless such characters outvoted it.
  - **Limits.** General mojibake (for example UTF-8 read as `cp1252`) cannot be detected from the
    bytes, so it loads; it is the same for every single-byte encoding. The unassigned-character
    rule follows Python's Unicode data (3.12 is Unicode 15.0), so a very new character in a UTF-16
    header can cause a visible false hold.
- **Symlinks.** Symlinks inside a landing folder are skipped and counted (`bronze_ignored
  source=<source> symlinks=K`, counts only), never followed, so nothing outside the landing folder can be copied
  to the raw zone or parsed. The landing folder itself may be a symlink (for example to a mounted
  share): that is an operator's explicit choice, so it is followed. The public `ingest_file`
  called directly with a symlink path also follows it (the link name becomes `_file_name`). The two
  followed cases are pinned by tests; refusing them would be a behavior change to decide first.
- **Logs and errors.** Log lines are `key=value` with string values JSON-quoted. Levels: a failed
  file (`bronze_error`) logs at ERROR, a held file (`bronze_unparsed`) at WARNING, as do
  `bronze_stale_temp` and `bronze_no_files`; every other event is INFO, so an alert on ERROR sees
  failed files and setup errors alike. Setup errors (a landing folder that is missing, cannot be
  listed (permission denied) or is not a directory, a bad source name or config) exit 2 and log only the error type and the source name, never the message, which can carry paths or
  config text. Reader and config errors name the error type and file only and are raised outside the
  `except` block (the `_guarded` pattern), so `__context__` and `__cause__` are `None`; the library
  error, which can hold file bytes or config text (a `UnicodeError` holds the raw bytes), is never
  attached. The same holds for the encoding hold and for config validation. The `_`-prefixed column
  error has a fixed text with no column names.
- **Archives.** `.zip` and `.gz` are expanded (standard library `zipfile` and `gzip`, no new
  dependency). The archive itself is copied once, unchanged, to the raw zone (also when it later
  fails); members are not stored separately. Each member is its own unit: its own hash (SHA-256 of
  the member bytes), its own bronze Parquet file through the normal atomic write, its own skip, and
  `_file_name = "<archive>!<member>"` (a `.gz` has one member named after the archive without
  `.gz`, for example `orders.csv.gz!orders.csv`). No metadata column was added.
  - **Members are untrusted.** A member name, size or content can be hostile. A name is stored only
    as data in `_file_name`. It is never used, even in part, to build a path: each member is
    streamed to a private `tempfile.TemporaryDirectory` (mode 0700) under a generated name,
    `member-<index>` plus an extension from a fixed whitelist (`.csv .tsv .psv .txt .dat .json
    .ndjson .jsonl .xlsx .xlsm .xls .ods .parquet`, anything else gets none). So `../x`, absolute
    paths, backslashes, drive letters, UNC paths, NULs, very long names and control characters
    cannot escape. Names never appear in logs or exception messages: logs use `<archive>!#<index>`
    and the member hash. Directory entries, names starting with `.` or `~$`, `__MACOSX/` entries
    and symlink entries are skipped and counted in one line per archive, `bronze_ignored
    source=<source> file_name=<archive> members=N symlinks=K` (counts only; `members` counts every
    skipped entry, directories and symlinks included; written only when N is above 0). The archive
    file name is the landing file's own name, not a member name.
  - **Caps** (module constants, no config keys): `MAX_MEMBERS = 100` per archive,
    `MAX_TOTAL_UNCOMPRESSED_BYTES = 512 MiB` per archive, `MAX_MEMBER_BYTES = 256 MiB` per member.
    They are enforced on the bytes actually decompressed while streaming, not on the sizes in the
    zip headers (headers can lie), so a gzip bomb, a high-ratio zip and concatenated gzip streams
    are stopped as they grow. The member count, encryption and unsupported compression methods
    are checked from the zip's central directory before anything is written. Supported
    compression: stored, deflate, bzip2 and lzma; any other method fails the archive.
  - **Failures.** A corrupt, truncated, bad-CRC, encrypted or unsupported archive, an exceeded cap,
    or a gzip with trailing non-zero garbage fails the archive: exit 1, raw copy kept, the rest of
    the batch continues, and it is not held. Errors are `ArchiveReadError` (a `ValueError`) naming
    the error type and the archive file only, raised outside the `except` block so `__context__`
    and `__cause__` are `None`. Checks up front write nothing. A failure mid-stream (a byte cap, a
    bad CRC) leaves members already completed in place, because each member is its own atomic
    unit; the failing member and later ones are not written. Temp files are removed on every
    failure path.
  - **Members follow the normal rules.** Each member is detected and read like a landing file
    (`format:` config, then signature, then text heuristics, with the extension from the
    whitelist), and the source config and `--delimiter` apply. A member that needs config, has no
    reader or has an encoding problem is held; a member with corrupt content fails; other members
    still load. Exit precedence stays 2, then 1, then 3, then 0.
  - **Nested and other archives are never expanded.** A member that is itself a zip or gzip is
    held as `no_reader` (exit 3). So are `.tar`, `.tar.gz`, `.tgz`, `.7z` and `.rar`. If
    `format:` is set in the source config, it replaces detection and the archive is read as that
    format, not expanded.
  - **Reruns.** A member whose hash already has a bronze file is skipped, so a rerun skips every
    member. The skip is by content: an identical member in a second archive is skipped too, and
    its `_file_name` is that of whichever archive loaded first. A rerun still re-reads archives
    whose members are all ingested, because the skip is per member hash and needs the member
    bytes; the caps bound the cost.
  - **`--pending`** expands archives too, read-only: it extracts to a temp directory outside the
    lake, writes nothing, and lists pending members by archive name and member index. Each
    member gets the same real-run outcome as a file (`ready`, `needs_config`, `no_reader`,
    `encoding` or `failed`). A broken archive is listed as one pending item with
    `reason=archive_error` and counts toward exit 3.
- **Metadata contract.** `_sheet_name` (string, the sheet a row came from, null for files that are not
  workbooks) is a fifth metadata column, added to `contracts/bronze/_metadata.yaml`. Older bronze files
  read back with null `_sheet_name` via `union_by_name`, so no migration. `_row_number` means: for
  delimited files the 1-based position among data rows (the header is not counted; blank lines are
  numbered rows with null values); for JSON the 1-based position among the elements of the
  top-level array; for NDJSON the position among the records (blank lines do not count); for
  Parquet the 1-based row position; for a workbook the row number as shown in the sheet, per
  sheet (see Header row); and for an archive member the numbering of the reader for the member's
  own format, so every member starts again at its own first row. The contract file states the
  same.

## Consequences

- A customer's file is always safe once it lands, whether or not we can read it yet.
- New format = one reader function, one fixture, one registry line.
- Most variations (delimiter, encoding, sheets) need config, not code.
- Parquet can be rebuilt from the raw zone if a reader has a bug.
- The raw zone roughly doubles storage.
- Excel stores `001` as the number 1, so the leading zeros are already gone in the file. Numbers come
  back in calamine's string form (for example `1.0`). The raw zone keeps the original bytes.
- Every file is collected into one in-memory frame before the write (the metadata step collects each
  part, then the parts are stacked), whatever the format, so peak memory is the size of the frame
  plus the decode copies. CSV, TSV and NDJSON are parsed lazily or line by line, which keeps the
  parse itself cheap, but all rows still end up in memory. Archive members are streamed to disk
  under the caps above, but each member is then read by its own reader and collected the same way.
  In a QA measurement a 40 MB UTF-8 CSV (1.4M rows) peaked at about 270 MB and an 80 MB UTF-16-LE
  CSV at about 300 MB.
- Archives add one more decompression pass per rerun and a temp directory per run; the raw zone
  holds only the archive, so a corrupt member is fixed by reprocessing from it.
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
