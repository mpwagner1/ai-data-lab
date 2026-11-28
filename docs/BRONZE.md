# Bronze ingestion

How customer files get from a landing folder into bronze, what each format becomes, how to run
the loader, and how the code is laid out. The reasons behind the design are in
[`decisions/0001-bronze-parquet-storage.md`](decisions/0001-bronze-parquet-storage.md) (storage)
and [`decisions/0002-bronze-multi-format.md`](decisions/0002-bronze-multi-format.md) (formats,
raw zone, held files). This guide says what the code does; the ADRs say why.

## 1. What bronze is

Bronze is the raw layer of the medallion platform (bronze, silver, gold). It keeps what the
source sent, plus enough metadata to trace every row back to its file.

- **Append-only.** Nothing is updated, overwritten or deleted. A reload adds a new file.
- **Source text, no casting, no cleaning.** Every source column is a string. Bronze does not
  trim, round, reformat, rename values or filter rows. Typing and validation happen in silver,
  where a contract defines what is valid. An unquoted empty CSV field is null; a quoted empty
  one is an empty string.
- **Five metadata columns on every row** (defined in
  [`contracts/bronze/_metadata.yaml`](../contracts/bronze/_metadata.yaml)):

| Column | Type | Meaning |
| --- | --- | --- |
| `_file_name` | string | The landing file name (not the path). For an archive member: `<archive>!<member>`, for example `orders.zip!orders_2026.csv` and `orders.csv.gz!orders.csv` |
| `_ingested_at` | timestamp (UTC) | When the run started. Every file written in one run has the same value |
| `_file_hash` | string | SHA-256 of the file's bytes (for an archive member, of the member's bytes) |
| `_row_number` | int | Row position, counted per file (per sheet for workbooks). See below |
| `_sheet_name` | string | The sheet a row came from. Null for files that are not workbooks |

`_row_number` means different things by format, so read the column with its format in mind (the
same rules as [`contracts/bronze/_metadata.yaml`](../contracts/bronze/_metadata.yaml)):

- **Delimited text:** 1-based position among data rows. The header is not counted.
- **JSON:** 1-based position among the elements of the top-level array.
- **NDJSON:** 1-based position among the records. Blank lines are skipped and do not count.
- **Parquet:** 1-based row position in the file.
- **Workbooks:** the row number as Excel shows it. With `header_row: 1` the first data row is 2;
  with `header_row: 6` it is 7. Each sheet is numbered on its own, and empty rows inside the data
  keep their numbers, so a person can open the original file and find the row.
- **Archive members:** the reader numbers each member as if it were a landing file of its own
  format, so every member starts again at its own first row.

Why these columns: `_file_hash` lets silver see a file that was loaded twice, and `_file_name`
plus `_row_number` point back to the exact row in the source.

## 2. The flow

```
data/landing/<source>/<file>              the customer's file (git-ignored, never changed)
        |  receive: hash, copy byte for byte
        v
lake/raw/<source>/receive_date=YYYY-MM-DD/<file_hash><ext>     the exact bytes, kept forever
        |  detect format, read, add metadata, re-hash the source
        v
lake/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<8 hex>.parquet
```

**Landing.** Drop files in `data/landing/<source>/`, or stage them from Azure Blob Storage (section 10). Hidden files, `~$` owner files, lock files,
subfolders and symlinks are not loaded (section 7). The loader never moves or deletes a landing file.

**Receive (raw zone).** Each file is hashed and copied unchanged to
`lake/raw/<source>/receive_date=YYYY-MM-DD/<file_hash><ext>`. `<ext>` is the lowercase original
extension (a dot and 1 to 10 letters or digits), otherwise none; `.tmp` is reserved and never
kept. The copy is hashed again, and if
the same bytes were received before (any day, any name) no second copy is made. An earlier copy
counts only if it is a regular file (not a symlink) whose bytes hash to `<file_hash>`. Every
matching entry in every partition is checked on each run, and one bad entry fails the file, even
when another partition holds a good copy (section 5, Unusable lake entries).

**Skip check.** A file counts as already loaded when a bronze entry's name starts with its hash and
the entry is usable: a regular file (not a symlink) with a readable Parquet footer. Such
files are skipped before any reader runs (`bronze_skip`), so a changed config does not re-parse
them. If any matching entry is not usable, the file fails and nothing is written for it. An archive
is the exception: it is expanded again on a rerun so that each member's hash can be checked
(section 4).

**Parse and write.** The format is detected (section 4) and the file is read into strings, with
metadata added and the result collected in memory. The source is hashed again; if it changed during
the read, the run fails rather than write rows that do not match `_file_hash`. The frame goes to a
hidden temp file with a unique name (`.<16 hex>.parquet.tmp`) and is published with `os.link`, which
fails if the target name exists. The temp file is synced to disk (`fsync`) before the link and the
target's folder after it. If the folder sync fails after the link, the file is already at its final
name and stays there: this run reports the item failed. The next run finds a usable entry, so a
bronze file is skipped (`bronze_skip`), a raw copy counts as received (the load then goes ahead),
and Azure staging skips the identical landing file. The folder is not synced again. The temp file is
removed either way, so a run that stops early leaves a
`.tmp` file, never a half-written Parquet file. The raw copy and Azure staging use the same helper. Names
look like this (the hash is 64 hex characters, shortened here):

```
lake/bronze/orders/ingest_date=2026-11-11/3f9c...e21a20261111T163300Z_1a2b3c4d.parquet
```

`<run_ts>` is the UTC start of the run (`YYYYMMDDTHHMMSSZ`). The random 8-hex suffix means two
loads in the same second never collide, so `--force` over identical files works. A header-only
file (a header and no rows) is written as a zero-row Parquet file. That records the load, so the
next run skips it instead of reading it again.

**Pending.** A raw file is pending when no bronze file name starts with its hash (an unusable
entry does not count as loaded; it fails the file). There is no
manifest, so every run tries every pending file: adding a reader or a config key replays held files
with no extra command. A failed file is pending too and is tried again next run. `--pending` lists
them (section 5).

**Reruns and `--force`.** A rerun over the same folder writes nothing and logs `bronze_skip` for
each loaded file. With `--force` a loaded file is written again as a new bronze file and the earlier
one stays. The rows are duplicated on purpose; silver dedupes on `_file_hash`. The raw zone is not
copied again. `--force` only ignores the fact that a usable bronze entry exists. The entry checks
still run: with an unusable raw or bronze entry the file fails and nothing is written.

## 3. How to run it

Run from the project root. `uv sync` installs the project, including the `ingestion` package from
`src/` (section 8), so `uv run python -m ingestion.bronze` needs no other setup. The paths
`data/landing`, `lake` and `config/sources` are relative to the current directory.

```bash
mkdir -p data/landing/<source>        # put the files here (git-ignored)
uv run python -m ingestion.bronze <source>                  # load new files
uv run python -m ingestion.bronze <source> --pending        # list what is not loaded, and why
uv run python -m ingestion.bronze <source> --force          # load again; keeps the earlier copy
uv run python -m ingestion.bronze <source> --delimiter '|'  # for .txt and .dat files
```

`<source>` is a name you choose, for example `orders`. It must start with a letter or digit and
use only letters, digits, `_` and `-`, because it becomes a folder name. Any other name exits 2.

| Flag | What it does |
| --- | --- |
| `--pending` | Lists every landing file, and each archive member, with no bronze rows yet, with its outcome (section 5). Reads each one the way a run does and writes nothing. Exits 3 if any item is listed, else 0. `--force` has no effect with it |
| `--force` | Loads every file again, even with a usable bronze entry (section 2). Unusable raw or bronze entries still fail the file |
| `--delimiter X` | One character, the delimiter for `.txt` and `.dat` files. Beats the source config `delimiter`, but only for those two extensions: a `.csv`, `.tsv` or `.psv` keeps its own delimiter. Must be exactly one byte in UTF-8 and not a quote or a line break, otherwise the run exits 2 (`error=InvalidDelimiter`). For a tab, pass a real tab (`$'\t'` in zsh or bash), not the two characters `\t`. The flag is not stored, so a later run without it holds the file again |

Read the data back with DuckDB:

```python
from pathlib import Path
from ingestion.bronze import read_bronze

read_bronze("<source>", Path("lake")).show()
```

Every bronze file of the source is read with `union_by_name`, so files with different columns
load together and a column missing from a file comes back as null. `_sheet_name` is null for old
files written before it existed.

`lake_root` is used as written. A root with a backslash in a folder name is refused with
`ValueError` (except on Windows, where it is the separator).

### Checking an existing lake

Check each lake written before this version of the readers, once per source. List the file names
to verify:

```python
from pathlib import Path
from ingestion.bronze import read_bronze

read_bronze("<source>", Path("lake")).filter(
    "contains(_file_name, '*') or contains(_file_name, '?') or contains(_file_name, '[')"
).project("_file_name, _file_hash, _ingested_at").distinct().show(max_rows=10000)
```

No rows means there is nothing to do. An archive member (`<archive>!<member>`) needs no check.
If code of your own called the loader with a folder path that holds one of those characters or
starts with `~`, check every file loaded that way too. For each name to check, load it again:

1. Stop runs for the source.
2. Move `lake/bronze/<source>/ingest_date=*/<_file_hash>*.parquet` to a folder outside the lake.
   Move rather than delete, so the old and new rows can be compared.
3. Make sure the file is in `data/landing/<source>/` under its original name, with bytes whose
   SHA-256 is `_file_hash`. If not, copy the raw copy
   (`lake/raw/<source>/receive_date=*/<_file_hash><ext>`) there.
4. Run the source again. Anything already built from the moved files needs rebuilding.

### Source config: `config/sources/<source>.yaml`

Optional. Without the file every key has its default. The file is YAML, read with `safe_load`
from `config/sources/` in the current directory. `.yml` is not read.

| Key | Type | Default | Rules |
| --- | --- | --- | --- |
| `delimiter` | string, one character | none | Used for every delimited file of the source, `.csv` included, so only set it when all its delimited files share one. Exactly one byte in UTF-8, not `"`, CR or LF. Without it, `.csv` is comma, `.tsv` tab and `.psv` pipe; `.txt` and `.dat` have no default and need this key or `--delimiter` |
| `encoding` | string | `utf-8` | A text codec (for example `cp1252`, `utf-16`). Applies to delimited, JSON and NDJSON files, including archive members. Ignored for workbooks and Parquet. Unknown or non-text codecs exit 2 |
| `format` | string | none (detect) | One of `delimited`, `xlsx`, `xls`, `ods`, `json`, `ndjson`, `parquet`, `zip`, `gzip`. Replaces detection for every file of the source, including archive members. Use it only for a source that holds one format. An unknown value exits 2 |
| `sheets` | list of strings | all sheets | Workbooks only. Only these sheets are read, in workbook order. A name that is not in the workbook fails that file. An empty list selects nothing and the workbook fails |
| `header_row` | positive integer (not a boolean) | `1` | Workbooks only. The 1-based sheet row holding the column headers, for every sheet read. Rows above it are not read into bronze (they stay in the raw zone). A value past the end of a sheet fails that file. Ignored for other formats |

Unknown keys exit 2. The error logs the exception type and the source name only, never the key
names or the config text, because a message can echo config values.

Three synthetic examples (the source names are made up):

```yaml
# config/sources/orders_txt.yaml: a pipe-delimited .txt file
delimiter: "|"
```

```yaml
# config/sources/orders_xlsx.yaml: an Excel file with the headers on sheet row 6
header_row: 6
# sheets: [Orders]     # optional: read only this sheet
```

```yaml
# config/sources/customers_cp1252.yaml: a Windows-1252 CSV file
encoding: cp1252
```

## 4. Supported formats

Detection runs in this order: the `format:` key in config, then the file signature (the first
bytes), then text rules. A file that matches none is held as `no_reader`. Bronze never guesses a
delimiter. Only the first 64 KB are looked at for detection.

| Format | How it is detected | What the reader does | Value form and limits |
| --- | --- | --- | --- |
| Delimited text: `.csv`, `.tsv`, `.psv` | Text with that extension (these never take the JSON rules) | Polars CSV with schema inference off. UTF-8 is scanned lazily; other encodings are decoded fully in memory first | Comma, tab and pipe by extension, or the config `delimiter`. Unquoted empty is null, quoted empty is `""`, spaces are kept, a UTF-8 BOM is removed. Header only gives a zero-row file. A zero-byte or whitespace-only file fails. A header starting with `_` fails. Polars suffixes duplicate header names (ADR 0002) |
| Delimited text: `.txt`, `.dat` | Text with that extension | Same reader | No default delimiter: from `--delimiter`, then config `delimiter`. With neither, the file is held as `needs_config`. Never sniffed |
| JSON array | Text (any extension except `.csv`, `.tsv`, `.psv`) whose first non-blank character is `[` | One array of objects. Columns are the union of keys in first-seen order | Scalars keep their source text: `1.50`, `1e3`, `-0`, 24-digit integers and `NaN` are exact. `true` and `false` become `"true"` and `"false"`. Nested objects and arrays become compact JSON text. Duplicate keys: the last value wins. An empty array, a non-object item, a key starting with `_`, or `[{}]` fails the file. Whole file in memory |
| NDJSON | Text whose first line is one complete JSON object (a single-line `{...}`) | One object per line; blank lines skipped | Same value rules as JSON. A pretty-printed multi-line object is detected as JSON and fails (it is not an array). A single-line envelope such as `{"data":[...]}` is one row, with the nested value as JSON text |
| Excel: `.xlsx`, `.xlsm` | Zip signature with `xl/workbook.xml` inside, so an xlsx saved as `.csv` is still a workbook. A broken zip with that extension fails as a workbook | fastexcel (calamine). Every sheet (or `sheets:`) is read and all sheets go into one Parquet file with `_sheet_name` | Cells are strings in calamine's form, which is not always the text Excel shows: `1.0` for a number, a large stored number in full digits, dates and datetimes as `YYYY-MM-DD HH:MM:SS` (date versus datetime is lost), formulas as their cached value or null, merged cells filled only in the top-left cell. Blank header cells become `unnamed_<n>`; a header starting with `_` or a duplicate header fails the file. A sheet with no cells is skipped; a workbook with no sheet with cells fails. Leading blank columns are dropped by calamine, so `unnamed_<n>` counts from the first used column. Hidden sheets are read. Whole sheets in memory, no cap |
| Excel: `.xls` | Compound-file signature `D0CF11E0`. A non-Excel file with that signature fails | Same reader | Same rules as `.xlsx` |
| OpenDocument: `.ods` | Zip with `content.xml` and `mimetype` inside. A broken zip with that extension fails | Same reader | Same rules as `.xlsx` |
| Parquet | `PAR1` signature | Polars, read eagerly. Every column is converted to a string | Integers as digits, booleans `true`/`false`, floats as the shortest round-trip text (`1.0`, `1e+20`, `NaN`), decimals at the column scale (`1.50`), dates `YYYY-MM-DD`, times `HH:MM:SS`, timestamps `YYYY-MM-DD HH:MM:SS.<fraction>` (the fraction follows the column's time unit, plus an offset when there is a time zone), durations as ISO-8601 (`PT1M30S`), binary as lowercase hex. Structs, lists and arrays become compact JSON. Maps become a JSON list of key and value pairs, for example `[{"key":"a","value":"x"}]`, because keys need not be strings. Binary nested inside a value fails the file. A column starting with `_` (including a bronze file fed back in), or no columns, fails. A zero-row file with columns is fine |
| gzip: `.gz` | `1F8B` signature | Archive (below). One member, named after the archive without `.gz` (`orders.csv.gz` gives `orders.csv`) | Member detected and read like a landing file |
| zip: `.zip` | `PK` signature, not a workbook or ODS | Archive (below). Each member is its own unit | Member detected and read like a landing file |
| Anything else | No signature, and the text rules do not match | Nothing | Held as `no_reader`. Includes binary files, and files with an extension bronze does not read (`.bin`, `.tar`, `.7z`, `.rar`) |

Fixed-width text needs care. In a `.txt` or `.dat` file it is held as `needs_config` until a
delimiter is set. Setting one would load it as delimited text, so leave the delimiter unset until a
fixed-width reader exists.

**Archives.** The archive is copied once to the raw zone, unchanged. Each member is then its own
unit: its own SHA-256 (of the member's bytes), its own skip check, its own bronze file through the
same atomic write, and `_file_name` is `<archive>!<member>`. Members are expanded from the raw
copy, not the landing file, after its hash is checked, so they always come from received bytes.
Members are
read with the source
config and `--delimiter` like any landing file. A member that needs config or has no reader is
held; a member with corrupt content fails; the other members still load.

- Caps (module constants in `src/ingestion/archives.py`, not config keys): 100 members per
  archive (`MAX_MEMBERS`), 512 MiB decompressed per archive (`MAX_TOTAL_UNCOMPRESSED_BYTES`) and
  256 MiB per member (`MAX_MEMBER_BYTES`). They count the bytes actually decompressed, not the
  sizes in the headers, so a gzip bomb is stopped as it grows.
- Encrypted members and unsupported compression fail the archive. Supported methods: stored,
  deflate, bzip2 and lzma.
- Directory entries, `.` and `~$` names, `__MACOSX/` entries and symlink entries are skipped and
  counted in one `bronze_ignored` line.
- Nested archives are not expanded. A zip or gzip member is held as `no_reader`, and so are
  `.tar`, `.tar.gz`, `.tgz`, `.7z` and `.rar`.
- If `format:` is set in config it replaces detection, so an archive is read as that format
  instead of being expanded.

## 5. Held, failed, pending and exit codes

A file is **held** when it is kept but cannot be read yet. Nothing is written for it, it stays
pending, and the run exits 3 if nothing failed. A file **fails** when reading it raises an error.
Nothing is written, the raw copy is kept, the rest of the batch continues, and the run exits 1.
Failed files are tried again on every run, as are held ones.

### Reasons

A run and `--pending` give each file one reason. Both use the same read path
([`src/ingestion/pending.py`](../src/ingestion/pending.py) and `bronze.py`), so they cannot disagree.

| `reason` | What it means | In a run | With `--pending` |
| --- | --- | --- | --- |
| `needs_config` | A `.txt` or `.dat` file with no delimiter from `--delimiter` or config | held: `bronze_unparsed` (WARNING) | `bronze_pending reason=needs_config` |
| `no_reader` | The format is not read yet: a binary file, a nested archive, `.tar`, fixed-width text, or a file with no recognised signature | held: `bronze_unparsed` | `bronze_pending reason=no_reader` |
| `encoding` | The text does not decode with the config `encoding`, or it looks like UTF-16/32 with the wrong byte order or a stray BOM (section 6) | held: `bronze_unparsed` | `bronze_pending reason=encoding` |
| `failed` | The file would fail: empty or corrupt content, invalid JSON, a bad workbook, an exceeded cap, an unusable lake entry, or a file that cannot be read at all (for example no read permission) | failed: `bronze_error` (ERROR), exit 1 | `bronze_pending reason=failed error=<type>`; no `file_hash` if the file cannot be hashed |
| `archive_error` | The archive itself cannot be read (corrupt, encrypted, unsupported compression, over a cap) | failed: `bronze_error error=ArchiveReadError`, exit 1 | one `reason=archive_error` line for the archive, no hash. Members listed before a failure found mid-stream (a bad CRC, for example) stay listed |
| `ready` | The reader and config are there and the read succeeded, so a run writes it | written: `bronze_write` | `bronze_pending reason=ready` |
| `skipped` | Same bytes as an earlier `ready` file in the same `--pending` list (the first copy in run order is the one that loads) | `bronze_skip` for the later copy | `bronze_pending reason=skipped` |

A copy of a file that is held or failed gets the same reason as the first copy, since a run would
hold or fail it the same way.

### `--pending`

`--pending` lists every landing file, and each archive member, that has no bronze rows yet, one
`bronze_pending` line each. It also lists, as `failed`, any file or member with an unusable lake
entry, even one already loaded. It reads each file the way a run does (detect, read, add metadata) and
then drops the result, so it costs about as much time and memory as a run, minus the write. It
writes nothing: no raw copy and no bronze file. Archive members are extracted to a temporary folder
outside the lake and removed afterwards.

Use it before a rerun. A reader or config change turns `needs_config` and `no_reader` into `ready`,
and `ready` means a run reads the file and writes it (a later copy with the same bytes is `skipped`).
A file that cannot be read at all is still
listed, as `failed` with its error type and no `file_hash`, and the other files are still checked.

Exit 3 if any item is listed, whatever its reason, since `ready` and `skipped` files are not loaded
yet; exit 0 if the list is empty. A failed file is listed, not failed, so `--pending` does not exit 1
for it. A landing folder that cannot be listed (for example no read permission on the folder) or is
not a directory is a setup error, as it is for a run: one `bronze_error` line and exit 2, with no
`bronze_pending` lines.

### Failures

Failures (exit 1), logged as `bronze_error ... error=<type>`: for example an empty or whitespace-only
file (`ValueError`), a corrupt or password-protected workbook (`ExcelReadError`), invalid JSON
(`JsonReadError`), an unreadable Parquet file (`ParquetReadError`), an archive error or cap
(`ArchiveReadError`), a file that changed during the read (`RuntimeError`), an unusable raw or
bronze entry (`BadLakeEntryError`, above), a file that cannot be
read at all (for example `PermissionError`). A raw or bronze partition folder that cannot be listed
also fails the file, because the loader cannot tell whether it was loaded. A `sheets:` name not in the workbook, or a `header_row`
past the end of a sheet, also fails the file. A failed member fails only that member; members
already written stay.

### Unusable lake entries

Before it relies on an entry already in the lake, bronze checks it. A failed check raises
`BadLakeEntryError`, which fails that file (or that archive member) like any other failure. Nothing
is written beside the entry, and the loader never overwrites, moves or deletes it.

| `zone` | Entries checked | A usable entry is | `<problem>` values |
| --- | --- | --- | --- |
| `raw` | Every `receive_date=*` entry named `<file_hash>` or `<file_hash>.<ext>` (`.tmp` aside) | A regular file, not a symlink, whose SHA-256 is `<file_hash>` | `symlink`, `not_a_regular_file`, `hash_mismatch` |
| `bronze` | Every `ingest_date=*` entry named `<file_hash>*.parquet` | A regular file, not a symlink, with a readable Parquet footer | `symlink`, `not_a_regular_file`, `unreadable_parquet_footer` |

The log line (`bronze_error`, or `bronze_pending reason=failed`) carries `zone`, `file_hash`,
`bad_entries` (comma-separated `<partition>/<entry name>:<problem>`) and `usable_entries`, the number
of other matches that passed. `usable_entries` above 0 means a good copy exists, and the file still
fails until the bad one is dealt with. An entry or folder that cannot be read at all fails the file
with its own error type (for example `PermissionError`); fix the permissions.

**Repair** is a manual step by an operator:

1. Stop runs for the source. Run `--pending` to list the affected files; it writes nothing.
2. Move each entry in `bad_entries` out of `lake/raw/<source>/` or `lake/bronze/<source>/`, to a
   folder outside the lake. Move it rather than delete it, so it can be examined. For a symlink,
   move the link, not its target. Renaming it inside the lake is not enough: `read_bronze` reads
   every `*.parquet` file under the source.
3. Make sure the bytes can be restored. If `usable_entries` is 0:
   - `raw`: the next run copies the landing file again. If the landing file is gone or has changed,
     put the original bytes back into landing first (their SHA-256 must equal `file_hash`).
   - `bronze`: the next run writes the file again from landing, with a new `_ingested_at`. If the
     landing file is gone, copy the raw copy back into landing under its original name (the
     `bronze_receive` log line has the name and hash). For an archive member, restore the archive.
4. Run the source again, then `--pending`. It should no longer list the file.

`--force` does not get past a bad entry; the file fails until the entry is moved.

Exit codes (from `src/ingestion/bronze_cli.py`). When more than one applies, the first one in the
list wins:

| Code | Meaning |
| --- | --- |
| 2 | Setup error, before any file is processed: invalid source name, invalid `--delimiter`, landing folder missing, not a directory or not listable, invalid config (unknown key, bad value, unknown `format`, unreadable YAML) |
| 1 | At least one file failed (a held file does not change this) |
| 3 | No file failed, but some files are held or pending. With `--pending`, any listed item |
| 0 | Every file was loaded or skipped, or the landing folder has no loadable files (a `bronze_no_files` warning is logged) |

### Log events

Logs are `key=value` lines on stderr. String values are JSON-quoted, so a file name with a space
or a newline cannot break the format. Levels: `bronze_error` is ERROR, so one alert covers a failed
file and a setup error; `bronze_unparsed` (a held file), `bronze_stale_temp` and `bronze_no_files`
are WARNING; every other event is INFO.

| Event | Keys | Meaning |
| --- | --- | --- |
| `bronze_receive` | `source`, `file_name`, `file_hash`, `new` (1 if the raw copy is new, 0 if it existed) | A landing file was received into the raw zone |
| `bronze_skip` | `source`, `file_name`, `file_hash`, `duration_s` | Already loaded; not read |
| `bronze_write` | `source`, `file_name`, `file_hash`, `rows_ingested`, `duration_s` | A bronze file was written |
| `bronze_unparsed` | `source`, `file_name`, `file_hash`, `reason` | Held this run (`needs_config`, `no_reader`, `encoding`) |
| `bronze_error` | `source`, `file_name`, `error` (exception type); for `BadLakeEntryError` also `zone`, `file_hash`, `bad_entries`, `usable_entries` | A file failed. A setup error (exit 2) has no `file_name`; it logs `source` and `error` only, the same for a missing, unlistable or non-directory landing folder (a path that is a file logs `FileNotFoundError`). An invalid `--delimiter` logs `error` only |
| `bronze_run` | `source`, `files`, `written`, `skipped`, `pending`, `failed` | Summary of a run. `files` counts landing files; members are in the counts |
| `bronze_no_files` | `source` | The landing folder has no loadable files |
| `bronze_ignored` | landing: `source`, `symlinks`; archive: `source`, `file_name`, `members`, `symlinks` | Counts of skipped entries. Names of skipped entries are not logged. `members` counts every skipped entry in the archive, directories and symlinks included |
| `bronze_sheet` | `file_name`, `sheet_index`, `header_row`, `skipped_rows`, `rows` | One workbook sheet read. The sheet index, never its name. For an archive member the file name is the generated temp name |
| `bronze_stale_temp` | `source`, `count`, `files` | Leftover bronze `.parquet.tmp` files. Reported, never deleted |
| `bronze_pending` | `source`, `file_name`, `reason`, `file_hash` (when it can be computed), `error` (for `failed` only), `member` (archive members only: the 1-based index); `zone`, `bad_entries`, `usable_entries` for `BadLakeEntryError` | One line per item listed by `--pending`. For an archive, `file_name` is the archive name |
| `bronze_pending_run` | `source`, `pending` | Summary of `--pending` |

Archive members are logged as `<archive>!#<index>` (the 1-based index in the archive) with their
hash, so a member name never reaches a log. Bronze logs counts, file names, hashes, durations and
error types. It never logs row contents, cell values, config text, sheet names, member names, or
exception messages (a message can carry a value). A setup error logs the exception type and the
source name only, never its message: the missing-folder message holds the landing path, and a
config message can echo config text.

## 6. Encodings

- **UTF-8 is the default.** A UTF-8 byte order mark is removed for CSV and JSON, so it never ends
  up in the first column name. Stray NUL bytes inside valid UTF-8 load unchanged.
- **Other encodings** come from the config `encoding` key (section 3) and apply to every delimited
  and JSON file of the source. A source with UTF-8 and cp1252 files needs two sources.
- **UTF-16 and UTF-32.** Plain `utf-16` and `utf-32` need a byte order mark. The explicit forms
  (`utf-16-le`, `utf-16-be`, `utf-32-le`, `utf-32-be`) do not, but the byte order is checked from
  the ASCII characters in the first 64 KB, and a file that looks wrong-order is held. A UTF-8 BOM
  read with any codec other than `utf-8-sig` is held too.
- **Why hold rather than load.** A file read with the wrong encoding gives garbage that looks like
  data, and bronze keeps it. A hold shows in the run log and in `--pending`, and nothing is lost
  because the raw copy is kept.
- **What a hold can miss.** Mojibake (UTF-8 read as cp1252, for example) cannot be detected from
  the bytes, so it loads. Some correct UTF-16 files are held on purpose, and some wrong-order files
  load where the evidence is ambiguous. ADR 0002 lists the cases.

## 7. Security and safety

- **The raw zone is never modified.** Copies are written with temp-file-then-link and never
  replaced, and the copy is hashed again. Landing files are never moved or deleted.
- **A name is not proof.** An existing raw or bronze entry is checked before it is relied on
  (section 5, Unusable lake entries).
- **Symlinks inside the landing folder are not followed.** They are skipped and counted
  (`bronze_ignored symlinks=K`). The landing folder itself may be a symlink (for example to a
  mounted share); that is an operator's choice and is followed. Calling `ingest_file` directly on a
  symlink path also follows it.
- **Archive members are untrusted.** A member name is stored only as data in `_file_name`, never
  used to build a path. Each member is streamed to a private temporary directory under a generated
  name, so names such as `../x` or absolute paths cannot escape. The caps in section 4 bound the cost.
- **The source name** must match `[A-Za-z0-9][A-Za-z0-9_-]*`, so it cannot escape the lake.
- **No secrets or row data** in logs or docs. The write path makes no network calls and needs no
  credentials. The Azure staging step (section 10) is the only code that calls Azure. Examples use
  synthetic names and values.

## 8. Assumptions and limits

- **One writer per source at a time.** Two concurrent runs can both pass the skip check and write
  the same file, so its rows appear twice. Each write uses its own temp file and nothing is
  overwritten, and silver dedupes on `_file_hash`, but bronze does not prevent it (ADR 0001). If
  both try to publish the same raw copy, one of them fails that file for that run.
- **Local filesystem with hard links.** The no-replace publish uses `os.link`, so the lake's
  filesystem must support hard links, with no fallback. Azure Blob Storage has no `os.link`; a move
  there needs a conditional create (an upload with `If-None-Match: *`, or `overwrite=False` in the
  Azure SDK). That is not built.
- **A forced reload duplicates that file's rows by design.** Silver dedupes on `_file_hash`.
- **What the entry checks cover.** Only entries for files that are in landing during a run (or
  `--pending`) are checked. The bronze check reads the Parquet footer, not the data pages, so damage
  inside the data is not detected. It does not check that the footer's rows match the raw bytes.
  Partition folders themselves are not checked. Each run hashes the raw copy of every landing file,
  which adds read time.
- **Durability.** The `fsync` calls cover the published file and its folder. A partition folder
  created by the same write is not synced into its own parent. On macOS, `fsync` does not force the
  drive's own cache to disk. The publish step needs a POSIX system (it opens a folder to sync it).
- **Memory.** Every file is collected in memory before it is written, for every format, so a
  file's size is limited by RAM. `--pending` holds each file the same way (section 5). Only archives
  have caps. A QA run measured a 40 MB UTF-8 CSV at a peak of about 270 MB (see
  [`plans/log.md`](plans/log.md), #33).
- **Many small files.** One Parquet file per load and one raw copy per distinct file. Compaction
  would be a separate job that must not alter bronze history.
- **Install.** `uv sync` installs the project, including the `ingestion` package from `src/` (the
  `[build-system]` and wheel package settings in `pyproject.toml`), so the CLI and the snippets run
  with `uv run` and no path setup. Pytest reads `src` through `pythonpath` in `pyproject.toml`.

## 9. Code map and adding a reader

| Path | Role |
| --- | --- |
| `src/ingestion/bronze.py` | Write path: receive, detect, read, metadata, atomic write, skip check, run loop, `read_bronze`. Holds `READERS` and `FileHeld` |
| `src/ingestion/bronze_cli.py` | Command line: flags, `--pending` listing, exit codes |
| `src/ingestion/pending.py` | The outcome of one file without a write (`ready`, held reasons, `failed`, `skipped`), shared with the run |
| `src/ingestion/raw_zone.py` | Raw zone copy (`receive_raw`), the no-replace `publish` helper, the landing folder scan |
| `src/ingestion/lake_entries.py` | Checks on existing raw and bronze entries, `BadLakeEntryError` and its log fields |
| `src/ingestion/detect.py` | `FileFormat`, `detect_format` (config, signature, text rules), `validate_format` |
| `src/ingestion/source_config.py` | `SourceConfig`, `load_source_config`, validation of every config key |
| `src/ingestion/metadata.py` | `hash_file`, the metadata columns (`BRONZE_META_COLUMNS`), `with_metadata` |
| `src/ingestion/parsed.py` | `ParsedPart`, the one result type every reader returns |
| `src/ingestion/csv_reader.py` | Delimited text: UTF-8 scan, other encodings, UTF-16/32 and BOM checks |
| `src/ingestion/excel_reader.py` | Workbooks and ODS through fastexcel (calamine) |
| `src/ingestion/json_reader.py` | JSON arrays and NDJSON |
| `src/ingestion/parquet_reader.py` | Parquet, converted to strings |
| `src/ingestion/archives.py` | Zip and gzip expansion, member safety and caps |
| `src/ingestion/archive_ingest.py` | Each archive member through the normal path; archive logs; `--pending` for members |
| `src/ingestion/azure_stage.py` | Azure Blob staging into the landing folder: settings, listing, name and cap rules, staging exit codes (section 10) |
| `contracts/bronze/_metadata.yaml` | The metadata columns and their meaning |
| `tests/ingestion/` | pytest tests, one file per module group; synthetic fixtures in `tests/fixtures/` |

Run the checks with `uv run ruff check .`, `uv run ruff format --check .` and `uv run pytest`
(see [`TESTING.md`](TESTING.md)).

### How to add a reader

A new format costs one reader function, one registry line and a fixture:

1. **Format and detection.** Add a member to `FileFormat` in `src/ingestion/detect.py` if there is
   none (config `format:` accepts it automatically). Add a binary signature in `_from_signature`, or
   a text rule in `_from_text`. Only the first 64 KB are available.
2. **Reader.** In a new module (for example `src/ingestion/<name>_reader.py`) write
   `def read_<name>(path: Path, config: SourceConfig, flag: str | None) -> list[ParsedPart]`. Return
   one `ParsedPart(sheet_name, lazy_frame, first_row_number)` per unit. Ignore `flag` unless the
   format is delimited text.
3. **Register.** Add one line to `READERS` in `src/ingestion/bronze.py`. Until then the format is
   reported as `no_reader`. Only a container format (like zip) also joins `ARCHIVE_FORMATS` in
   `archive_ingest.py`, with its expansion in `archives.py`.
4. **Config keys, only if needed.** Add the field to `SourceConfig`, validate it, add the name to
   `_KNOWN_KEYS`, and document it in section 3.
5. **Fixture, tests and docs.** A small synthetic file under `tests/fixtures/`, tests under
   `tests/ingestion/`, and a row in the table in section 4. A decision change needs a new ADR or an
   amendment approved by Michael.

Rules a reader must follow:

- Every column is `pl.String` and holds the source text. No casting, rounding, trimming or value
  rewriting. A rename is allowed only when the format needs one, and it is documented (Excel's
  `unnamed_<n>` is the one example).
- Reject a source column whose name starts with `_`. `with_metadata` does not check for it, so
  the source value would be silently replaced by the metadata column.
- Raise a `ValueError` subclass whose message names the error type and the file name only. Never
  put keys, values, cell text, record text or library messages in it.
- Raise outside any `except` block, so that `__context__` and `__cause__` are `None`. A library
  exception can hold file content. `json_reader.py` and `archives.py` show the pattern: catch,
  keep only the type name, then raise after the block.
- Let `UnicodeError` through: bronze turns it into an `encoding` hold. Log nothing from content;
  counts and indices are fine. Header-only input is a zero-row part; empty input must raise.

## 10. Reading test files from Azure Blob Storage

Test files can sit in a private Azure Blob Storage container instead of the repo. The staging step
`ingestion.azure_stage` downloads the blobs under one container prefix into `data/landing/<source>/`.
Bronze then runs on that folder unchanged, with the same raw zone, skip check, exit codes and logs
as above. The raw zone and bronze stay on the local disk, and nothing is written to Blob. The
reasons are in [ADR 0003](decisions/0003-azure-blob-source.md).

```bash
uv run python -m ingestion.azure_stage <source> <prefix>   # download blobs into data/landing/<source>/
uv run python -m ingestion.bronze <source>                 # then load them as any landing file
```

The prefix is the container folder for the source: `orders/` for the `orders` source. Blob
`orders/a.csv` is staged as `data/landing/orders/a.csv`. Staging creates the folder when it
downloads a blob.

Settings come only from the environment: `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_CONTAINER`.
Sign in with `az login` first. The code uses only that session (`AzureCliCredential`), and your user
needs the `Storage Blob Data Reader` role on the container ([SECURITY](SECURITY.md)).

| Flag | Default | What it does |
| --- | --- | --- |
| `--max-blob-mib` | 256 | Largest blob, in MiB. Checked on the listed size before download and on the bytes received |
| `--max-blobs` | 100 | Most blobs staged per run. Blobs past this count are not staged, and that counts as one extra failure |
| `--max-total-mib` | 2048 | Most bytes received in one run (2 GiB), staged and identical-skipped alike |

Each flag takes a whole number of at least 1. Any other value exits 2.

- **One prefix, not recursive.** Only blobs directly under the prefix are listed. Sub-prefixes
  (folders) and folder markers (zero-byte names ending in `/`) are ignored and counted in
  `ignored`. The name of each skipped entry is logged (`azure_stage_ignored`).
- **Never overwrites.** A local file with the same name and the same bytes is skipped. Different
  bytes fail that blob as a name collision (`NameCollisionError`), and nothing is replaced. To load
  a changed blob, give it a new name, or remove the old landing file yourself.
- **Names are untrusted.** A blob is rejected if its name after the prefix is empty, `.` or `..`,
  contains `/`, `\` or `..`, has a control character, or ends in `.tmp` (in any case). The prefix
  itself must not be empty and must not contain `..`, a backslash, a control character or start
  with `/`. Otherwise the run exits 2.
- **Failures do not stop the run.** A rejected or failed blob is logged as `azure_stage_error` with
  its exception type, counted, and the other blobs continue.
- **Size caps.** A blob over `--max-blob-mib` fails before download. A blob that streams past the
  cap fails too, and nothing is written for it.
- **Blob count cap.** Blobs past `--max-blobs` are not staged. The run counts one extra failure
  (`error=TooManyBlobs`).
- **Total cap.** Before a blob that would take the run past `--max-total-mib`, staging stops. That
  blob and the later ones are not downloaded, `azure_stage_error error=TotalSizeCap` is logged, and
  one extra failure is counted. Blobs already staged stay. If a blob's bytes pass the cap while it
  streams, that blob fails with `TotalSizeCapError`, and the same stop and extra failure follow.
- **A listing error stages nothing.** The listing is read before the first download, so a failed
  listing downloads nothing.
- **Interrupted runs.** A run that stops early can leave a hidden temp file
  (`.<16 hex><ext>.tmp`) in the landing folder. Bronze skips it because it is hidden, staging does
  not reuse it, and neither removes it.
- **Leftovers from earlier staging code.** It used a visible `<name>.tmp` temp file, which bronze
  reads like any other landing file. Check each staged landing folder once for such files and
  remove them by hand.
- **Read only.** The code makes only list and download calls. It never writes, deletes, renames or
  re-tiers a blob.

Exit codes for the staging step. It has no code 3: a staged file is not "held" until bronze reads it.

| Code | Meaning |
| --- | --- |
| 2 | Setup, sign-in or access error before any download: invalid source or prefix, a missing or malformed `AZURE_STORAGE_ACCOUNT` or `AZURE_STORAGE_CONTAINER`, a bad flag, no `az login` session, or no read access. Logged as `azure_stage_error` with the exception type |
| 1 | At least one blob failed (rejected, name collision, over a cap, or a download error, including a sign-in error during a download), or a blob count or total cap was hit. Blobs already staged stay |
| 0 | Every blob was staged or skipped, or the prefix had nothing to stage |

Bronze has its own exit codes (section 5). Exit 0 from staging does not mean bronze has loaded
everything; `ingestion.bronze <source> --pending` shows what is still held.

Logs are `event=<name>` followed by `key=value` pairs, with string values JSON-quoted, as in bronze.

| Event | Level | Keys | Meaning |
| --- | --- | --- | --- |
| `azure_stage_write` | INFO | `source`, `blob_name`, `size`, `file_hash`, `duration_s` | A blob was staged. `size` is the bytes received |
| `azure_stage_skip` | INFO | `source`, `blob_name`, `size`, `file_hash`, `duration_s` | An identical local file was already there. Nothing was written |
| `azure_stage_ignored` | INFO | `file_name` | A folder marker or sub-prefix was skipped |
| `azure_stage_error` | ERROR | `source`, `blob_name`, `error` for a blob; `error` and `source` for setup and listing errors; `detail` (the setting name) for a missing or malformed setting; `max_blobs` or `received` with the two caps | A blob failed, a cap was hit, or setup failed. A setting is named in the log, never its value |
| `azure_stage_run` | INFO | `source`, `staged`, `skipped`, `ignored`, `failed` | Summary of the run |

The logs never contain tokens, URLs or query strings, connection strings, the account or container
values, or blob content. Blob names are logged. Library error messages are never logged, only their
type, and the `azure` library logger is held at WARNING so request URLs stay out of the output.

**Not built:** writing the raw zone or bronze to Blob, event triggers, Key Vault, scheduled staging,
recursive listing and single-blob staging. See [ADR 0003, Not built](decisions/0003-azure-blob-source.md#not-built).

## 11. How it was built

Alfred (the head agent) wrote the [plan](plans/2026-11-12-bronze-multi-format.md) and the ADRs,
and Michael approved them. Each task then got an issue and went through the `builder` agent (code
on its own branch and PR), the `code-reviewer` agent (comments only, never edits) and the `qa`
agent (tests only). Michael merged every PR; no agent pushes to `main` or merges. Every dispatch
and result is in [`plans/log.md`](plans/log.md).

Review changed the design several times. Some examples:

- **Atomic writes and unique names.** The first write was not atomic, and two loads in the same
  second could collide. Files now get a random suffix and are written to a temp file and linked
  into place.
- **Header-only files** were first skipped without a record, so they were re-read every run.
  They are now written as zero-row Parquet files.
- **Errors without context.** Review of the JSON, NDJSON and Parquet readers (PR #55) found that
  `from None` still kept the library error as `__context__`, which can hold the whole file. Errors
  are now raised after the `except` block, and a lone surrogate fails the file instead of holding
  it.
- **UTF-16 byte order.** The first fix let a tie between the two byte orders load a wrong-order
  file. Review of PR #67 found it, and PR #70 made a tie hold. Section 6 and ADR 0002 have the cases.
- **Config and YAML errors** are logged by exception type only, and tests check that no config
  text reaches the output.
- **Late CLI reviews** (PR #77, and the hardening in PR #81). `--pending` was first a separate
  check; it now runs the real read path, so it cannot disagree with a run. Packaging lets `uv sync`
  install the project, so the old path workaround is gone. Failures log at ERROR, held files at
  WARNING, and setup errors log the type and source only.

Details of each change are in [`plans/log.md`](plans/log.md).

## 12. What comes next

- **Silver** is not built. It will read bronze, cast each column to the type in
  `contracts/silver/<table>.yaml`, quarantine rows that fail, dedupe on `_file_hash`, and carry
  `_file_name` and `_file_hash` through for lineage. `contracts/silver/` has no contracts yet.
- **Fixed-width** is the planned next reader, configured with column positions. It needs a spec
  from the customer; until a reader exists, fixed-width files are held as `no_reader`.
