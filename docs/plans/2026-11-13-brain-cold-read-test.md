# Brain cold-read test

Date: 2026-11-13. Part of the [Obsidian brain plan](2026-11-12-obsidian-brain.md), task 14.

The brain must let a model understand this project from the notes alone, with no repo access.
This test checks that. Re-run it after a large `/sync-brain`, and after any note rewrite.

## How to run it

1. Give a model that is not Claude access to the vault only: no repo, no chat history.
2. Do not show it the answer key below.
3. Paste this prompt, then the ten questions:

   > You are reading a knowledge base of Markdown notes. Answer each question using only those
   > notes. Name the note you got each answer from. If the notes do not say, write "not in the
   > notes". Do not guess and do not use outside knowledge about this project.

4. Score each answer 1 (all key points), 0.5 (about half) or 0. A pass is 9 or more out of 10.
5. For every miss, find the note that should have carried the fact and fix it through
   `brain-writer`. A wrong answer that repeats an old claim means a stale note.

When the code changes, update the key to match the code first.

## Questions

1. What is ai-data-lab, and why does it exist?
2. Which layers are built today, which are not, and what is the next piece of work? Is Dagster
   used in any code?
3. Name the metadata columns that bronze adds to every row, and say what the last one holds.
4. The same CSV, byte for byte, is dropped into the landing folder for a source and the loader
   runs twice. What happens each time? What does `--force` change?
5. A `.txt` file arrives with no config. What does the loader do with it, what exit code
   results, and how do you fix it?
6. Explain the difference between a held file and a failed file, list all four exit codes, and
   say which wins when several apply.
7. A zip with 150 entries arrives. What happens, and what limits and safety rules apply to
   archives?
8. List the agents, what each is allowed to edit, which models they run on, and who merges.
9. Why are bronze files named by the content hash plus run time plus a random suffix, and how
   are they written safely?
10. Name at least four known limits or things to be careful about when using bronze today.

## Answer key

1. An AI-assisted medallion data platform (bronze, silver, gold) with lineage a model can read.
   A public learning and portfolio project that tests what AI agents do well and badly on real
   data engineering.
2. Only bronze (and the raw zone) is built. Silver is next, then gold, built from silver only.
   No silver contracts yet and no dbt models. Dagster is declared as a dependency but no code
   uses it.
3. `_file_name`, `_ingested_at`, `_file_hash`, `_row_number`, `_sheet_name`. The last is the
   sheet name for workbooks and null otherwise. Five, not four.
4. First run: copied to the raw zone, read, and written as a new Parquet file. Second run:
   skipped because a bronze file name already starts with the hash; nothing written, exit 0.
   `--force` writes another file and duplicates the rows on purpose; silver would dedupe on
   `_file_hash`.
5. Held as `needs_config`. The raw copy is kept, no bronze rows are written, and other files
   still load. Exit 3. Fix with `--delimiter` or a config delimiter and rerun; `--pending`
   shows the reason.
6. Held: cannot be read yet because something is missing (exit 3). Failed: reading raised an
   error (exit 1); the raw copy is kept and the batch continues. Codes: 2 setup error, 1 any
   failure, 3 held or pending with no failure, 0 done. Order: 2, then 1, then 3, then 0.
7. The archive fails as a whole (exit 1), because the 100-entry cap counts every entry,
   including directories. Other caps: 512 MiB per archive and 256 MiB per member, measured on
   decompressed bytes. Names are never used to build paths, encrypted members fail the archive,
   nested archives are held.
8. Alfred (head agent: plans, dispatches one at a time, logs); builder (the only one that
   changes code); code-reviewer (comments only); QA (tests only); docs-writer (README and
   docs); brain-writer (vault notes). Opus plans, Sonnet builds and reviews, Haiku writes docs.
   No API key. The human owner merges every pull request.
9. The hash makes "was this loaded?" a file-name lookup. Run time and a random suffix keep names
   unique (two forced loads in one second once collided). Written to a temp file, then
   published with a hard link that fails if the name exists, so a crash never leaves a partial
   file. Plain Parquet moves to cloud storage without a rewrite.
10. Any four: whole files are read into memory with no size cap; no locking, so one load per
    source at a time; silver and gold not built; an archive whose members are all held stays
    pending every run; some wrong encodings cannot be detected and load silently; the raw zone
    roughly doubles storage; nothing runs on Azure yet.
