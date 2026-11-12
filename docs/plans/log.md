# Dispatch log

| Date | Issue | Agent | Model | Result |
|------|-------|-------|-------|--------|
| 2026-11-11 | #10 | builder | Sonnet | PR #11 opened. Builder reported ruff check, ruff format and pytest (16) passed; sqlfluff not run (no SQL). Awaiting code-reviewer. |
| 2026-11-11 | #10 | code-reviewer | Sonnet | PR #11 reviewed: no blockers, 4 low findings (comment on PR). |
| 2026-11-11 | #10 | builder (resumed) | Sonnet | Fixed findings 1, 2, 4 on PR #11 (ruff, format, pytest 18 passed). PR #11 merged by Michael. |
| 2026-11-11 | #13 | builder | Sonnet | PR #14 opened: write path, hash-skip, --force, CLI (pytest 33 passed). |
| 2026-11-11 | #13 | code-reviewer | Sonnet | PR #14 reviewed: 2 bugs (non-atomic write, same-second --force collision) and 5 smaller findings. |
| 2026-11-11 | #13 | builder (resumed) | Sonnet | Atomic write, unique names, resilient CLI, quoted logs, missing-folder handling (pytest 40 passed). |
| 2026-11-11 | #13 | qa | Sonnet | Added CLI --force, pre-load change and CSV edge-case tests (pytest 46 passed); commit touches tests/ only. The agent's hand-back text was a placeholder; results were taken from its PR comment and the commit. No code bugs found. |
| 2026-11-11 | #13 | code-reviewer | Sonnet | PR #14 re-reviewed: no blockers; findings 1-3, 5-7 resolved, 4 deferred by design; 5 minor points and 3 test gaps (PR comment). |
| 2026-11-11 | #13 | - | - | PR #14 (write path, QA tests) and PR #15 (ADR and plan update) merged by Michael. |
| 2026-11-11 | #16 | builder | Sonnet | PR #17 opened: CLI invalid-source exit 2, stale temp-file warning, numeric duration_s, three test gaps closed (pytest 51 passed). |
| 2026-11-11 | #16 | code-reviewer | Sonnet | PR #17 reviewed: no blockers; 2 non-blocking notes (stale-temp warning cannot tell a dead run from a live writer, covered by the single-writer assumption; weak "Traceback" assertion). |
| 2026-11-11 | #19 | builder | Sonnet | PR #20 opened: header-only CSVs are written as zero-row Parquet files so they are skipped on later runs (pytest 54 passed). Awaiting review. |
| 2026-11-11 | #19 | code-reviewer | Sonnet | PR #20 reviewed: no blockers; 2 minor test gaps (differing-columns header-only read, metadata dtype assertions). |
| 2026-11-12 | #26 | Alfred | Opus | Wrote plan 2026-11-12-bronze-multi-format.md and ADR 0002. Michael approved the ADR with `_sheet_name` and `--delimiter` additions; docs tasks routed to docs-writer. PR opened. |
| 2026-11-12 | #28 | builder | Sonnet | Dispatched: bronze multi-format task 1 (reader registry, SourceConfig, delimited reader). Result pending. |
| 2026-11-12 | #28 | builder | Sonnet | PR #36 opened and merged by Michael: reader registry, SourceConfig, delimited reader, --delimiter flag (pytest 70 passed, existing tests unchanged; adds pyyaml and types-pyyaml). No code-reviewer pass before merge. |
| 2026-11-12 | #28 | code-reviewer | Sonnet | Merged PR #36 reviewed after the fact: no blockers; 3 findings to fix before task 2 (config errors escape main, delimiter must be one byte, test-driven reader-call workaround) and smaller items (comment on PR #36). |
| 2026-11-12 | #38 | builder | Sonnet | Dispatched: multi-format task 1b (config and delimiter validation, reader call cleanup). Result pending. |
| 2026-11-12 | #38 | builder | Sonnet | PR #40 opened: config errors exit 2 without config text, single-byte delimiter and text-codec validation, reader call cleanup (pytest 92 passed). |
| 2026-11-12 | #38 | code-reviewer | Sonnet | PR #40 reviewed: no blockers; 3 minor items (private _is_text_encoding fails open, unknown-key error message names config keys, small test gaps). |
| 2026-11-12 | #29 | builder | Sonnet | Dispatched: multi-format task 2 (raw zone, detection, pending, exit code 3), plus config.format dispatch and two #40 review fixes (validate_encoding by trial encode/decode, no config key names in the unknown-key error). Result pending. |
| 2026-11-12 | #29 | builder | Sonnet | PR #42 opened: raw zone, signature detection, derived pending, exit code 3, --pending, config.format dispatch, validate_encoding fix (pytest 130 passed). |
| 2026-11-12 | #29 | code-reviewer | Sonnet | PR #42 reviewed: 4 findings (bronze.py 511 lines over the 500 limit; symlinks in landing escape the folder; UTF-16 .csv held as no_reader even with config; bad config format exits 1 normally but 2 under --pending, raw copy happens before validation). Fixes pending. |
| 2026-11-12 | #29 | builder (resumed) | Sonnet | Fixed the four PR #42 findings: symlinks skipped and counted, raw-zone code moved to raw_zone.py (bronze.py 450 lines), UTF-16 via config encoding, bad config format validated before raw copy (pytest 142 passed). |
| 2026-11-12 | #29 | code-reviewer | Sonnet | PR #42 second review: findings 1, 2 and 4 resolved, 3 mostly; new: a wrong config encoding can load a garbage zero-row file marked ingested, and the NUL rule is inconsistent by offset. PRs #41, #42 and #43 merged by Michael before those two were fixed; tracked in #44. |
| 2026-11-12 | #44 | builder | Sonnet | Dispatched: multi-format task 2b (wrong config encoding holds instead of loading garbage, narrowed NUL rule, symlink test gaps). Result pending. |
| 2026-11-12 | #44 | builder | Sonnet | PR #46 opened: wrong config encoding holds as `encoding` (BOM rule, decoded-header check), narrowed NUL rule, symlink test gaps (pytest 156 passed). |
| 2026-11-12 | #44 | code-reviewer | Sonnet | PR #46 reviewed: fix first. UTF-8 data with an explicit-endian config (utf-16-be/le) still loads a garbage zero-row file marked ingested; header-only wrong-endian without newline loads; low items (UTF-8 BOM with latin-1 config, Unicode 15 limit, test strength). |
| 2026-11-12 | #44 | builder (resumed) | Sonnet | Dispatched on PR #46 for review findings 1, 2, 3, 4, 5. Result pending. |
| 2026-11-12 | #44 | builder (resumed) | Sonnet | PR #46 fixes: no-NUL rule and low-order-byte wrong-endian check for UTF-16/32 configs, UTF-8 BOM held with non-utf-8-sig codecs, docstring and test fixes (pytest 226 passed). |
| 2026-11-12 | #44 | code-reviewer | Sonnet | PR #46 third review: merge. ~200 cases executed, no wrong-encoding file loads silently. One medium: `_wrong_byte_order` falsely holds some correct UTF-16 CJK files whose characters have a zero low byte (visible exit 3, not silent). |
