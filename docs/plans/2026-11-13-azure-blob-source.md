# Plan: read test files from Azure Blob Storage into bronze

Date: 2026-11-13. Status: draft, waiting for the owner's approval.

## Goal

Store test files in an Azure Blob Storage container and let the project read them and ingest
them through the existing bronze flow. The first version only reads from Blob. The raw zone and
bronze stay on the local disk.

## Decisions already made (owner, 2026-11-13)

- **Account:** the owner's personal storage account, used only for this lab.
- **Auth:** the owner's own `az login` session through `DefaultAzureCredential`, with the
  `Storage Blob Data Reader` role on the single test container. Short-lived tokens, no secret in
  any file, no shared admin credential (`docs/SECURITY.md`).
- **Scope:** read from Blob only. Writing the raw zone or bronze to Blob is a later project that
  needs its own ADR (ADR 0001 explains why the hard-link publish cannot work on Blob).

## Design (recommended)

A new, separate staging step downloads blobs into the local landing folder. The existing bronze
code then runs unchanged on `data/landing/<source>/`. This keeps every guarantee bronze has today
(raw zone, hash skip, atomic writes, exit codes) and touches none of it.

```
Blob container/<prefix>  --stage-->  data/landing/<source>/  --bronze-->  lake/raw, lake/bronze
```

- New module `src/ingestion/azure_stage.py`, run as `uv run python -m ingestion.azure_stage
  <source> <prefix>`. It lists the blobs directly under the prefix, downloads each into the
  landing folder, and exits. It never writes, deletes or renames anything in Blob.
- Settings come from environment variables only: `AZURE_STORAGE_ACCOUNT` and
  `AZURE_STORAGE_CONTAINER`. `.env.example` holds placeholders. No account, container or
  subscription name is written in the repo, docs, logs or the Obsidian notes.
- Blob names are untrusted data, like archive member names. Only blobs directly under the prefix
  (no recursion). A name is rejected if it has a path separator beyond the prefix, `..`, a
  control character, or is empty. Folder marker blobs (zero bytes with a trailing slash, which
  accounts with a hierarchical namespace create) are skipped and counted.
- Never overwrite a local file. If a local file of the same name exists with different bytes, that
  blob fails with a name-collision error and nothing is replaced. If the bytes are identical, it
  is skipped.
- Caps: 256 MiB per blob and 100 blobs per run by default, both checked before download from the
  blob properties and again on the bytes received. Configurable by flags.
- Logs are `key=value` and carry counts, blob names, sizes and hashes only. Never tokens, URLs
  with query strings, connection strings, account keys or blob content.
- Exit codes follow bronze: 2 setup or auth error (missing setting, no login, no access), 1 at
  least one blob failed, 0 done or nothing to stage.
- Two new dependencies through `uv add`: `azure-storage-blob` and `azure-identity`.

## Tasks

| # | Task | Agent | Model | Acceptance |
| --- | --- | --- | --- | --- |
| 0 | Owner sets up the account side: create a container for test files, give the owner's user the `Storage Blob Data Reader` role on that container only, run `az login`, and put the two environment variables in a git-ignored `.env`. Agents never change roles or create Azure resources. | Owner | n/a | `az storage blob list` works against the test container with `--auth-mode login`, and a write attempt is refused. |
| 1 | Write ADR 0003 (Azure Blob as a source, staging approach, auth, what is not built) and ask the owner to approve it. | Alfred | Opus | ADR committed on a branch; owner approves before task 2. |
| 2 | Implement `azure_stage.py`, its CLI, `.env.example` and the dependencies. | builder | Sonnet | Behaviour in the Design section; ruff, format and pytest pass; no network in tests. |
| 3 | Tests with a fake blob client: listing and prefix rules, rejected names, folder markers, caps, collision and identical-skip, exit codes, and that no log line contains a token, URL query or blob content. | qa | Sonnet | New tests fail when the code is broken (mutation check); all checks pass. |
| 4 | Review the diff, with a security focus (path safety, secret handling, read-only use). | code-reviewer | Sonnet | Comments only; blocking findings fixed before merge. |
| 5 | Update docs: a short Azure section in `docs/BRONZE.md`, a line in `docs/SECURITY.md` about the read-only role, README quick start. Placeholders only. | docs-writer | Haiku | Docs match the code; no real names. |
| 6 | Smoke test with synthetic files: the owner uploads a few made-up test files to the container; Alfred runs stage, then bronze, then `--pending`, using the owner's session after the owner says go. | Alfred + owner | Opus | Files land in bronze with correct metadata; a second run skips them; a failed login exits 2 with no secret printed. |
| 7 | `/sync-brain` so the vault notes describe the new source; Azure area note updated (it currently says there is no Azure code). | brain-writer | Sonnet | Audit clean; no account, container or subscription names in any note. |

Each code task: one issue, one branch `agent/<issue>-<slug>`, one PR; the owner merges.

## Risks and rules

- **Real data stays out.** Only synthetic or made-up files go in the test container. No employer,
  client or business data, ever.
- **Blast radius.** The role is read-only and scoped to one container, so the code cannot change
  or delete anything in the storage account even by mistake.
- **Public repo.** No names, endpoints, tokens or connection strings in code, tests, fixtures,
  docs, plans, logs or the brain. Tests use fake names.
- **Hard stops (AGENTS.md).** No agent deploys, applies migrations, changes roles or deletes
  files. Blob is read, never written, in this version.
- **Out of scope:** writing raw or bronze to Blob, event triggers, Key Vault, Databricks, silver.

## Open questions

1. Container name and prefix layout: one prefix per source (for example `orders/`), matching the
   landing folder name? (Recommended.)
2. Should the stage step also accept a single blob name, or prefix only? (Prefix only keeps it
   small.)
