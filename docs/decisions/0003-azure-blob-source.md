# 0003: Azure Blob Storage as a read-only source for test files

Date: 2026-11-15
Status: Accepted (approved by the owner 2026-11-17). Plan: `docs/plans/2026-11-13-azure-blob-source.md`.

## Context

Test files should live in a cloud container so they can be shared and replaced without touching the
repository, and the project should be able to read them and ingest them. Bronze today reads only the
local landing folder `data/landing/<source>/`, and it is built on local-filesystem guarantees: a
no-replace publish with `os.link`, a hash-prefix skip check and a raw zone (ADR 0001, ADR 0002). Azure
Blob Storage has no hard links, so moving the lake itself to Blob is a separate and larger change. This
decision only covers getting files in.

## Decision

- **Stage, then ingest.** A new step downloads blobs from one container prefix into
  `data/landing/<source>/`. The existing bronze code then runs unchanged on that folder, so every
  bronze guarantee (raw zone, hash skip, atomic writes, held versus failed, exit codes) stays as it is.
  Command: `uv run python -m ingestion.azure_stage <source> <prefix>`.
- **One prefix per source, prefix only.** The prefix names the source, for example `orders/` for the
  `orders` source. Only blobs directly under the prefix are listed (no recursion). No single-blob mode
  in version 1.
- **Read-only.** The code uses only list and download calls. It never writes, deletes, renames or
  re-tiers a blob. The owner grants the `Storage Blob Data Reader` role on the single test container, so
  the platform refuses writes even if the code were wrong.
- **Authentication.** `DefaultAzureCredential`, which uses the owner's `az login` session. No account
  key, connection string, SAS token or client secret is used, stored or logged. Tokens are short-lived.
- **Settings.** `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_CONTAINER` come from environment variables
  only. The owner keeps the values in 1Password and runs the command with `op run --env-file=.env`
  (`docs/SECURITY.md`). `.env.example` has placeholder references only. The code never calls 1Password.
  No account, container, subscription or 1Password name appears in the repo, logs or notes.
- **Blob names are untrusted data.** A blob is rejected if its name has a path separator beyond the
  prefix, `..`, a control character, or is empty. Zero-byte folder markers (created by accounts with a
  hierarchical namespace) are skipped and counted. The local file name is the part after the prefix and
  is never used to build any other path.
- **Never overwrite a local file.** If a file with that name already exists with different bytes, that
  blob fails with a name-collision error and nothing is replaced. If the bytes are identical, it is
  skipped. A changed blob therefore needs a new name or a cleared landing file, which keeps the raw zone
  and the hash-based history honest.
- **Caps.** 256 MiB per blob and 100 blobs per run by default, checked against the blob properties
  before download and again on the bytes actually received. Flags can change them.
- **Logs and exit codes.** `key=value` logs with counts, blob names, sizes and hashes only: never tokens,
  URLs with query strings, connection strings, account keys or content. Exit 2 for a setup or
  authentication error (missing setting, not signed in, no access), 1 if at least one blob failed, 0 when
  done or nothing to stage. These match the bronze exit codes.
- **Dependencies.** `azure-storage-blob` and `azure-identity`, added with `uv add` and committed in
  `uv.lock`.
- **Tests use a fake client.** No test touches the network or a real account (`docs/TESTING.md`). The
  real container is used only for a manual smoke test with synthetic files.

## Consequences

- Bronze, the raw zone and the lake stay local, and nothing about their behavior changes.
- A copy of each test file exists in the container, in the landing folder and in the raw zone. That is
  extra storage, and it keeps ingestion reproducible when the container changes.
- Ingestion needs two commands, stage then bronze. A later wrapper can join them.
- A blob whose content changes under the same name is refused locally, not silently re-ingested.
- The owner must keep the role read-only and the container free of real data. Only synthetic test files
  go there. Nothing real from any employer or client is ever stored or read.

## Not built

- Writing the raw zone or bronze Parquet to Blob. That needs a no-replace upload to replace the hard link
  (`If-None-Match: *`, for example `overwrite=False` in the Azure SDK), a rewrite of the write path and its
  own ADR. ADR 0001 describes the open points.
- Event triggers, Key Vault, scheduled staging, recursive listing and single-blob staging.

## Alternatives considered

- **Read blobs directly inside bronze (for example DuckDB or fsspec reading `az://` paths).** Fewer
  copies, but it bypasses the landing folder and the raw zone that bronze's guarantees rest on, and it
  would force a second read path through every reader.
- **Move the raw zone and bronze to Blob now.** The real goal later, but it replaces the hard-link publish,
  the skip check and the single-writer assumption at once. Too large for a way to get test files in.
- **A SAS token in an environment variable.** Simple, but it is a long-lived secret to rotate, and a leaked
  link grants access. `az login` gives short-lived tokens and no secret.
- **A service principal with a client secret.** A dedicated identity is better in production, but it adds a
  long-lived secret and setup that a learning lab does not need.
- **A connection string or account key.** Full account access in one value. Rejected.
- **Download with the `az` or `azcopy` command line.** Works, but it adds an untested shell step, makes
  name safety and caps harder to enforce, and cannot be unit tested with a fake client.
