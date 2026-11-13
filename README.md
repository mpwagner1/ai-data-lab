# ai-data-lab

A learning and portfolio repo for building an **AI-assisted medallion data platform**
(bronze → silver → gold) with end-to-end lineage and AI agents.

## Goal

Explore how AI agents can help design, build, test and document a modern data platform:

- **Bronze**: raw ingestion, landed as-is, governed by data contracts.
- **Silver**: cleaned, typed, conformed data.
- **Gold**: business-ready models.
- **Lineage**: traceable from source to gold.
- **Agents**: AI assistants that work within the repo's contracts, plans and decisions.

## Stack

Python 3.12 · [uv](https://docs.astral.sh/uv/) · Polars · DuckDB · dbt-core (+ dbt-duckdb) ·
Dagster · pytest · ruff · sqlfluff · pre-commit

## Layout

```
src/ingestion/       ingestion code
contracts/bronze/    bronze data contracts
contracts/silver/    silver data contracts
docs/decisions/      architecture decision records
docs/plans/          plans written before large changes
tests/               pytest tests
```

## Quickstart

```bash
uv sync
uv run pre-commit install
uv run pytest
```

## Conventions

- Work on a branch and open a PR; never push to `main`.
- Never commit secrets (`.env` is git-ignored) or row-level data.

## Bronze ingestion

Put files in `data/landing/<source>/` (CSV, TSV, pipe, TXT, Excel, ODS, JSON, NDJSON, Parquet,
gzip or zip), then, from the project root:

```bash
PYTHONPATH=src uv run python -m ingestion.bronze <source>              # load new files, skip loaded ones (by hash)
PYTHONPATH=src uv run python -m ingestion.bronze <source> --pending    # list files not loaded yet, and why
PYTHONPATH=src uv run python -m ingestion.bronze <source> --force      # reload; adds a new file, keeps the old
PYTHONPATH=src uv run python -m ingestion.bronze <source> --delimiter '|'  # delimiter for .txt and .dat files
```

Exit codes: 0 ok, 1 a file failed, 2 bad source, folder or config, 3 files held (see `--pending`).
Each loaded file is written as `lake/bronze/<source>/ingest_date=YYYY-MM-DD/<file_hash><run_ts>_<8 hex>.parquet`.
Read it back with DuckDB (missing columns come back null):

```python
from pathlib import Path
from ingestion.bronze import read_bronze

read_bronze("<source>", Path("lake")).show()
```

Source config (`config/sources/<source>.yaml`), supported formats, held files, log events and how
to add a reader: [docs/BRONZE.md](docs/BRONZE.md).
Decision records: [0001 storage](docs/decisions/0001-bronze-parquet-storage.md) and
[0002 formats](docs/decisions/0002-bronze-multi-format.md).
