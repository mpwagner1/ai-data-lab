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
