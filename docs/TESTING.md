# Testing

## Run

- All tests: `uv run pytest`
- One file: `uv run pytest tests/<package>/test_<module>.py`
- One test: `uv run pytest tests/<package>/test_<module>.py -k <name>`
- Config lives in `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["src"]`).

## Layout

- pytest only.
- One test file per module, mirroring the source tree:
  `src/ingestion/bronze.py` → `tests/ingestion/test_bronze.py`.
- Fixture files live in `tests/fixtures/`. Keep them small: a few rows, just enough to
  exercise the behavior. Name them for the case (`orders_with_bad_date.csv`).
- Fixtures use synthetic data only. Never real PII or production extracts.
- Shared fixtures go in `tests/conftest.py`; use `tmp_path` for anything written.

## What to test

- Every module gets tests, and every bug fix gets a regression test.
- Bronze: the five metadata columns exist, values are never cast, reruns append.
- Silver: output schema equals its contract in `contracts/silver`; bad rows are
  quarantined with a reason.
- Gold: dbt tests in the schema YAML (`unique`, `not_null`, `relationships`).
- Cover edge cases: empty file, missing column, duplicate rows, nulls.

## Conventions

- Test names state behavior: `test_adds_file_hash_to_every_row`.
- Arrange, act, assert. One behavior per test. Use `pytest.mark.parametrize` for cases.
- Compare Polars frames with `polars.testing.assert_frame_equal`.
- No network, no real cloud services, no sleeps. Keep the suite fast.
- Tests have type hints like any other code.
- Never log or print row contents on failure beyond what pytest shows.
