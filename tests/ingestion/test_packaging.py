"""The CLI must run from the repo root with no PYTHONPATH (the project is installed by uv sync)."""

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, *args], cwd=REPO_ROOT, env=env, capture_output=True, text=True, check=False
    )


def test_module_help_runs_without_pythonpath() -> None:
    result = _run("-m", "ingestion.bronze", "--help")
    assert result.returncode == 0, result.stderr
    assert "usage" in result.stdout


def test_package_imports_without_pythonpath() -> None:
    assert _run("-c", "import ingestion").returncode == 0
