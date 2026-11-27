"""Guards for the GitHub Actions workflows that linters do not enforce."""

import re
from pathlib import Path
from typing import Any

import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
PINNED_USES = re.compile(r"uses:\s+\S+@[0-9a-f]{40}\s+# v\d+\.\d+\.\d+$")


def _workflow_files() -> list[Path]:
    return sorted([*WORKFLOWS.glob("*.yml"), *WORKFLOWS.glob("*.yaml")])


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_every_action_is_pinned_to_a_sha_with_a_version_comment() -> None:
    unpinned = [
        f"{path.name}: {line.strip()}"
        for path in _workflow_files()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip().removeprefix("- ").startswith("uses:") and not PINNED_USES.search(line)
    ]
    assert unpinned == []


def test_every_workflow_defaults_to_read_only_contents() -> None:
    for path in _workflow_files():
        assert _load(path).get("permissions") == {"contents": "read"}, path.name


def test_every_checkout_drops_its_credentials() -> None:
    for path in _workflow_files():
        for job in _load(path)["jobs"].values():
            for step in job.get("steps", []):
                if step.get("uses", "").startswith("actions/checkout@"):
                    assert step["with"]["persist-credentials"] is False, path.name


def test_ci_green_needs_every_other_job_and_always_runs() -> None:
    jobs = _load(WORKFLOWS / "ci.yml")["jobs"]
    gate = jobs["ci-green"]
    assert gate["name"] == "CI green"
    assert gate["if"] == "always()"
    assert set(gate["needs"]) == set(jobs) - {"ci-green"}
