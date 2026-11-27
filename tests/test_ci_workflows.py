"""Guards for the GitHub Actions workflows that linters do not enforce."""

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
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


def test_no_job_or_step_continues_on_error() -> None:
    for path in _workflow_files():
        for name, job in _load(path)["jobs"].items():
            assert "continue-on-error" not in job, f"{path.name}: {name}"
            for step in job.get("steps", []):
                assert "continue-on-error" not in step, f"{path.name}: {name}"


def _gate_step() -> dict[str, Any]:
    steps = _load(WORKFLOWS / "ci.yml")["jobs"]["ci-green"]["steps"]
    assert len(steps) == 1
    # No `if`, `shell` or other key that could skip or soften the check.
    assert set(steps[0]) == {"name", "env", "run"}
    return steps[0]


def _run_gate(needs: dict[str, dict[str, str]]) -> int:
    """Run the script from ci.yml the way the runner does and return its exit code."""
    command = ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", _gate_step()["run"]]
    env = {**os.environ, "NEEDS": json.dumps(needs)}
    return subprocess.run(command, env=env, capture_output=True, check=False).returncode


def test_ci_green_reads_the_results_of_its_needs() -> None:
    assert _gate_step()["env"] == {"NEEDS": "${{ toJSON(needs) }}"}


def test_ci_green_passes_when_every_job_succeeded() -> None:
    assert _run_gate({"a": {"result": "success"}, "b": {"result": "success"}}) == 0


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", ""])
def test_ci_green_fails_when_any_job_did_not_succeed(result: str) -> None:
    assert _run_gate({"a": {"result": "success"}, "b": {"result": result}}) != 0
    assert _run_gate({"a": {"result": result}, "b": {"result": "success"}}) != 0


def test_ci_green_fails_with_no_results() -> None:
    assert _run_gate({}) != 0


TRIGGER_PAYLOADS = {
    "issue_comment": "comment",
    "pull_request_review_comment": "comment",
    "pull_request_review": "review",
    "issues": "issue",
}


def test_claude_job_requires_a_trusted_author_on_every_trigger() -> None:
    workflow = _load(WORKFLOWS / "claude.yml")
    # YAML 1.1 reads the `on` key as the boolean True.
    assert set(workflow[True]) == set(TRIGGER_PAYLOADS)
    clauses = workflow["jobs"]["claude"]["if"].strip().splitlines()
    assert len(clauses) == len(TRIGGER_PAYLOADS)
    for event, payload in TRIGGER_PAYLOADS.items():
        guarded_start = (
            f"(github.event_name == '{event}' && "
            """contains(fromJSON('["OWNER","MEMBER","COLLABORATOR"]'), """
            f"github.event.{payload}.author_association) && "
        )
        assert sum(clause.startswith(guarded_start) for clause in clauses) == 1, event
