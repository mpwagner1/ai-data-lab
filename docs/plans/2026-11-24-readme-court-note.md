# Plan: README note on the court workflow

Date: 2026-11-24
Status: Tasks done 2026-11-24; PR 138 awaits merge (issue #136, questions #137)

## Goal
Add one line to the README, mainly to test the court's full flow from plan to issue to dispatch to PR to log.

## Assumptions
- batman asked for "a quick note just so we can test the functionality", so alfred picked the wording and the place.
- The line goes at the end of `## Conventions` in `README.md`:
  `- Agent work is planned and routed by the court-of-owls plugin; plans and the dispatch log live in docs/plans/.`
  (with `docs/plans/` in backticks). This is true as of #132, which adopted the plugin.
- This is a docs-only change, so oracle owns it. No code, contract or infrastructure changes.
- One GitHub issue, one branch `agent/<issue>-readme-court-note` and one PR. Nothing is stacked.
- The review step is included only to exercise the flow. It isn't strictly needed for a single line.

## Tasks
| # | Task | Agent | Model | Acceptance criteria | Depends on |
|---|------|-------|-------|---------------------|------------|
| 1 | Add the line above to the end of `## Conventions` in `README.md`, on branch `agent/<issue>-readme-court-note`, and open a PR that links the issue | `court-of-owls:oracle` | Sonnet | `git diff main --stat` shows only `README.md` with 1 line added; the line names the plugin and `docs/plans/`; the nothing-sensitive `git grep` check prints nothing; `uv run pre-commit run --all-files` passes or is reported under Not run with a reason; the PR body states what, why, how checked | - |
| 2 | Review the PR from task 1 | `court-of-owls:damian` | Sonnet | Review left as PR comments only, with each finding carrying evidence or "no findings" | 1 |

alfred logs each dispatch in `docs/plans/log.md`.

## Risks
- **oracle may not have a shell.** Its tools are Read, Grep, Glob, Edit, Write and vault tools, with no Bash, so it may not be able to create the branch, commit, push or open the PR. That makes this a useful test. If it happens, I'll log the gap and ask batman whether grayson should take docs-only PRs. I won't do the git steps myself.
- **The note could drift from reality** if the workflow changes later. Mitigation: oracle owns the README and updates it after merges.

## Open questions
- If oracle can't open PRs, should grayson do the git and PR steps for docs changes, or should oracle be given a shell?
