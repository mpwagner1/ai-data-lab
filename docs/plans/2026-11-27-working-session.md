# Plan: working session, guardrails and court upgrades (2026-11-27)

Board is the record. One branch and one PR per item, branched from `main`. Alfred plans and
dispatches one agent at a time. Nobody merges, closes issues, tags or releases.

## Priority 1: ai-data-lab

| # | Task | Agent (model) | Story | Accept when |
| --- | --- | --- | --- | --- |
| 1 | Protect the guardrails | grayson (Sonnet) | AB#21 | Deny rules for settings, hooks, `~/.codex/**`, board delete and az admin commands; JSON valid |
| 2 | CLAUDE.md rules | grayson (Sonnet) | AB#22 | Approval, untrusted data, fix-round limit, stale review and security-findings rules present |
| 3 | CI baseline | lucius (Sonnet) | AB#23 | `CI green` job needs every job; actions pinned by SHA; Dependabot cooldown 7; actionlint and zizmor pass |
| 4 | Risk register | grayson (Sonnet) | AB#24 | Every suppression has reason, owner, expiry; a test fails on an expired or unlisted entry |

Each PR gets one damian review (Sonnet). The review HEAD must match the PR head.

## Priority 2: bronze publish path

| 5 | red-hood review, then Codex adversarial review; drake reproduces; grayson fixes; damian reviews | red-hood, drake, grayson, damian | AB#2 | Findings are failing tests; Critical or High get no PR |

## Priority 3: court-of-owls

Items 6-14: AB#25 to AB#33. Agents: grayson (6-10, 13, 14), lucius (11, 12). Item 14 is a draft PR.

## Priority 4

Item 15: lucius, AB#34. Item 16: oracle, AB#35.

## Stories only

AB#36 threat model, AB#37 OIDC login, AB#38 scoped board token. State New.

## Notes

- The brief names PRs #133 and #141 as awaiting the owner; both changes are already merged.
- Risk register check is a test, so item 4 does not depend on item 3 being merged.
