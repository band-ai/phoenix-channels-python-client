---
name: band-dependabot-triage
description: Assess open Dependabot PRs for this repository, identify blocked upgrades, and validate authorized dependency changes. Use for reviewing or handling Dependabot updates or investigating a missing upgrade. Approval, ticket creation, and merges follow the user's authorization.
---

# Dependabot triage

Read [workflow-runtime.md](../workflow-runtime.md). Begin with a read-only
survey; do not infer permission to approve, create issues, push, or merge from
a request for diagnosis.

## Survey

Read `.github/dependabot.yml`: it currently declares root `pip` and
`github-actions` updates weekly. Derive grouping and limits from the actual
configuration and current GitHub documentation; do not assume SDK-specific
limits or infer starvation merely from queue size.

List open Dependabot PRs with pagination, recording number, head SHA, base,
changed files, dependency and target version, bump size, check state, and
review blockers. Identify ecosystems from configuration and file changes,
not solely a title or label. A missing upgrade requires evidence: constraints,
prior closed PRs, configured ignores, or Dependabot update logs.

## Validate requested changes

Read each PR's manifest, lockfile, or workflow diff and upstream release notes.
Check direct constraints, transitive resolution, minimum Python support, and
public API effects. Major upgrades need an explicit compatibility assessment.
Do not presume a red PR is caused by its upgrade; use
[CI triage](../band-ci-triage/SKILL.md) to establish attribution.

Use isolated worktrees/environments for local validation. For authorized batch
work, obtain or reuse the tracking ticket and use
[branch creation](../band-linear-create-branch/SKILL.md). Do not assume a
team, assignee, or cycle. Python updates may change `pyproject.toml` and
`uv.lock`; reproduce the actual constraints and target versions, checking
installed `uv` help before choosing lock commands. Actions updates are
reviewed separately from Python resolution.

Run the project's full test/check gate from the shared runtime for dependency
changes. There are no live-service or extra-specific suites to invent.
A green consolidated batch proves that batch, not every individual PR head.
Before recommending approval of an individual PR, verify its own current head,
applicable checks, and compatibility evidence. Do not label pending or absent
checks green or claim a local OS run proves the entire CI matrix.

On failure, isolate the responsible change before excluding it. If the user
requested tracking issues, prepare evidence-backed followups in the agreed
team. Leave excluded PRs open unless closure is authorized; report each
unresolved upgrade and its next action.

## Authorized publication

Post reviews or push only within the user's authorization, describing the
exact tested set, revision, checks, and limitations. Keep scratch batch
branches local unless publishing them is requested.

Merge only an explicitly authorized set. Re-fetch each PR head, current base,
checks, effective rules, and approvals before each merge; queue admission is
not completion. Confirm server-side merged state and merge commit. Rebased
heads require fresh evidence. Do not bypass required checks or force-push
reconstructed locks as a generic recovery recipe.

## Report

Give a compact table of PR/head, update, verdict, evidence, and next action.
Separate approved, pending, blocked, and untested updates. Include any remote
actions actually taken and remaining checks. Do not claim the queue drained
until the corresponding PRs are confirmed merged or closed.
