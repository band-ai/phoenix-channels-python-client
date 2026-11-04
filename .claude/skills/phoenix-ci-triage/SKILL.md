---
name: phoenix-ci-triage
description: Diagnose failing GitHub Actions checks for a Phoenix Channels Python Client PR or commit. Use for "why is CI red" or "fix these checks". Use phoenix-pr-triage for readiness and phoenix-investigate-test-error once a failing test is isolated.
---

# CI triage

Read [workflow-runtime.md](../workflow-runtime.md). Diagnosis is read-only;
implement in-scope fixes only when requested.

## Attribute the failure

Resolve the repo, PR head/base, check URL, run `headSha`, event, attempt, job,
Python version, and operating system. A stale run describes its own revision.
A merge-group run tests a synthetic candidate; inspect its relation to the
PR head and base. Pending, cancelled, skipped, and unavailable are not passes.

Use installed CLI help to verify commands. Typical reads are `gh pr checks`,
`gh run view --log-failed`, and `gh run view --job <id> --log`, always scoped
to the resolved repository. Preserve the first actionable failure and enough
context to explain it. Redact sensitive log contents. Inaccessible logs leave
an evidence gap, not an invented diagnosis.

## Repository workflows

Read the workflow at the tested revision:

- `.github/workflows/ci.yml`: dependency setup, pytest matrix, coverage,
  lint/type jobs, draft handling, and merge-group events. Derive versions and
  commands from the tested revision rather than hard-code them here.
- `.github/workflows/pr-conventions.yml`: human PR title and branch checks
  derive allowed types from `release-please-config.json` through
  `.github/scripts/conventional-types.sh`. Bot PRs follow separate handling.
- `.github/workflows/release.yml`: release-please creates the release.
- `.github/workflows/publish.yml`: builds an existing release tag and publishes
  through the `release` environment. It supports manual recovery; diagnosis
  does not authorize dispatch or upload.

Recheck these facts from current YAML rather than treating this map as the
workflow definition. There is no CI rollup job or separate framework lane.
For release/queue failures, inspect current rulesets and environment policy
when relevant; YAML alone cannot prove effective permissions.

## Reproduce and report

Separate source/test, resolution/packaging, workflow, prerequisites, and
infrastructure causes. A passing matrix entry does not establish why another
entry fails or exclude dependency, runner, or test-assumption causes. Reproduce the smallest failing boundary with the
job's dependency setup. Use an isolated environment if another run owns the
working environment. For an unclear test failure, use
[phoenix-investigate-test-error](../phoenix-investigate-test-error/SKILL.md).

Validate authorized fixes using the runtime's validation scope. Do not weaken
checks, add retries, or publish artifacts to obtain green status. After an
authorized push, inspect the new head's checks; report pending work rather
than borrowing success from an earlier head. Do not merge through this skill.

Report revision/job, cause and supporting evidence, fix or next experiment,
and check results. For a readiness request use
[phoenix-pr-triage](../phoenix-pr-triage/SKILL.md).

Command references: [checks](https://cli.github.com/manual/gh_pr_checks),
[run details and logs](https://cli.github.com/manual/gh_run_view).
