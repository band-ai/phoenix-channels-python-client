---
name: phoenix-pr-triage
description: Give a Phoenix Channels Python Client PR a quick read-only readiness assessment. Use for "triage this PR" or "is this ready". Inspect goal, owning layer, checks and feedback; return DRAFT, MERGE, HEAVY REVIEW, CHANGES NEEDED, or RETHINK. Use phoenix-code-review for the full six-angle review.
argument-hint: "[target] [--level low|medium]"
---

# PR triage

Follow [the shared runtime](../workflow-runtime.md). This is a bounded
assessment, not implementation, merge approval, or proof of runtime behavior.
Do not edit the user's checkout, post feedback, mark ready, or merge.

## Resolve and inspect

Resolve the target and goal using
[pr-target-scope.md](../phoenix-code-review/pr-target-scope.md). Read the diff
and linked task at the current PR head, using an isolated detached worktree
when needed. Inspect only this run's resources and preserve user changes.

Check draft state, current-head CI, changed file kinds, unresolved feedback,
acceptance coverage, and review history. Use
[feedback intake](../phoenix-here-is-a-review/pr-feedback.md) for pagination
and dispositions, and distinguish
[no-fix records](../phoenix-code-review/no-fix-records.md) from open findings.
Missing evidence is a gap, not proof work was never done. Inspect the actual
workflow before interpreting skips; draft CI skips are expected here.

Source changes without tests, newly disabled tests, public API changes, and
shared protocol/lifecycle changes warrant closer attention. Review history
can reveal recurring causes or fix-induced regressions, but thread counts or
line proximity alone do not establish a design defect.

## Bounded scan

At `low` (default), inspect A–D from
[angle-groups.md](../phoenix-code-review/angle-groups.md), the claimed goal,
and the chosen owner. At `medium`, also map E/F hypotheses without proving
them. Scale delegation to independent concerns and available capacity. If the
diff cannot be meaningfully scanned within triage, route to full review rather
than claim unexamined code is clean. Do not execute repros during triage.

Treat scan output as leads. A defect visible directly in verified source can
be reported as such; a runtime hypothesis needs full review. Distinguish a
supported ownership concern from an architectural preference.

## Verdict

Take the first applicable disposition:

1. **RETHINK:** evidence shows the goal is superseded or the approach/owner
   does not meet the requirement. State the concrete consequence and smallest
   next investigation or scope correction. Recurring review concerns remain
   unverified leads until their mechanism is established.
2. **DRAFT:** report remaining readiness work and any blockers without
   promoting the PR.
3. **CHANGES NEEDED:** current checks fail, a required test was disabled, or
   a concrete defect is established from the inspected source.
4. **HEAVY REVIEW:** unresolved hypotheses/feedback, scope mismatch, missing
   material evidence, or behavior/complexity beyond this scan requires full
   review. Recommend [phoenix-code-review](../phoenix-code-review/SKILL.md).
5. **MERGE:** inspected signals are clear. If checks remain pending, qualify
   the verdict with that condition; this is not approval or merge authority.

For failing-check diagnosis, use
[phoenix-ci-triage](../phoenix-ci-triage/SKILL.md). Report the verdict, head,
goal/owner assessment, evidence, and next step concisely. Always state what
was not checked: low omits E/F scans, medium leaves their hypotheses unproven,
and neither establishes comprehensive security coverage.

Await workers and remove only this run's detached worktree/artifacts using
the shared runtime's cleanup rules.
