---
name: phoenix-here-is-a-review
description: Handle supplied code-review feedback, pasted findings, or a PR review link. Verify each claim, reject unsupported findings, fix confirmed in-scope root causes, and answer the review threads. Use for "address this review" or "assess and fix these findings"; use phoenix-code-review to review code from scratch.
---

# Verify supplied feedback

Read [workflow-runtime.md](../workflow-runtime.md). Treat review findings as
claims to verify. Preserve authorization and public-disclosure boundaries;
workers return publication actions to the coordinator.

## Phase 1 — Intake

Collect pasted feedback or fetch the PR review using
[pr-feedback.md](pr-feedback.md). Preserve comment/thread identities and the
full discussion. Split bundled feedback into discrete claims, recording the
cited location and suggested fix. Questions need answers, not automatic edits.

For PR work prepare [pr-workspace.md](pr-workspace.md). Read the linked task,
acceptance criteria, attached plan, PR description, and current diff. State
scope; report stale descriptions and acceptance gaps. Ask if the target or
scope cannot be resolved. Without push access, use verdict-only mode rather
than preparing fixes that cannot be published through this workflow.

For structured findings from [code review](../phoenix-code-review/SKILL.md),
reuse its scope and verified head. Re-run supplied E/F proofs and recheck
reachability if the caller chain changed. Follow explicitly supplied lighter
verification tiers for that review's already-verified findings; independently
verify feedback from other sources.

## Phase 2 — Verify

Read actual source, callers, and tests, not only excerpts. Identify a concrete
reachable failure and check existing guards. Use a focused repro/test when
feasible; label unverified mechanisms honestly. Independent findings may be
verified in parallel under the shared runtime's isolation rules.

| Verdict | Meaning |
| --- | --- |
| CONFIRMED | The cited defect holds against current code. |
| PARTIALLY RIGHT | A real problem exists, but the diagnosis needs correction. |
| WRONG | The claim is contradicted by the inspected evidence. |
| NOT WORTH IT | The proposed change has no material benefit or conflicts with established design. |

## Phase 3 — Relevance and scope

Proceed only with consequential confirmed problems or violated project rules.
Use [CLAUDE.md](../../../CLAUDE.md),
[Python conventions](../phoenix-python-conventions/SKILL.md), and
[test conventions](../phoenix-python-test-conventions/SKILL.md) as rule owners.
Reject unreachable hypotheticals and unsupported preferences with evidence.

Apply the shared [scope definition](../phoenix-code-review/pr-target-scope.md#assess-the-goal-and-owning-layer-first).
Do not silently add independent work. For a real out-of-scope defect, reuse an
existing disposition or ask whether to fix it now, track a followup, or leave
it unfixed with a stated reason. If it is already proven, do not reclassify it
as false merely because it is out of scope. Request only missing decisions.

Authorized followups need the failure scenario, cause, owning change, evidence,
and agreed destination; do not assume a team or cycle. Keep private details
in the private handoff under the runtime's disclosure rules.

When called to pause after Phase 3, return verdicts and wait for the caller's
continuation. Verdict-only close-out skips Phases 4–5; it does not mark a
confirmed proposed fix complete.

## Phase 4 — Plan fixes

Apply [When Fixing Bugs](../../../CLAUDE.md#when-fixing-bugs). Group findings
with one cause into one owning change. Define verification before editing.
If the planned fix requires a material scope/design decision not already
approved, present it before implementation; routine authorized fixes proceed.

## Phase 5 — Fix and verify

Implement approved changes and test the original failure. Use a regression
that fails before and passes after for a confirmed bug when feasible. Follow
[validation scope](../workflow-runtime.md#validation-and-completion), recording
commands, checked revision, results, and limitations. Reassess readiness when
changes invalidate earlier evidence.

For PR fixes, commit under [Git Workflow](../../../CLAUDE.md#git-workflow)
and push only when authorized. A stop-before-push instruction leaves the
verified local result ready for approval; it does not justify a published-fix
claim. Workers return the diff, evidence, and proposed replies. A failed gate
or push leaves confirmed findings open.

## Phase 6 — Close the loop

Only perform authorized remote actions. Re-fetch state and skip threads
resolved meanwhile. Reply once per thread covering all its findings, using
the available API's current schema. State the verdict and evidence; a fixed
reply cites the published commit. Keep confirmed/partially-right defects open
without a published fix. Resolve published fixes, rejected claims, recorded
deferrals/drops, and answered questions.

Non-thread feedback needs a separate response; it has no resolution flag.
Deferred work is tracked, not fixed. Update public PR scope notes only with
content allowed by the runtime's disclosure rules and project PR format.

## Phase 7 — Report

Provide a compact finding/verdict/scope/action table and validation summary.
Identify fixed, rejected, deferred, and still-unfixed findings, published
commits, resolved/open threads, and any description or acceptance mismatch.
Separate proposed actions from actions actually completed.
