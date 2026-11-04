---
name: band-plan
description: Turn a Linear ticket into a reviewed Phoenix Channels Python Client implementation plan. Use for "plan this ticket" or a request to design ticket implementation. Verify the cause, discuss scenarios, and publish only after finalization. Never implements; use band-here-is-a-review for supplied feedback.
argument-hint: "[ticket-id-or-url]"
---

# Plan a ticket

Read [workflow-runtime.md](../workflow-runtime.md). Keep this task planning-only.
Use one canonical plan artifact under its artifact rules and state its location.
Read [CLAUDE.md](../../../CLAUDE.md) and the applicable
[Python](../band-python-conventions/SKILL.md) and
[test](../band-python-test-conventions/SKILL.md) conventions.

## Phase 0 — Resolve and size

Read the ticket with `get_issue` and its complete comments with `list_comments`.
Extract the identifier from a URL rather than infer it from a slug. Inspect
attachments and read any prior plan. Ask whether to revise or replace an
existing plan; retain its attachment identity for authorized replacement.
If Linear tools are unavailable, request the ticket/comments and deliver the
plan locally rather than claiming it was attached.

A light ticket stays in one area and changes no public API, configuration, or
wire shape. Run all applicable phases inline for it, explaining skipped
research. Otherwise use full planning. Upgrade the scope classification if
investigation reveals a broader contract change.

## Phase 1 — Establish ground truth

Fetch `origin` and inspect `origin/main`, reporting divergence from local code.
Read relevant implementation and behavior tests, not only the reported line.
Verify external behavior against current primary documentation and pinned
source. For Phoenix framing and membership, inspect the corresponding upstream
serializer/client contract; for WebSocket behavior, inspect the installed
`websockets` version. Read external consumers only when the proposed change
actually affects them; do not assume another repository is available.
Delegate independent research with self-contained read-only briefs when
available. Each brief names the question, scope, sources, and evidence needed.
Integrate every result, including unverified claims.

## Phase 2 — Survey owners and reuse

Find the existing mechanism and single owner of each changed policy/value.
Trace the producer, protocol parser, subscription/runtime, supervisor, and
consumer as relevant. Record affected public callers and why the chosen owner
has the right responsibility and lifetime. For a local fix, explain why no
external comparison is necessary.

## Phase 3 — Establish the cause and level

State the underlying problem in one sentence, with evidence. Decide whether
it is a local defect or a responsibility/design problem. Consider duplicate
paths, ownership across reconnect generations, and whether a proposed guard
only hides the symptom. Choose the smallest change at the cause's owner.
If that exceeds the ticket, retain alternatives for discussion rather than
silently expanding scope or substituting a workaround.

## Phase 4 — Research relevant options

Research the actual mechanism or protocol question using primary sources.
Evaluate existing local mechanisms before proposing dependencies. For serious
library candidates use [research.md](research.md). Explain when external
research is unnecessary. Record compatibility and maintenance tradeoffs;
never treat a plausible API name as proof of a capability.

## Phase 5 — Draft

Use [plan-template.md](plan-template.md). Keep facts, decisions, sources, and
scenarios in the canonical draft; investigation history stays in chat.
Identify unresolved material decisions for Phase 7.

## Phase 6 — Review

Give an independent reviewer the ticket and draft without the author's
research conclusions, where delegation is available. Require re-verification
against `origin/main`, a challenge to the cause/owner/scope, and a pre-mortem
of likely failure reasons. Use a disclosed separate pass if independence is
unavailable. Compare every scenario with the proposed design and test plan.

Apply the same claim-verification discipline as
[review remediation](../band-here-is-a-review/SKILL.md). Resolve minor
choices with evidence; bring material scope, compatibility, or design choices
to the user. Do not create followup tickets without authorization.

## Phase 7 — Discuss

Present the understood goal/cause, observable scenarios, interpretation risks,
approach, and decisions needed. Ask for feedback, not implementation. Revise
the same draft and repeat affected research/review after material changes.
A quiet pause is not finalization. An explicit finalization signal advances
to validation; it never authorizes implementation.

## Phase 7a — Validate the agreed design

Run [scenario-validation.md](scenario-validation.md) against the agreed draft,
including light plans. Repair and revalidate deficiencies. A material change
returns to discussion and requires renewed finalization. A source-grounded
walkthrough supports a design; it is not a runtime test pass.

## Phase 8 — Publish when authorized

Re-read the complete draft and its validation matrix. Correctness-critical
unknowns block publication. Add a self-contained Implementation Prompt with
the ticket, cause/owner, goals/non-goals, files, validated scenarios, remaining
checks, and definition of done. Include any non-blocking verification limit.

Publish only the agreed artifact/update using [publish.md](publish.md).
Report its location or a blocked operation. The implementing agent must prove
the resulting behavior separately.
