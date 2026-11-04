---
name: phoenix-investigate-test-error
description: Investigate a concrete failing or flaky Phoenix Channels Python Client test whose cause is unclear. Compare product behavior, test assumptions, and test tooling using empirical evidence. Use phoenix-ci-triage first when only a failing Actions check is known.
---

# Investigate a test failure

Read [workflow-runtime.md](../workflow-runtime.md) and
[test conventions](../phoenix-python-test-conventions/SKILL.md). This workflow
diagnoses; implement a fix only when requested. For an unidentified CI failure,
start with [phoenix-ci-triage](../phoenix-ci-triage/SKILL.md).

## Establish a baseline

Reproduce the exact test command, preserving the revision, environment,
traceback, expected/actual result, and relevant fixtures/helpers. For suspected
flakiness, repeat the focused test and record the run count and failures;
compare isolated and suite execution where relevant. If it cannot reproduce,
report that limit rather than asserting a cause.

## Competing explanations

Assign independent investigators these three questions using the runtime's
ownership/isolation rules, or perform them as disclosed sequential passes.
Each receives the same self-contained baseline and must report its experiment,
raw result, and whether its hypothesis was confirmed, refuted, or unverified.

1. **Product behavior:** trace the real entry point through
   `phoenix_channels_python_client/`, including protocol routing, topic runtime,
   supervision, or reconnect policy as relevant. Demonstrate the violated
   contract through a reachable sequence.
2. **Test assumptions:** compare assertions with verified protocol and client
   contracts. Inspect ordering, generation/reference ownership, timer
   registration, cancellation, and isolation. Follow the test conventions'
   synchronization boundary rather than treating elapsed time as evidence.
3. **Test tooling:** isolate the peer, harness, clock, or fixture behavior.
   Inspect `tests/fake_server.py`, `tests/harness.py`, `tests/clock.py`,
   `tests/scheduling.py`, and the relevant support modules. Demonstrate the
   tooling mismatch independently of the subject when possible.

Reading alone is not empirical validation. Use a focused test, standalone
repro, isolated instrumentation, or a controlled expected/actual comparison.
Instrumentation, dependency edits, and bisects must preserve the baseline and
run in isolation. Do not fix while investigating or discard user changes.

## Synthesize

Challenge unsupported conclusions and reconcile conflicting results. A bug
can coexist with a brittle test. Rank supported causes with file/line and
experiment evidence; list refuted hypotheses, unverified claims, and the next
specific fix or discriminating experiment. Name the owning layer. Clean up
only this investigation's resources using the runtime guide.
