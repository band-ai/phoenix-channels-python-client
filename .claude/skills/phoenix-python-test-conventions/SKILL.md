---
name: phoenix-python-test-conventions
description: Write, extend, or review Phoenix Channels Python Client tests and assess behavior coverage. Use for pytest fixtures, assertions, fake WebSocket peers, lifecycle synchronization, controlled clocks, flaky tests, or coverage of changed Python behavior.
---

# Test conventions

Project-wide rules live in [Testing Style](../../../CLAUDE.md#testing-style)
and [Coding Style](../../../CLAUDE.md#coding-style). Also read
[Python conventions](../phoenix-python-conventions/SKILL.md).

## Choose coverage before writing

Name each consequential decision and its reachable outcomes. Map scenarios to
tests, including complementary branches: null/current/stale references,
current/old connection generations, callback success/failure, cancellation,
retry, and cleanup where relevant. Report unexamined outcomes; line coverage
and passing counts alone do not establish behavior coverage.

Choose the lowest boundary that preserves the interaction. Parser tests can
call the parser; delivery, reconnection, and lifecycle claims should exercise
the real client with a focused peer. A failing-before/passing-after regression
is the preferred proof of a bug fix.

## Existing test tools

- `tests/conftest.py`: protocol, connected client, local server, and received
  message fixtures.
- `tests/fake_server.py`: `FakePhoenixServer`, real loopback WebSocket framing,
  server events, and independent protocol vocabulary.
- `tests/support.py`: client construction, shared inputs, protocol
  parametrization, and observable helpers.
- `tests/harness.py`: focused protocol/runtime harnesses for owning-unit tests.
- `tests/clock.py`: controlled loop and `clock`/`clocked_peer` fixtures.
- `tests/scheduling.py`: gates and timer observation for native task behavior.

Inspect the nearest test and helper before choosing one; do not infer a
helper's guarantees from its name. New shared mechanics belong in focused
helper modules, fixtures in `conftest.py`, and scenario actions/assertions in
the test. Test packages keep `__init__.py` free of helper logic.

## Assertions and layout

Follow the neighboring suite's layout: module-level functions or behavior
classes. Preserve class-level markers and parametrization. Name each test for
one observable claim; use parametrization for independent variations instead
of branching on a scenario inside a test. Use plain `assert` for readable
pytest diffs. Compare the relevant complete outcome rather than hiding
unexpected fields; an uncontrolled incidental field can use a focused matcher.

Public behavior is the preferred observation boundary. Intentional internal
unit tests may inspect their owning component's invariants; they do not prove
end-to-end delivery. A mock call alone does not prove the resulting behavior.
Keep multi-step sequences together when their interaction is the claim.

Shared test inputs have one owner. Keep the fake server's wire vocabulary
independent of client constants so it can detect protocol mistakes. Ground
ordinary peer frames in the upstream contract; label malformed or stale
frames as deliberate fault injection.

## Real boundaries

Run the client behavior being tested for real. Use a working external peer,
not a replacement for the subject's routing, scheduling, or cleanup. Do not
replace running client methods to force a sequence. Patching is justified for
an otherwise inaccessible boundary or controlled nondeterminism; document why
and keep the real interaction intact. Existing native scheduling probes are
available when a public callback cannot expose the required ordering.

## Synchronization and time

Prove ordering, cancellation, and cleanup with events, queues, gates, and
awaited completion. Polling or a grace-period sleep is not correctness proof.
A timeout around a test is a hung-test guard, not the synchronization signal.

Timer tests use `@pytest.mark.clock` with fixtures from `tests/clock.py`;
`pytest.ini` disables the automatic looptime plugin. Establish the relevant
timer registration or wait before advancing `clock`. Advance time for a
claimed deadline expiry, not to guess whether unrelated work completed.
Real socket readiness remains independent of virtual time. A peer receipt
alone does not prove the client's matching wait has settled.

## Isolation and operating systems

Use `tmp_path` for temporary files and OS-assigned ports for local peers.
Avoid live external services, shared mutable state, and fixed ports. Await
all tasks, servers, and client cleanup, including failed/cancelled startup.
Tests must work alone and in suite order without depending on another test.
Do not assume xdist or random-order plugins are installed.

CI exercises Ubuntu and Windows. Use portable path construction and local
peers. An unavoidable OS-specific boundary needs an explained skip; a skip
is not coverage of that operating system. Do not introduce Hypothesis or
other dependencies merely to satisfy a generic testing recipe.
