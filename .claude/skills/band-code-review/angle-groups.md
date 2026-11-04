# Six review angles

Shared by [code review](SKILL.md) and
[PR triage](../band-pr-triage/SKILL.md). Each reviewer reads the intro and
its assigned section. Test code belongs to D alone, which applies all relevant
angles to it. Each finding has one owning group.

## A — Style and hygiene

Apply [Coding Style](../../../CLAUDE.md#coding-style),
[Comments & Docs Style](../../../CLAUDE.md#comments--docs-style), and
[Python conventions](../band-python-conventions/SKILL.md). Cite the owning
rule rather than invent a parallel standard. Group recurring violations into
one finding with the affected sites.

## B — Design and reuse

Challenge the [goal and owner assessment](pr-target-scope.md) with actual
callers and requirements. Search before claiming duplication. Verify external
behavior in current primary sources and the pinned dependency. Distinguish a
concrete ownership defect from a preference or speculative reuse opportunity.

## C — Runtime safety and observability

Inspect concurrency, resource ownership, and failure reporting under the
[Python conventions](../band-python-conventions/SKILL.md#logging-and-exceptions).
Identify the concrete interleaving for a race. A wrong multi-step outcome goes
to E for proof. Review connection generations, task cancellation, socket
shutdown, and callback lifetimes when the diff affects them.

## D — Test quality

Read [test conventions](../band-python-test-conventions/SKILL.md) in full.
Review changed helpers with their consumers. Map consequential changed
outcomes to tests and report missing branches. Distinguish an intentional
owning-unit internal test from a claim of end-to-end behavior. A skipped or
patched-away interaction cannot establish that interaction's correctness.
Material test-quality violations are `issue:` findings; do not import another
repository's test layout, dependencies, or clock marker.

## E — Edge-case flow analysis

Own failures needing a sequence of state transitions or interactions. A mapper
records changed-flow invariants from verified requirements, a states/events
table, and ordered effects. Consider repeats, retry, cancellation, disconnect,
concurrent changes, and failure between effects where relevant.

Return hypotheses with start state, triggering sequence, reachable event
source, and broken invariant. Include a coverage map per flow: `clean`,
`hypothesis`, or `not examined`. The shared skeptic proves or drops each
hypothesis. Prefer the existing real WebSocket peer and deterministic gates;
do not require a new modeling dependency merely to run review.

## F — Logical bugs

An explorer maps changed local decisions, callers, and tests, then proposes
concrete inputs/states and paths that could fail. One skeptic independently
checks hypotheses from both E and F at the current head. A published defect
requires an executed failing test or repro plus public reachability or the
real caller/event chain. Reasoning alone is not a proven finding.

Drop claims requiring impossible events, private-state corruption, or a mocked
away guard. Return each drop's reason and footprint for
[no-fix records](no-fix-records.md). Proven reachable defects outside
[scope](pr-target-scope.md) remain findings marked `out_of_scope`; they are
not dismissed claims. Proven E/F findings are always `issue:` and include the
proof command and failure sequence in `fix_note`.

These angles do not establish comprehensive security coverage.
