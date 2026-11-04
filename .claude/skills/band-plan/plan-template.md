# Plan content

Keep these sections concise and revise them in place:

1. **Problem:** verified cause, owning layer, and why that level is appropriate.
2. **Goals and non-goals:** ticket requirements and explicitly excluded work.
3. **Scenarios:** given/when/then outcomes for callers and real peers, including
   relevant boundaries, failure/recovery, and unchanged neighboring behavior.
4. **Approach:** proposed changes and paths, with sources supporting contracts.
5. **Alternatives:** realistic options and reasons for the recommendation.
6. **Compatibility:** public symbols, configuration, defaults, wire shapes,
   supported Python versions, release impact, and any migration requirement.
7. **Cross-cutting:** security, logging, concurrency/ordering, resource cleanup,
   and Ubuntu/Windows behavior, marking irrelevant concerns explicitly.
8. **Test plan:** each scenario's meaningful regression and existing fixtures
   or peers to reuse. A needed helper is a step before its consuming test.
9. **Design validation:** the matrix from
   [scenario-validation.md](scenario-validation.md), with expected outcomes,
   sourced traces, verdicts, execution limits, and remaining runtime checks.
10. **Definition of done:** exact applicable commands from
    [CLAUDE.md](../../../CLAUDE.md#commands), targeted pytest paths and expected
    results, plus material manual checks. Follow the shared runtime's PR
    readiness and authorization rules.

Apply the [Python conventions](../band-python-conventions/SKILL.md) and
[test conventions](../band-python-test-conventions/SKILL.md) rather than
repeating their rules. Protocol/lifecycle coverage reuses `FakePhoenixServer`
and the local clock/scheduling tools where appropriate; do not invent a
live-service suite or require a new dependency for a generic recipe.

Every step must serve the ticket or verified cause. Propose independent
out-of-scope changes for discussion rather than silently include them. Resolve
material draft questions before publication; preserve remaining non-blocking
verification limits explicitly.
