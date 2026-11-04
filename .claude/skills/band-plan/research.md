# Evaluate a dependency

Use for a serious candidate, with evidence from current primary documentation
and the tagged source of the version under consideration.

- Maintenance, release activity, license, and adoption.
- Fit with `pyproject.toml`: supported Python versions, async and typing
  contracts, dependency weight, and existing Pydantic/WebSocket constraints.
- Resolution against `uv.lock` and applicable `dev`/`test` extras. Do not
  assume extra-specific environments or conflicts absent from this project.
- The exact capability needed, including failure and cancellation semantics.
  If execution is necessary to establish it, name the confirming call and
  mark it unverified until run under the planning permissions. A critical
  unknown blocks publication; a non-blocking one needs an implementation check.
