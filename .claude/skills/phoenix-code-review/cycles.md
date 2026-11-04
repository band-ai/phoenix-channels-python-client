# Fix cycles

One cycle is a full review, verification, and its authorized fix round. Triage
and behavior-preserving cleanup happen once before the first cycle. Without
`--fix`, run one review and the verdict-only close-out. With `--fix`, respect
the requested cycle cap; findings from the last cycle still get their fix round.

## Evidence ledger

Record the head at each cycle start and each verified finding's severity,
file/symbol, broken invariant, fixing commit, and origin. Distinguish a fresh
finding from a regression of an earlier fix or a problem introduced by one.
Unverified/dropped claims are not defects; use
[no-fix records](no-fix-records.md) for investigated claims that do not hold.

## Continue or stop

An actionable issue is a verified `issue:` finding with a CONFIRMED or
PARTIALLY RIGHT verdict, in scope or explicitly authorized for this run, and
not deferred. This applies to every angle group, independently of whether
the finding blocks GitHub approval.

- No unresolved actionable issues: finish authorized nits/suggestions and stop.
  Do not add a cycle solely to re-review final cleanup.
- Actionable issues remain and fixes are making supported progress: apply
  verified fixes and continue within the cap.
- Regressions recur or fixes appear to create further failures: investigate
  the shared cause and ownership before adding another patch. Counts and
  commit ancestry are leads, not proof of an architectural defect. Present
  evidence-backed design alternatives when a material decision is needed,
  preserving existing authorization and leaving unfixed confirmed threads open.
- At the cap: finish that round's authorized fixes and report the remaining
  findings and that the last changes have not received another review cycle.

Validate new edits under [the shared runtime](../workflow-runtime.md).
Changed scope or a proposed design decision goes through the remediation
skill's scope gate, not an automatic extension of the run. Report whether the
run converged, reached its cap, or stopped for a concrete unresolved decision.
