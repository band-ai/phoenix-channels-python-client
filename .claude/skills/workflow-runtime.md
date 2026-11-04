# Shared workflow runtime

Use the host's available tools and permissions. Project rules and commands
live in [CLAUDE.md](../../CLAUDE.md). The same skills are exposed through
`.agents/skills` for hosts that discover that directory.

## Tools and authorization

Read linked skill instructions and relevant supporting files. Resolve links
relative to their file and repository paths from the repository root. Verify
installed CLI help and connected tool schemas before using them.

A workflow does not expand the user's scope or permissions. Preserve existing
authorization; ask only for missing material decisions. Local review stays
local unless publication is authorized. Preparing fixes does not itself
permit pushes, review submission, thread replies, ticket updates, or merges.
Follow explicit stop points, including requests to stop before pushing.

For authorized PR creation, start as draft. Assess the final diff, validation,
and unresolved feedback before marking it ready. Triage and plan approval do
not prove implementation readiness. PR naming and descriptions follow
[Git Workflow](../../CLAUDE.md#git-workflow).

## Delegation

Use available delegation within the host's capacity, inheriting its model.
Give each worker its objective, sources, scope, permitted side effects, and
required evidence. Schedule excess roles in later batches. If delegation is
unavailable or disallowed, perform separate sequential passes and disclose
that independent review was unavailable.

Reviewers do not edit shared files, switch refs, or publish. Assign one writer
per file. Experiments that mutate source, Git state, or dependencies need an
isolated checkout/environment or exclusive serial ownership. Preserve user
changes. Return remote actions to the coordinator for authorized publication.
A worker's verification pause does not end an authorized fix task.

## Plans and artifacts

Planning stays planning-only. Use native plan mode when available; otherwise
state the boundary explicitly. Use the host's plan file or one scratch file
outside tracked product files. If writes are prohibited, keep the plan in
chat. Revise one canonical draft rather than accumulating copies.

Finalization authorizes only the agreed plan publication, not implementation.
Obtain renewed finalization for material design changes. Report publishing
restrictions without bypassing them.

Do not expose internal service details, private repository paths, actual
private issue links, or credentials in committed files or public PR text.
Keep scratch artifacts out of commits. Public documentation links
must support the associated claim. Use generic issue identifiers in examples.

## Validation and completion

Select commands from [Commands](../../CLAUDE.md#commands). For localized
Python changes, run affected tests plus lint, format, and type checks. Shared
protocol/lifecycle changes, dependencies, packaging, or broad refactors require
the full suite and checks. Prose-only changes need content/link checks;
executable helpers need behavioral verification. CI remains necessary for
merge readiness. Report checks as passed, failed, blocked, or not run, with
the checked revision and scope; carry results forward only when still valid.

Track workers, processes, temporary worktrees, and scratch files. Await them
or cancel by their recorded handles and confirm completion before removal.
Never kill by process name or remove another run's resources. Report anything
that could not be stopped or checked.
