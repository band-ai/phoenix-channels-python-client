---
name: band-linear-create-branch
description: Create or preview a Git branch for a Linear ticket in this repository. Use for a request to start ticket work on a branch; resolve an explicit issue or confirm an inferred issue before changing Git state.
---

# Create a ticket branch

Read [workflow-runtime.md](../workflow-runtime.md) and
[Git Workflow](../../../CLAUDE.md#git-workflow), which owns the naming rule.
The allowed commit types come from `release-please-config.json`; do not copy
its list into this skill.

1. Resolve the ticket from explicit input. Confirm an inferred ticket before
   changing Git state. Preserve an already supplied branch name.
2. Inspect the current branch and worktree. Preserve uncommitted changes and
   use the base the user requested; do not silently replace local commits.
3. Discover `git lb`. Inspect the installed `git-lb` help/source for supported
   options before use. If supported, `git lb --dry-run TEAM-123` previews its
   proposed name without creating a branch.
4. Validate the proposed name against the actual branch check in
   `.github/workflows/pr-conventions.yml` and the user's requested name.
   A helper's label mapping does not override repository policy or user intent.
5. For an accepted helper result, run `git lb TEAM-123`. If unavailable or
   unsuitable, use the user-supplied valid name with `git switch -c <name>`;
   otherwise ask for a name, following the project rule.
6. Verify the resulting branch and report it. A preview request stops at the
   preview. Relay missing credentials or issue lookup errors without exposing
   values or installing tooling automatically.
