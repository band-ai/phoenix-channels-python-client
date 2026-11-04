# Phoenix Channels Python Client

A modern, async Python client library for Phoenix Channels.

## Commands

```bash
# Install for development
uv sync --extra dev

# Run tests
uv run pytest

# Lint, format and type-check
uv run ruff check
uv run ruff format --check
uv run pyrefly check

# Every pre-commit hook
uv run pre-commit run --all-files
```

## Coding Style

- **Declarative Code:** Emphasize intent over implementation mechanics. Code at the call site should clearly express what is happening, while plumbing, boilerplate, and low-level details are hidden behind clean abstractions.
- **Single Concern:** Enforce a strict single responsibility across all levels. Functions, classes, modules, and files must each focus on one clear concern and have a single reason to change.
- **DRY (Don't Repeat Yourself):** Eliminate duplicate logic, behavior, and structural redundancy across the codebase.
- **Reuse Before Inventing:** Scan the existing codebase first. Prefer reusing, improving, or extending what we already have over introducing parallel implementations or reinventing local solutions.
- **No Magic Numbers or Strings:** Replace raw inline literals and string values with descriptive, well-named constants, enums, or configuration parameters.
- **Ruff (strict):** CI always runs `uv run ruff check` with every rule enabled (`select = ["ALL"]`); `pyproject.toml` lists the few ignored rules and why. Imports stay at module top-level (PLC0415). Use `TYPE_CHECKING` for circular-type-only imports; reserve `# noqa: …` only with a real reason on the same line.
- **Strong Types:** Prefer typed models and enums over bare dict/str payloads. Use Pydantic for structured data (config, API shapes, persisted state). Prefer `StrEnum` (or other enums) for closed string sets instead of free-form string literals.
- **Clean Code & Simple Flows:** Prioritize high readability and linear, straightforward execution paths. Keep control flow flat, minimize nesting, and favor simple, predictable logic over clever or overly complex patterns.
- **Match-Case Over If-Else Forests:** Prefer pattern matching (`match`/`case`) or lookup structures over deeply nested, sprawling if-else chains.
- **Do Not Overcomplicate:** If product requirements overly complicate the implementation, pause and ask the user if the requirements can be simplified, letting them decide if it is an absolute must.
- **Leverage Existing Solutions:** Look online for current official docs and API references rather than relying on memory or outdated assumptions. Favor well-maintained third-party libraries with solid provenance instead of reinventing the wheel.

## Comments & Docs Style

- **No Fluff, To the Point:** Keep comments and docs concise, direct, and strictly relevant.
- **No Narration, Keep Factual:** Do not narrate obvious step-by-step code actions in comments or docs; state only factual rationale, constraints, or non-obvious context.
- **Code Should Speak for Itself:** Prioritize self-explanatory code. Only add comments where intent, logic, or an edge case is not immediately obvious from the code itself.

## Testing Style

- **Apply Coding Style to Tests:** All coding style principles outlined above apply equally to test code.
- **Never Assert Assumptions:** Tests should never assert assumptions.
- **Unit & E2E Coverage:** In addition to unit tests, maintain end-to-end (E2E) tests that cover lifelike user flows as well as complex, multi-step scenarios.
- **Common Tooling Over Ad-Hoc Patching:** Build shared test tools and fixtures upfront rather than patching things as you go. Avoid test overfitting and excessive mocking.
- **Investigate Failures Objectively:** When a test fails, do not automatically assume the test itself is broken. Investigate both directions: determine whether the test is flawed or if it has exposed a genuine bug in the implementation.

## When Fixing Bugs

- **Root Cause Over Ad-Hoc Patches:** Never apply ad-hoc or superficial patches. Always investigate deeply to identify and resolve the true underlying root cause.
- **Never Assume, Always Verify:** Do not act on unverified assumptions. Always validate findings, hypotheses, and behaviors with concrete evidence and reproduction.
- **Plan Before Implementing:** Formulate and evaluate the fix before writing code. Apply a clean, elegant, and architecturally sound solution rather than bolt-on patching.

## Git Workflow

- **PR Titles:** Every PR title must be a Conventional Commit (`type(scope): description`) — squash-merge uses it as the commit subject, and that's what release-please parses to decide the next version and changelog. Enforced by CI (`pr-conventions.yml`); the allowed types are release-please's own, defined once in `release-please-config.json`'s `changelog-sections` — don't duplicate that list elsewhere. See release-please for how it maps commit types to version bumps.
- **Branch Naming:** Name branches `<type>/<slug>-<LINEAR-ID>`, using the same types as PR titles and ending in the Linear issue the PR addresses (e.g. `feat/add-user-auth-ENG-123`) — every PR needs a Linear ticket. Use `git lb` to create a branch from a Linear issue if it's installed; otherwise ask for the proper branch name. Enforced by CI (`pr-conventions.yml`), reading the same type list as the PR-title check. Ask for the Linear ticket ID (or confirmation that it's fine to open the PR without one) before starting work and creating the branch — this repo has no single default team, so it can't be guessed. Asking late, after the branch and PR already exist, just trades the same question for a failing required check.
- **Finalize the PR:** Before asking for review, update the PR title and description to match the final diff. Keep the description short and simple, in three parts:
  - **Scope:** what the PR covers, and what it deliberately leaves out, with links to any follow-up tickets. Reviewers then know where the change ends and don't comment on code outside it.
  - **What:** the changes, briefly.
  - **Why:** the reason for them.

## When Debugging

- **Durable Logging:** Feel free to add high-quality, meaningful logs that not only help isolate the current bug but remain valuable and maintainable for future debugging.
- **Verify, Prototype & Validate:** Verify everything. Prototype solutions, test hypotheses actively, and validate all behaviors before drawing conclusions.

## Skills

Project rules live in this file. Skills add task-specific guidance.
Skills are maintained in `.claude/skills/` and exposed through `.agents/skills/`
by a relative symlink. `AGENTS.md` links to this file so supported agent hosts
share the same project rules. Invoke a skill with `/name` in Claude Code or
`$name` in Codex; hosts may also select relevant skills from their descriptions.

| Skill | Use |
| --- | --- |
| [phoenix-plan](.claude/skills/phoenix-plan/SKILL.md) | Investigate and review a ticket plan before implementation. |
| [phoenix-python-conventions](.claude/skills/phoenix-python-conventions/SKILL.md) | Write or review Python source, logging, and error handling. |
| [phoenix-python-test-conventions](.claude/skills/phoenix-python-test-conventions/SKILL.md) | Write or assess tests, coverage, peers, and synchronization. |
| [phoenix-code-review](.claude/skills/phoenix-code-review/SKILL.md) | Run the six-angle review when explicitly requested. |
| [phoenix-here-is-a-review](.claude/skills/phoenix-here-is-a-review/SKILL.md) | Verify and address supplied review feedback. |
| [phoenix-pr-triage](.claude/skills/phoenix-pr-triage/SKILL.md) | Assess a PR quickly without changing code or publishing. |
| [phoenix-ci-triage](.claude/skills/phoenix-ci-triage/SKILL.md) | Diagnose failing GitHub Actions checks. |
| [phoenix-investigate-test-error](.claude/skills/phoenix-investigate-test-error/SKILL.md) | Investigate a failing or flaky test with an unclear cause. |
| [phoenix-linear-create-branch](.claude/skills/phoenix-linear-create-branch/SKILL.md) | Create a branch for an identified Linear ticket. |
| [phoenix-dependabot-triage](.claude/skills/phoenix-dependabot-triage/SKILL.md) | Assess Dependabot updates and validate authorized changes. |
