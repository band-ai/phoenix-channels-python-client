## PR Title Format

> **Required:** PR titles must follow Conventional Commits format:
> `type(scope): description`
>
> **Types:** the `type` values in `release-please-config.json`'s `changelog-sections`
>
> **Examples:**
> - `feat(client): add automatic reconnection`
> - `fix(protocol): resolve v2 message parsing`
> - `docs: update usage examples`

---

## Summary

<!-- Brief description of what this PR does -->

## Changes

<!-- List the main changes in this PR -->
-

## Related Issues

<!-- Link any related issues using "Fixes #123" or "Relates to #123" -->

## Testing

<!-- Describe how you tested these changes -->
- [ ] Unit tests pass (`uv run pytest`)
- [ ] Pre-commit checks pass (`uv run pre-commit run --all-files`)

## Checklist

- [ ] PR title follows Conventional Commits format
- [ ] Code follows project style guidelines
- [ ] Tests added/updated as needed
- [ ] Documentation updated as needed
