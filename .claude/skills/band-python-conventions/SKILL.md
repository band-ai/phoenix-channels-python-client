---
name: band-python-conventions
description: Python source conventions for Phoenix Channels Python Client. Use when writing or reviewing Python source, imports, logging, or error handling. Use band-python-test-conventions as well for tests and coverage.
---

# Python conventions

Read [Coding Style](../../../CLAUDE.md#coding-style),
[Comments & Docs Style](../../../CLAUDE.md#comments--docs-style), and
[When Fixing Bugs](../../../CLAUDE.md#when-fixing-bugs) for shared rules.
Test-specific guidance lives in
[band-python-test-conventions](../band-python-test-conventions/SKILL.md).

## Imports and public API

Use type hints on function parameters and return values. Target the Python
version in `pyproject.toml`; use native generics and union syntax. Use
`from __future__ import annotations` when forward references require it.

Use absolute imports from the module that owns a symbol, under
`phoenix_channels_python_client` or `tests`. Public re-exports belong in
`phoenix_channels_python_client/__init__.py`; implementation modules import
owners directly. Preserve documented public imports when changing internals.
Ordinary `__init__.py` files do not own runtime logic or test helpers.

## Logging and exceptions

Use `logging.getLogger(__name__)` and lazy `%s` arguments for log messages.
Do not print from library code. Logs must not expose credentials or message
contents; choose levels that keep routine reconnect activity usable.

Use the existing exception hierarchy in
`phoenix_channels_python_client/exceptions.py` for channel failures. Preserve
exception causes and cancellation semantics. Catch only exceptions the layer
can handle; distinguish model validation failures when their recovery differs.
Do not replace exceptions with process exits. Pydantic serialization uses
`model_dump()`; protocol framing belongs to the existing protocol handler.
