"""Callable installer shapes for registration-boundary tests."""

from __future__ import annotations

import asyncio


class AsyncInstaller:
    """Expose an asynchronous __call__ rather than an async function object."""

    def __init__(self) -> None:
        self.ran = asyncio.Event()

    async def __call__(self, response: object) -> None:
        del response
        self.ran.set()
