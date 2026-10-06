from __future__ import annotations

import asyncio
from collections.abc import Callable

from tests.test_v2_protocol.conftest import FakePhoenixServer, phoenix_server

__all__ = [
    "ASYNC_TIMEOUT_S",
    "FakePhoenixServer",
    "phoenix_server",
    "wait_for_condition",
]

# Upper bound for awaiting a task or event that should finish promptly; without
# pytest-timeout, this turns a hang into a failure.
ASYNC_TIMEOUT_S = 2.0


async def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 1.0,
    interval: float = 0.05,
) -> bool:
    """
    Poll for a condition to become true, with timeout.

    Args:
        condition: A callable that returns True when the condition is met.
        timeout: Maximum time to wait in seconds.
        interval: Time between polls in seconds.

    Returns:
        True if condition was met, False if timeout occurred.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False
