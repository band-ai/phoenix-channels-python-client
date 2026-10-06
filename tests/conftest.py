from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from phoenix_channels_python_client.client import PHXChannelsClient
from tests.test_v2_protocol.conftest import FakePhoenixServer, phoenix_server

__all__ = [
    "API_KEY",
    "ASYNC_TIMEOUT_S",
    "FakePhoenixServer",
    "make_client",
    "phoenix_server",
    "wait_for_condition",
]

API_KEY = "test_key"

# Upper bound for awaiting a task or event that should finish promptly; without
# pytest-timeout, this turns a hang into a failure.
ASYNC_TIMEOUT_S = 2.0


def make_client(server: FakePhoenixServer, **options: Any) -> PHXChannelsClient:
    return PHXChannelsClient(server.url, api_key=API_KEY, **options)


async def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = ASYNC_TIMEOUT_S,
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
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False
