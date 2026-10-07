"""Constants and helpers shared by the tests. Fixtures live in conftest.py."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)

from tests.fake_server import FakePhoenixServer

API_KEY = "test_key"

TOPIC = FakePhoenixServer.TOPIC
OTHER_TOPIC = FakePhoenixServer.OTHER_TOPIC
REJECTED_TOPIC = FakePhoenixServer.REJECTED_TOPIC

# A custom (non-phx_) event for tests that only need some server push.
EVENT = "test_event"

# Not a Phoenix frame in either protocol, so the client can't parse it.
UNPARSEABLE_FRAME = "not a phoenix frame"

# Tests that stop the client don't care why; this is only logged.
STOP_REASON = "test stop"

# Upper bound for awaiting a task or event that should finish promptly; without
# pytest-timeout, this turns a hang into a failure.
ASYNC_TIMEOUT_S = 2.0

# A busy topic callback holds back its leave reply, so don't wait long for it.
LEAVE_TIMEOUT_S = 0.05

# Long enough for a local join reply, short enough to time out an unanswered one.
JOIN_TIMEOUT_S = 0.1

# How often a condition is polled; far below every timeout the tests use.
POLL_INTERVAL_S = 0.01

T = TypeVar("T")

# Every reconnect delay and cooldown is near zero, so no test waits on backoff.
FAST_RECONNECT = ReconnectPolicy(
    base_delay_s=0.01,
    max_delay_s=0.05,
    stable_reset_s=0.1,
    service_restart_min_delay_s=0.01,
    service_restart_max_delay_s=0.02,
    try_again_later_min_delay_s=0.03,
    try_again_later_max_delay_s=0.05,
    rapid_disconnect_uptime_s=0.05,
    rapid_window_s=1.0,
    rapid_first_min_delay_s=0.01,
    rapid_second_min_delay_s=0.02,
    rapid_cooldown_base_s=0.03,
    rapid_cooldown_step_s=0.01,
    rapid_cooldown_max_s=0.05,
    rapid_hold_down_jitter_low_ratio=0.5,
)

# Runs the test once per protocol version, through the `protocol` fixture.
each_protocol = pytest.mark.parametrize(
    "protocol",
    list(PhoenixChannelsProtocolVersion),
    ids=lambda version: f"v{version.value}",
)


def make_client(
    server: FakePhoenixServer,
    path: str = FakePhoenixServer.SOCKET_PATH,
    **options: Any,
) -> PHXChannelsClient:
    options.setdefault("api_key", API_KEY)
    return PHXChannelsClient(
        server.url_for(path), protocol_version=server.protocol, **options
    )


async def wait_for_condition(
    condition: Callable[[], bool], timeout: float = ASYNC_TIMEOUT_S
) -> bool:
    """Poll ``condition`` until it is true; False if ``timeout`` passes first."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(POLL_INTERVAL_S)
    return False


async def wait_forever() -> None:
    """A stand-in for work that only ends when cancelled."""
    await asyncio.Event().wait()


async def deliver(
    server: FakePhoenixServer,
    client: PHXChannelsClient,
    topic: str = TOPIC,
    payload: Mapping[str, object] | None = None,
    event: str = EVENT,
) -> None:
    """Push one server event on the client's current join of ``topic``."""
    join_ref = client.get_current_subscriptions()[topic].join_ref
    await server.simulate_server_event(topic, event, payload or {}, join_ref=join_ref)


async def expect_message(received: asyncio.Queue[T]) -> T:
    return await asyncio.wait_for(received.get(), ASYNC_TIMEOUT_S)


async def reconnect_after(
    client: PHXChannelsClient, drop_connection: Awaitable[None]
) -> None:
    """Run ``drop_connection`` and wait until the client holds a new connection."""
    generation = client._conn_generation
    await drop_connection
    assert await wait_for_condition(lambda: client._conn_generation > generation)


async def reconnect(
    server: FakePhoenixServer,
    client: PHXChannelsClient,
    code: int = CloseCode.SERVICE_RESTART,
) -> None:
    """Close every connection with ``code`` and wait for the client to reconnect."""
    await reconnect_after(client, server.close_all_clients(code=code))


def rejoin_settled(client: PHXChannelsClient, topic: str = TOPIC) -> Callable[[], bool]:
    """True once the topic's join on the current connection has an outcome, or
    the topic is gone."""

    def settled() -> bool:
        subscription = client.get_current_subscriptions().get(topic)
        return subscription is None or (
            subscription.conn_generation == client._conn_generation
            and subscription.current_join_ready.done()
        )

    return settled
