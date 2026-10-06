from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import ChannelMessage

from .conftest import ASYNC_TIMEOUT_S, FakePhoenixServer, wait_for_condition

TOPIC = "test-topic"

# A held callback never processes the leave reply, so don't wait long for it.
LEAVE_TIMEOUT_S = 0.05

# Long enough for a shutdown that isn't held to finish against the local server.
SETTLE_S = 0.2

# Reconnect after a service restart without waiting on backoff.
FAST_RECONNECT = ReconnectPolicy(
    base_delay_s=0.01,
    service_restart_min_delay_s=0.01,
    service_restart_max_delay_s=0.01,
    rapid_first_min_delay_s=0.0,
)


class HeldCallback:
    """A topic callback that, once cancelled, holds its unwinding until released.

    shutdown() cancels topic callbacks, so this holds shutdown() open on demand.
    """

    def __init__(self) -> None:
        self.running = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, message: ChannelMessage) -> None:
        self.running.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            await self.release.wait()
            raise


def make_client(server: FakePhoenixServer, **options: Any) -> PHXChannelsClient:
    return PHXChannelsClient(
        server.url, api_key="test_key", leave_timeout_s=LEAVE_TIMEOUT_S, **options
    )


async def run_callback(
    server: FakePhoenixServer, client: PHXChannelsClient, callback: HeldCallback
) -> None:
    await client.subscribe_to_topic(TOPIC, callback)
    join_ref = client.get_current_subscriptions()[TOPIC].join_ref
    await server.simulate_server_event(TOPIC, "hold", {}, join_ref=join_ref)
    await asyncio.wait_for(callback.running.wait(), ASYNC_TIMEOUT_S)


async def test_reconnect_landing_during_shutdown_is_closed(
    phoenix_server: FakePhoenixServer,
) -> None:
    path = urlparse(phoenix_server.url).path
    held = HeldCallback()
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    async with client:
        try:
            await run_callback(phoenix_server, client, held)
            phoenix_server.handshake_gate.clear()
            await phoenix_server.close_all_clients(code=1012)
            await asyncio.wait_for(
                phoenix_server.handshake_pending.wait(), ASYNC_TIMEOUT_S
            )

            run = asyncio.create_task(client.run_forever())
            stop = asyncio.create_task(client.shutdown("host stop"))
            assert await wait_for_condition(
                lambda: client._state is ClientState.SHUTTING_DOWN
            )
            phoenix_server.handshake_gate.set()
            assert await wait_for_condition(
                lambda: phoenix_server.get_connection_attempts(path) == 2
            )
        finally:
            held.release.set()

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert client.connection is None
        assert await wait_for_condition(
            lambda: not phoenix_server.list_client_connections()
        )


async def test_shutdown_during_initial_connect_fails_the_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    phoenix_server.handshake_gate.clear()
    enter = asyncio.create_task(client.__aenter__())
    await asyncio.wait_for(phoenix_server.handshake_pending.wait(), ASYNC_TIMEOUT_S)

    await asyncio.wait_for(client.shutdown("host stop"), ASYNC_TIMEOUT_S)
    with pytest.raises(PHXConnectionError):
        await asyncio.wait_for(enter, ASYNC_TIMEOUT_S)


async def test_concurrent_shutdowns_wait_for_the_same_shutdown(
    phoenix_server: FakePhoenixServer,
) -> None:
    held = HeldCallback()
    client = make_client(phoenix_server)
    async with client:
        try:
            await run_callback(phoenix_server, client, held)
            stops = [
                asyncio.create_task(client.shutdown("first stop")),
                asyncio.create_task(client.shutdown("second stop")),
            ]
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
            finished, _ = await asyncio.wait(stops, timeout=SETTLE_S)
            assert not finished
        finally:
            held.release.set()

        for stop in stops:
            await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)
        assert client.connection is None

    async with client:
        await client.subscribe_to_topic(TOPIC)
        for _ in range(3):
            await asyncio.sleep(0)
        assert client.connection is not None
        assert client._state is ClientState.CONNECTED


async def test_reentering_during_a_shutdown_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    held = HeldCallback()
    client = make_client(phoenix_server, auto_reconnect=False)
    async with client:
        try:
            await run_callback(phoenix_server, client, held)
            await phoenix_server.close_all_clients(code=1000)
            assert await wait_for_condition(lambda: client._state is ClientState.CLOSED)

            stop = asyncio.create_task(client.shutdown("host stop"))
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
            with pytest.raises(PHXConnectionError):
                await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)
        finally:
            held.release.set()

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)


async def test_shutdown_from_a_topic_callback_cancels_the_callback(
    phoenix_server: FakePhoenixServer,
) -> None:
    callback_cancelled = asyncio.Event()

    async def stop_from_callback(message: ChannelMessage) -> None:
        try:
            await client.shutdown("callback stop")
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    client = make_client(phoenix_server)
    async with client:
        await client.subscribe_to_topic(TOPIC, stop_from_callback)
        join_ref = client.get_current_subscriptions()[TOPIC].join_ref
        await phoenix_server.simulate_server_event(TOPIC, "stop", {}, join_ref=join_ref)

        await asyncio.wait_for(callback_cancelled.wait(), ASYNC_TIMEOUT_S)
        await asyncio.wait_for(client.shutdown("host stop"), ASYNC_TIMEOUT_S)
        assert client.connection is None


async def test_forced_close_racing_shutdown_does_not_leak_into_the_next_session(
    phoenix_server: FakePhoenixServer,
) -> None:
    path = urlparse(phoenix_server.url).path
    client = make_client(phoenix_server)
    async with client:
        await asyncio.wait_for(
            asyncio.gather(
                client.close_connection("forced"), client.shutdown("host stop")
            ),
            ASYNC_TIMEOUT_S,
        )

    async with client:
        run = asyncio.create_task(client.run_forever())
        attempts = phoenix_server.get_connection_attempts(path)
        await phoenix_server.close_all_clients(code=1000)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert phoenix_server.get_connection_attempts(path) == attempts
