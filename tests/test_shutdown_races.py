from __future__ import annotations

import asyncio

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import ChannelMessage

from tests.fake_server import FakePhoenixServer
from tests.support import (
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    LEAVE_TIMEOUT_S,
    STOP_REASON,
    TOPIC,
    deliver,
    make_client,
    wait_for_condition,
)

# Long enough for a shutdown that isn't held to finish against the local server.
SETTLE_S = 0.2


class HeldCallback:
    """A topic callback that, once cancelled, holds its unwinding until released.

    shutdown() cancels topic callbacks, so this holds shutdown() open on demand.
    Leaving the ``with`` block releases it, even when the test fails.
    """

    def __init__(self) -> None:
        self.running = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    def __enter__(self) -> HeldCallback:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release.set()

    async def __call__(self, message: ChannelMessage) -> None:
        self.running.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            await self.release.wait()
            raise


async def run_held_callback(
    server: FakePhoenixServer, client: PHXChannelsClient, callback: HeldCallback
) -> None:
    await client.subscribe_to_topic(TOPIC, callback)
    await deliver(server, client)
    await asyncio.wait_for(callback.running.wait(), ASYNC_TIMEOUT_S)


async def test_reconnect_landing_during_shutdown_is_closed(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    async with client:
        with HeldCallback() as held:
            await run_held_callback(phoenix_server, client, held)
            phoenix_server.handshake_gate.clear()
            await phoenix_server.close_all_clients(code=CloseCode.SERVICE_RESTART)
            await asyncio.wait_for(
                phoenix_server.handshake_pending.wait(), ASYNC_TIMEOUT_S
            )

            run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
            stop = asyncio.create_task(client.shutdown(STOP_REASON))
            assert await wait_for_condition(
                lambda: client._state is ClientState.SHUTTING_DOWN
            )
            phoenix_server.handshake_gate.set()
            # The held reconnect lands only now, after the shutdown began.
            assert await wait_for_condition(lambda: client.connection is not None)

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

    await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)
    with pytest.raises(PHXConnectionError):
        await asyncio.wait_for(enter, ASYNC_TIMEOUT_S)


async def test_concurrent_shutdowns_wait_for_the_same_shutdown(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, leave_timeout_s=LEAVE_TIMEOUT_S)
    async with client:
        with HeldCallback() as held:
            await run_held_callback(phoenix_server, client, held)
            stops = [
                asyncio.create_task(client.shutdown(STOP_REASON)),
                asyncio.create_task(client.shutdown(STOP_REASON)),
            ]
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
            finished, _ = await asyncio.wait(stops, timeout=SETTLE_S)
            assert not finished

        for stop in stops:
            await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)
        assert client.connection is None

    async with client:  # re-entering works once every shutdown has finished
        assert client.connection is not None


async def test_cancelling_one_caller_does_not_abort_the_shared_shutdown(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, leave_timeout_s=LEAVE_TIMEOUT_S)
    async with client:
        with HeldCallback() as held:
            await run_held_callback(phoenix_server, client, held)
            abandoned = asyncio.create_task(client.shutdown(STOP_REASON))
            waiting = asyncio.create_task(client.shutdown(STOP_REASON))
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)

            abandoned.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(abandoned, ASYNC_TIMEOUT_S)

        await asyncio.wait_for(waiting, ASYNC_TIMEOUT_S)
        assert client.connection is None
        assert client._state is ClientState.CLOSED


async def test_reentering_during_a_shutdown_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, auto_reconnect=False)
    async with client:
        with HeldCallback() as held:
            await run_held_callback(phoenix_server, client, held)
            await phoenix_server.close_all_clients(code=CloseCode.NORMAL_CLOSURE)
            assert await wait_for_condition(lambda: client._state is ClientState.CLOSED)

            stop = asyncio.create_task(client.shutdown(STOP_REASON))
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
            with pytest.raises(PHXConnectionError):
                await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)


async def test_shutdown_from_a_topic_callback_cancels_the_callback(
    phoenix_server: FakePhoenixServer,
) -> None:
    callback_cancelled = asyncio.Event()

    async def stop_from_callback(message: ChannelMessage) -> None:
        try:
            await client.shutdown(STOP_REASON)
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    client = make_client(phoenix_server, leave_timeout_s=LEAVE_TIMEOUT_S)
    async with client:
        await client.subscribe_to_topic(TOPIC, stop_from_callback)
        await deliver(phoenix_server, client)

        await asyncio.wait_for(callback_cancelled.wait(), ASYNC_TIMEOUT_S)
        assert await wait_for_condition(lambda: client._state is ClientState.CLOSED)
        assert client.connection is None


async def test_forced_close_racing_shutdown_does_not_leak_into_the_next_session(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    async with client:
        await asyncio.wait_for(
            asyncio.gather(
                client.close_connection(STOP_REASON), client.shutdown(STOP_REASON)
            ),
            ASYNC_TIMEOUT_S,
        )

    async with client:
        run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
        attempts = phoenix_server.get_connection_attempts(FakePhoenixServer.SOCKET_PATH)
        await phoenix_server.close_all_clients(code=CloseCode.NORMAL_CLOSURE)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert (
            phoenix_server.get_connection_attempts(FakePhoenixServer.SOCKET_PATH)
            == attempts
        )
