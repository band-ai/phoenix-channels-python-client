from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Self

import pytest
from websockets.frames import CloseCode
from websockets.protocol import State

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import SocketError
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from tests.fake_server import FakePhoenixServer
from tests.scheduling import TaskLifecycleGate
from tests.support import (
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    LEAVE_TIMEOUT_S,
    STOP_REASON,
    STOPS_ON_NORMAL_CLOSE,
    TERMINAL_REFUSAL,
    TOPIC,
    UNPARSEABLE_FRAME,
    ReconnectCounter,
    crash_with_rejoin_in_flight,
    deliver,
    derive_policy,
    each_refusal,
    join_channel,
    make_client,
    reconnect,
    refused,
    wait_for_condition,
    wait_forever,
)

# Long enough for a shutdown that isn't held to finish against the local server.
SETTLE_S = 0.2

# The backoff after a close, long enough that even its jitter's lower half
# outlasts the test, so only a shutdown can end the wait.
HELD_BACKOFF_S = 4 * ASYNC_TIMEOUT_S
HELD_BACKOFF = derive_policy(
    FAST_RECONNECT, base_delay_s=HELD_BACKOFF_S, max_delay_s=HELD_BACKOFF_S
)

# More refusals than any test reaches, so only the client's own stop ends them.
REFUSE_EVERY_HANDSHAKE = 1_000_000


class HeldCallback:
    """A topic callback that, once cancelled, holds its unwinding until released.

    shutdown() cancels topic callbacks, so this holds shutdown() open on demand.
    Leaving the ``with`` block releases it, even when the test fails.
    """

    def __init__(self) -> None:
        self.running = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release.set()

    async def __call__(self, message: Message) -> None:
        del message
        self.running.set()
        try:
            await wait_forever()
        except asyncio.CancelledError:
            self.cancelled.set()
            await self.release.wait()
            raise


async def run_held_callback(
    server: FakePhoenixServer, client: Client, callback: HeldCallback
) -> None:
    await join_channel(client, TOPIC, callback)
    await deliver(server, client)
    await asyncio.wait_for(callback.running.wait(), ASYNC_TIMEOUT_S)


async def shut_down_during_a_held_reconnect(
    server: FakePhoenixServer, client: Client, callback: HeldCallback
) -> tuple[asyncio.Task[None], asyncio.Task[None]]:
    """Hold a reconnect's handshake, then start shutdown; return (run, stop)."""
    await run_held_callback(server, client, callback)
    server.handshake_gate.clear()
    await server.close_all_clients(code=CloseCode.SERVICE_RESTART)
    await asyncio.wait_for(server.handshake_pending.wait(), ASYNC_TIMEOUT_S)

    run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
    stop = asyncio.create_task(client.shutdown(STOP_REASON))
    assert await wait_for_condition(lambda: client.state is ClientState.SHUTTING_DOWN)
    return run, stop


async def test_reconnect_handshake_racing_shutdown_leaks_no_connection(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    async with client:
        with HeldCallback() as held:
            run, stop = await shut_down_during_a_held_reconnect(
                phoenix_server, client, held
            )
            phoenix_server.handshake_gate.set()
            await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
            assert not run.done()
            assert not stop.done()

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert client.connection is None
        assert await wait_for_condition(
            lambda: not phoenix_server.list_client_connections()
        )


@each_refusal
async def test_a_connect_failure_during_shutdown_closes_cleanly(
    phoenix_server: FakePhoenixServer, refusal: HTTPStatus
) -> None:
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    async with client:
        with HeldCallback() as held:
            run, stop = await shut_down_during_a_held_reconnect(
                phoenix_server, client, held
            )
            with phoenix_server.refusing(1, refusal):
                phoenix_server.handshake_gate.set()
                await asyncio.wait_for(held.cancelled.wait(), ASYNC_TIMEOUT_S)
                assert not run.done()
                assert not stop.done()

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert client.state is ClientState.CLOSED


async def test_a_refusal_is_ignored_from_the_moment_shutdown_is_called(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        stop = asyncio.create_task(client.shutdown(STOP_REASON))
        # shutdown() has run to its first await; the task it created has not started.
        await asyncio.sleep(0)

        assert client.state is ClientState.SHUTTING_DOWN
        assert client._retry_after_connect_failure(refused(TERMINAL_REFUSAL)) is None
        assert client.terminal_error is None

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)


async def test_shutdown_during_the_reconnect_backoff_makes_no_new_connection(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=HELD_BACKOFF) as client:
        run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
        attempts = phoenix_server.get_connection_attempts(FakePhoenixServer.SOCKET_PATH)
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)
        # The supervisor enters the backoff wait straight after this transition.
        assert await wait_for_condition(
            lambda: client.state is ClientState.RECONNECTING
        )

        await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert (
            phoenix_server.get_connection_attempts(FakePhoenixServer.SOCKET_PATH)
            == attempts
        )


async def test_shutdown_during_recovery_leaves_no_tasks(
    protocol: ProtocolVersion,
) -> None:
    # Proactor replaces the listener's accept task per connection: own it too.
    before = asyncio.all_tasks()
    async with FakePhoenixServer(protocol) as server:
        async with make_client(server, reconnect_policy=FAST_RECONNECT) as client:
            await join_channel(client, TOPIC)
            await crash_with_rejoin_in_flight(server, client)
        assert client.connection is None
        assert await wait_for_condition(lambda: not server.list_client_connections())

    assert await wait_for_condition(lambda: not asyncio.all_tasks() - before), [
        repr(task) for task in asyncio.all_tasks() - before
    ]


async def test_a_slow_on_topic_lost_does_not_outlive_shutdown(
    phoenix_server: FakePhoenixServer,
) -> None:
    running = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow_on_topic_lost(topic: str, error: Exception) -> None:
        del topic, error
        running.set()
        try:
            await wait_forever()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    client = make_client(phoenix_server, on_topic_lost=slow_on_topic_lost)
    async with client:
        await join_channel(client, TOPIC)
        await phoenix_server.close_channel(TOPIC)
        await asyncio.wait_for(running.wait(), ASYNC_TIMEOUT_S)

        await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)

        assert cancelled.is_set()


async def test_shutdown_during_initial_connect_fails_the_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    phoenix_server.handshake_gate.clear()
    enter = asyncio.create_task(client.__aenter__())
    await asyncio.wait_for(phoenix_server.handshake_pending.wait(), ASYNC_TIMEOUT_S)

    await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)
    with pytest.raises(SocketError):
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
        assert client.state is ClientState.CLOSED


async def test_reentering_during_a_shutdown_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, auto_reconnect=False)
    async with client:
        with HeldCallback() as held:
            await run_held_callback(phoenix_server, client, held)
            await phoenix_server.close_all_clients(code=CloseCode.NORMAL_CLOSURE)
            assert await wait_for_condition(lambda: client.state is ClientState.CLOSED)

            gate = TaskLifecycleGate(Client._shutdown.__code__, hold_start=True)
            with gate.installed():
                stop = asyncio.create_task(client.shutdown(STOP_REASON))
                await asyncio.wait_for(gate.started.wait(), ASYNC_TIMEOUT_S)
                with pytest.raises(SocketError, match="already running"):
                    await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

        await asyncio.wait_for(stop, ASYNC_TIMEOUT_S)


async def test_shutdown_from_a_topic_callback_finishes_without_a_cleanup_cycle(
    phoenix_server: FakePhoenixServer,
) -> None:
    callback_finished = asyncio.Event()

    async def stop_from_callback(message: Message) -> None:
        del message
        try:
            await client.shutdown(STOP_REASON)
        finally:
            callback_finished.set()

    client = make_client(phoenix_server, leave_timeout_s=LEAVE_TIMEOUT_S)
    async with client:
        await join_channel(client, TOPIC, stop_from_callback)
        await deliver(phoenix_server, client)

        await asyncio.wait_for(callback_finished.wait(), ASYNC_TIMEOUT_S)
        assert await wait_for_condition(lambda: client.state is ClientState.CLOSED)
        assert client.connection is None


async def test_forced_close_racing_shutdown_does_not_leak_into_the_next_session(
    phoenix_server: FakePhoenixServer,
) -> None:
    # A leaked forced-close flag would reconnect after the normal close below.
    client = make_client(phoenix_server, reconnect_policy=STOPS_ON_NORMAL_CLOSE)
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


async def test_reentering_does_not_report_a_reconnect(
    phoenix_server: FakePhoenixServer,
) -> None:
    reconnects = ReconnectCounter()
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_reconnect=reconnects
    )
    async with client:
        pass

    async with client:
        assert reconnects.count == 0

        await reconnect(phoenix_server, client)

        # on_reconnect runs after the rejoin, which follows the new connection.
        assert await wait_for_condition(lambda: reconnects.count == 1)


async def test_shutdown_during_a_close_still_closes_the_socket(
    phoenix_server: FakePhoenixServer,
) -> None:
    # Without a heartbeat task to stop first, the supervisor goes straight to close.
    async with make_client(phoenix_server, heartbeat_interval_s=None) as client:
        socket = client.connection
        assert socket is not None

        # The server must stay unresponsive until the assert, or the closing
        # handshake completes on its own.
        with phoenix_server.unresponsive():
            await phoenix_server.send_raw(UNPARSEABLE_FRAME)
            assert await wait_for_condition(lambda: client.connection is None)

            await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)

            assert socket.state is State.CLOSED


async def a_handshake_was_refused(server: FakePhoenixServer) -> None:
    assert await wait_for_condition(lambda: server.refused_handshakes >= 1)


async def expire_on_first_refusal(
    server: FakePhoenixServer, client: Client, deadline: asyncio.Timeout
) -> asyncio.Task[None] | None:
    """Expire ``deadline`` once a handshake was refused; return entry's supervisor."""
    await a_handshake_was_refused(server)
    supervisor = client.supervisor_task
    deadline.reschedule(asyncio.get_running_loop().time())
    return supervisor


async def time_out_entry_on_first_refusal(
    server: FakePhoenixServer, client: Client
) -> asyncio.Task[None] | None:
    """Enter under a deadline that expires on the first refused handshake.

    Returns the supervisor entry started; fails unless the deadline ended entry.
    The deadline starts at ``ASYNC_TIMEOUT_S``, so a missing refusal fails here
    instead of leaving entry waiting forever.
    """
    expiry: asyncio.Task[asyncio.Task[None] | None] | None = None
    try:
        async with asyncio.timeout(ASYNC_TIMEOUT_S) as deadline:
            expiry = asyncio.create_task(
                expire_on_first_refusal(server, client, deadline)
            )
            await client.__aenter__()
    except TimeoutError:
        assert expiry is not None
        return expiry.result()
    finally:
        if expiry is not None:
            expiry.cancel()
    pytest.fail("entry ended before its deadline")


async def test_a_timed_out_entry_stops_connecting(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    try:
        with phoenix_server.refusing(REFUSE_EVERY_HANDSHAKE):
            supervisor = await time_out_entry_on_first_refusal(phoenix_server, client)

            assert supervisor is not None
            assert supervisor.done()
            assert client.state is ClientState.CLOSED

        async with client:  # the stopped client enters again once the server accepts
            assert client.connection is not None
    finally:
        await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)


async def test_a_second_cancel_during_entry_cleanup_still_stops_connecting(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)
    try:
        with phoenix_server.refusing(REFUSE_EVERY_HANDSHAKE):
            entering = asyncio.create_task(client.__aenter__())
            await a_handshake_was_refused(phoenix_server)
            supervisor = client.supervisor_task
            assert supervisor is not None

            entering.cancel()
            await asyncio.sleep(0)  # the cancelled entry starts its cleanup shutdown
            assert client.state is ClientState.SHUTTING_DOWN

            entering.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(entering, ASYNC_TIMEOUT_S)

            # Joins the cleanup the entry started; it was not abandoned.
            await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)
            assert supervisor.done()
            assert client.state is ClientState.CLOSED
            assert client.connection is None
    finally:
        await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)


async def test_closing_a_pending_entry_is_silent(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    phoenix_server.handshake_gate.clear()
    entry = client.__aenter__()
    try:
        entry.send(None)  # suspends at the initial-connection await
        entry.close()  # raises if entry awaits while handling GeneratorExit
    finally:
        phoenix_server.handshake_gate.set()
        await asyncio.wait_for(client.shutdown(STOP_REASON), ASYNC_TIMEOUT_S)

    # The closed entry no longer awaits its outcome; retrieve it here.
    initial_connection = client.initial_connection_future
    assert initial_connection is not None
    assert isinstance(initial_connection.exception(), SocketError)
