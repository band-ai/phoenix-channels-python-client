from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any

import looptime
import pytest
from websockets import ClientConnection
from websockets.protocol import State

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage, PHXEvent
from phoenix_channels_python_client.utils import make_message
from tests.clock import CLOCK_RESOLUTION, Clock
from tests.fake_server import SYSTEM_TOPIC, FakePhoenixServer, WireEvent
from tests.support import (
    STOP_REASON,
    STOPS_ON_NORMAL_CLOSE,
    TOPIC,
    CallbackError,
    deliver,
    each_protocol,
    expect_message,
    make_client,
    wait_forever,
)

INTERVAL = 4.0
THRESHOLD = 11.0
WINDOWS = 4
WRONG_REF = "wrong-ref"


async def test_unmarked_tests_keep_the_default_loop() -> None:
    loop = asyncio.get_running_loop()
    assert not isinstance(loop, looptime.LoopTimeEventLoop)
    if sys.platform == "win32":
        assert isinstance(loop, asyncio.ProactorEventLoop)


@pytest.mark.parametrize("threshold", [0, -1, float("inf"), float("nan")])
@pytest.mark.parametrize("interval", [None, INTERVAL])
def test_invalid_threshold_is_rejected(
    threshold: float, interval: float | None
) -> None:
    with pytest.raises(ValueError, match="heartbeat_dead_threshold_s"):
        PHXChannelsClient(
            "ws://localhost",
            "key",
            heartbeat_interval_s=interval,
            heartbeat_dead_threshold_s=threshold,
        )


@pytest.mark.parametrize(
    "interval", [THRESHOLD, THRESHOLD + 1, float("inf"), float("nan")]
)
def test_enforcement_requires_a_smaller_finite_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="finite heartbeat interval"):
        PHXChannelsClient(
            "ws://localhost",
            "key",
            heartbeat_interval_s=interval,
            heartbeat_dead_threshold_s=THRESHOLD,
        )


@asynccontextmanager
async def monitored(
    peer: FakePhoenixServer, **options: Any
) -> AsyncIterator[PHXChannelsClient]:
    options.setdefault("auto_reconnect", False)
    async with make_client(
        peer,
        heartbeat_interval_s=INTERVAL,
        heartbeat_dead_threshold_s=THRESHOLD,
        **options,
    ) as client:
        yield client
    assert client._heartbeat_timeout is None
    assert client._heartbeat_task is None
    assert client.connection is None


def timer(client: PHXChannelsClient) -> asyncio.Timeout:
    active = client._heartbeat_timeout
    assert active is not None
    return active


async def verify_delivery(
    peer: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    payload = {"message": "heartbeat recovery"}
    await deliver(peer, client, payload=payload)
    assert (await expect_message(received)).payload == payload


@pytest.mark.clock
@each_protocol
@pytest.mark.parametrize("throwing", [False, True])
@pytest.mark.parametrize(
    ("interval", "threshold"), [(INTERVAL, THRESHOLD), (30.0, 60.0)]
)
async def test_healthy_replies_renew_before_callbacks(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    *,
    throwing: bool,
    interval: float,
    threshold: float,
) -> None:
    acks: asyncio.Queue[float | None] = asyncio.Queue()

    def acknowledged() -> None:
        acks.put_nowait(timer(client).when())
        if throwing:
            raise CallbackError

    async with make_client(
        clocked_peer,
        heartbeat_interval_s=interval,
        heartbeat_dead_threshold_s=threshold,
        on_heartbeat_ack=acknowledged,
    ) as client:
        for _ in range(WINDOWS):
            await clock.advance(interval)
            await clocked_peer.heartbeats.get()
            assert await acks.get() == clock.loop.time() + threshold
        assert client.connection is not None
        assert client.connection.state is State.OPEN


@pytest.mark.clock
@each_protocol
async def test_delayed_reply_keeps_its_reference_across_send_ticks(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
) -> None:
    clocked_peer.answer_heartbeats = False
    acknowledged = asyncio.Event()
    async with monitored(clocked_peer, on_heartbeat_ack=acknowledged.set) as client:
        await clock.advance(INTERVAL)
        connection, frame = await clocked_peer.heartbeats.get()
        await clock.advance(INTERVAL)
        assert client._pending_heartbeat_ref == frame.ref
        await clocked_peer.reply_heartbeat(connection, frame)
        await acknowledged.wait()
        assert clocked_peer.heartbeats.empty()
        assert timer(client).when() == 2 * INTERVAL + THRESHOLD
        await clock.advance(INTERVAL)
        _, next_frame = await clocked_peer.heartbeats.get()
        assert next_frame.ref != frame.ref


@pytest.mark.clock
@each_protocol
async def test_pong_and_topic_traffic_do_not_renew_and_recovery_delivers(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
) -> None:
    peer = clocked_peer
    peer.answer_heartbeats = False
    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    reconnected = asyncio.Event()
    disconnected = asyncio.Event()

    async def on_disconnect(error: Exception | None) -> None:
        del error
        disconnected.set()

    async def on_reconnect() -> None:
        reconnected.set()

    async with monitored(
        peer,
        auto_reconnect=True,
        reconnect_policy=STOPS_ON_NORMAL_CLOSE,
        on_reconnect=on_reconnect,
        on_disconnect=on_disconnect,
    ) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        old_task = client._heartbeat_task
        old_socket = client.connection
        await clock.advance(INTERVAL)
        connection, stale = await peer.heartbeats.get()
        pong = await connection.ping()
        await pong
        await verify_delivery(peer, client, received)
        assert timer(client).when() == THRESHOLD
        await clock.advance(THRESHOLD - INTERVAL - CLOCK_RESOLUTION)
        assert old_socket is not None
        assert old_socket.state is State.OPEN
        await clock.advance(CLOCK_RESOLUTION)
        await connection.wait_closed()
        assert clock.loop.time() == THRESHOLD
        assert old_task is not None
        await asyncio.gather(old_task, return_exceptions=True)
        assert old_task.done()
        peer.answer_heartbeats = True
        await disconnected.wait()
        # The existing backoff is smaller than an interval; only time advances here.
        await clock.advance(INTERVAL)
        await reconnected.wait()
        assert client._heartbeat_task is not old_task
        await old_socket.wait_closed()
        fresh_deadline = timer(client).when()
        assert fresh_deadline is not None
        assert fresh_deadline == clock.loop.time() + THRESHOLD
        peer.answer_heartbeats = False
        await clock.advance(INTERVAL)
        new_connection, pending = await peer.heartbeats.get()
        assert pending.ref != stale.ref
        await peer.simulate_server_event(
            SYSTEM_TOPIC, WireEvent.REPLY, {}, ref=stale.ref
        )
        await verify_delivery(peer, client, received)
        assert timer(client).when() == fresh_deadline
        assert client._pending_heartbeat_ref == pending.ref
        assert client.is_topic_joined(TOPIC)
        await peer.reply_heartbeat(new_connection, pending)
        await verify_delivery(peer, client, received)
        assert timer(client).when() == clock.loop.time() + THRESHOLD


@pytest.mark.clock
@each_protocol
async def test_wrong_and_duplicate_replies_do_not_renew(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
) -> None:
    peer = clocked_peer
    peer.answer_heartbeats = False
    acks: asyncio.Queue[None] = asyncio.Queue()
    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    async with monitored(
        peer, on_heartbeat_ack=lambda: acks.put_nowait(None)
    ) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        await clock.advance(INTERVAL)
        connection, frame = await peer.heartbeats.get()
        await peer.simulate_server_event(
            SYSTEM_TOPIC, WireEvent.REPLY, {}, ref=WRONG_REF
        )
        await verify_delivery(peer, client, received)
        assert timer(client).when() == THRESHOLD
        await peer.reply_heartbeat(connection, frame)
        await acks.get()
        deadline = timer(client).when()
        await clock.advance(INTERVAL)
        await peer.heartbeats.get()
        await peer.reply_heartbeat(connection, frame)
        await verify_delivery(peer, client, received)
        assert acks.empty()
        assert timer(client).when() == deadline
        assert deadline is not None
        await clock.advance(deadline - clock.loop.time())
        await connection.wait_closed()
        await client.run_forever(install_signal_handlers=False)
        assert client.connection is None


@pytest.mark.clock
@each_protocol
async def test_warn_only_replaces_references_without_closing(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    peer = clocked_peer
    peer.answer_heartbeats = False
    async with make_client(peer, heartbeat_interval_s=INTERVAL) as client:
        references = []
        for _ in range(WINDOWS):
            await clock.advance(INTERVAL)
            _, frame = await peer.heartbeats.get()
            references.append(frame.ref)
        assert len(set(references)) == WINDOWS
        assert "server may be unresponsive" in caplog.text
        assert client.connection is not None
        assert client.connection.state is State.OPEN


@pytest.mark.clock
async def test_disabling_heartbeats_disables_enforcement(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
) -> None:
    async with make_client(
        clocked_peer, heartbeat_interval_s=None, heartbeat_dead_threshold_s=THRESHOLD
    ) as client:
        await clock.advance(THRESHOLD * WINDOWS)
        assert client._heartbeat_task is None
        assert client.connection is not None
        assert client.connection.state is State.OPEN
        assert clocked_peer.heartbeats.empty()


@dataclass
class HeldOperation:
    entered: asyncio.Event
    cancelled: asyncio.Event
    release: asyncio.Event


@contextmanager
def hold_send(
    client: PHXChannelsClient, monkeypatch: pytest.MonkeyPatch
) -> Iterator[HeldOperation]:
    held = HeldOperation(asyncio.Event(), asyncio.Event(), asyncio.Event())
    send = client._protocol_handler.send_message

    async def sending(connection: ClientConnection, message: ChannelMessage) -> None:
        await send(connection, message)
        held.entered.set()
        try:
            await wait_forever()
        except asyncio.CancelledError:
            held.cancelled.set()
            await held.release.wait()
            raise

    with monkeypatch.context() as patch:
        patch.setattr(client._protocol_handler, "send_message", sending)
        try:
            yield held
        finally:
            held.release.set()


@contextmanager
def hold_close(
    client: PHXChannelsClient, monkeypatch: pytest.MonkeyPatch
) -> Iterator[HeldOperation]:
    socket = client.connection
    assert socket is not None
    held = HeldOperation(asyncio.Event(), asyncio.Event(), asyncio.Event())
    close = socket.close

    async def closing(code: int = 1000, reason: str = "") -> None:
        held.entered.set()
        try:
            await close(code=code, reason=reason)
            await wait_forever()
        except asyncio.CancelledError:
            held.cancelled.set()
            await held.release.wait()
            raise

    with monkeypatch.context() as patch:
        patch.setattr(socket, "close", closing)
        try:
            yield held
        finally:
            held.release.set()


@pytest.mark.clock
@pytest.mark.parametrize("renew", [False, True])
async def test_deadline_covers_blocked_send_and_shutdown_joins_it(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    renew: bool,
) -> None:
    peer = clocked_peer
    peer.answer_heartbeats = False
    acknowledged = asyncio.Event()
    async with monitored(peer, on_heartbeat_ack=acknowledged.set) as client:
        old_task = client._heartbeat_task
        old_socket = client.connection
        with hold_send(client, monkeypatch) as held:
            await clock.advance(INTERVAL)
            await held.entered.wait()
            connection, frame = await peer.heartbeats.get()
            if renew:
                await clock.advance(INTERVAL)
                await peer.reply_heartbeat(connection, frame)
                await acknowledged.wait()
            await clock.advance(THRESHOLD - clock.loop.time())
            if renew:
                assert not held.cancelled.is_set()
                stop = asyncio.create_task(client.shutdown(STOP_REASON))
                try:
                    await held.cancelled.wait()
                    assert not stop.done()
                    assert old_task is not None
                    assert not old_task.done()
                    stop.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await stop
                    assert not old_task.done()
                finally:
                    held.release.set()
                await client.shutdown(STOP_REASON)
            else:
                await held.cancelled.wait()
                held.release.set()
                await connection.wait_closed()
                await client.run_forever(install_signal_handlers=False)
        assert old_task is not None
        assert old_task.done()
        assert old_socket is not None
        assert old_socket.state is State.CLOSED
        assert client._heartbeat_timeout is None


@pytest.mark.clock
@pytest.mark.parametrize("error", [OSError, TimeoutError])
async def test_send_failure_closes_instead_of_disabling_monitoring(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
    error: type[Exception],
) -> None:
    async with monitored(clocked_peer) as client:
        socket = client.connection
        assert socket is not None

        async def failed_send(
            connection: ClientConnection, message: ChannelMessage
        ) -> None:
            del connection, message
            raise error

        monkeypatch.setattr(client._protocol_handler, "send_message", failed_send)
        await clock.advance(INTERVAL)
        await socket.wait_closed()
        await client.run_forever(install_signal_handlers=False)
        assert socket.state is State.CLOSED
        assert client._heartbeat_timeout is None


@pytest.mark.clock
@pytest.mark.parametrize("cancel", [False, True])
async def test_public_force_close_aborts_and_preserves_failure(
    clocked_peer: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cancel: bool,
) -> None:
    entered = asyncio.Event()
    async with monitored(
        clocked_peer, reconnect_policy=STOPS_ON_NORMAL_CLOSE
    ) as client:
        socket = client.connection
        assert socket is not None
        close = socket.close

        async def failed_close(code: int = 1000, reason: str = "") -> None:
            del code, reason
            entered.set()
            if cancel:
                await wait_forever()
            raise CallbackError

        with monkeypatch.context() as patch:
            patch.setattr(socket, "close", failed_close)
            closing = asyncio.create_task(client.close_connection(STOP_REASON))
            try:
                await entered.wait()
                if cancel:
                    closing.cancel()
                with pytest.raises(asyncio.CancelledError if cancel else CallbackError):
                    await closing
                await socket.wait_closed()
            finally:
                patch.setattr(socket, "close", close)
        await client.run_forever(install_signal_handlers=False)
        assert socket.state is State.CLOSED


@pytest.mark.clock
@each_protocol
@pytest.mark.parametrize("held_stage", ["rejoin", "callback"])
async def test_monitoring_runs_during_reconnect_setup(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    held_stage: str,
) -> None:
    peer = clocked_peer
    entered = asyncio.Event()
    release = asyncio.Event()
    disconnected = asyncio.Event()

    async def on_disconnect(error: Exception | None) -> None:
        del error
        disconnected.set()

    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()

    async def reconnected() -> None:
        entered.set()
        if held_stage == "callback":
            await release.wait()

    async with monitored(
        peer,
        auto_reconnect=True,
        reconnect_policy=STOPS_ON_NORMAL_CLOSE,
        on_reconnect=reconnected,
        on_disconnect=on_disconnect,
        join_timeout_s=THRESHOLD * WINDOWS,
    ) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        old_task = client._heartbeat_task
        if held_stage == "rejoin":
            peer.defer_join_targets.add((peer.next_client_id, TOPIC))
        await peer.close_all_clients()
        await disconnected.wait()
        await clock.advance(INTERVAL)
        if held_stage == "rejoin":
            connection, _join = await peer.deferred_joins.get()
        else:
            await entered.wait()
            [connection] = peer.list_client_connections()
        try:
            assert old_task is not None
            assert old_task.done()
            deadline = timer(client).when()
            assert deadline is not None
            peer.answer_heartbeats = False
            await clock.advance(INTERVAL)
            heartbeat_connection, _ = await peer.heartbeats.get()
            assert heartbeat_connection is connection
            # The deferred join never stopped heartbeat reads.
            assert connection.state is State.OPEN
            await clock.advance(deadline - clock.loop.time() - CLOCK_RESOLUTION)
            assert connection.state is State.OPEN
            await clock.advance(CLOCK_RESOLUTION)
            await connection.wait_closed()
        finally:
            release.set()
            peer.defer_join_targets.clear()
            peer.answer_heartbeats = True


@pytest.mark.clock
@each_protocol
async def test_finishing_rejoin_does_not_reset_the_window(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
) -> None:
    peer = clocked_peer
    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    reconnected = asyncio.Event()
    disconnected = asyncio.Event()

    async def on_disconnect(error: Exception | None) -> None:
        del error
        disconnected.set()

    async def on_reconnect() -> None:
        reconnected.set()

    async with monitored(
        peer,
        auto_reconnect=True,
        reconnect_policy=STOPS_ON_NORMAL_CLOSE,
        on_reconnect=on_reconnect,
        on_disconnect=on_disconnect,
        join_timeout_s=THRESHOLD * WINDOWS,
    ) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        peer.defer_join_targets.add((peer.next_client_id, TOPIC))
        await peer.close_all_clients()
        await disconnected.wait()
        await clock.advance(INTERVAL)
        connection, join = await peer.deferred_joins.get()
        deadline = timer(client).when()
        assert deadline is not None
        peer.answer_heartbeats = False
        await clock.advance(INTERVAL)
        await peer.heartbeats.get()
        await peer.release_join(connection, join)
        await reconnected.wait()
        await verify_delivery(peer, client, received)
        assert timer(client).when() == deadline
        client.auto_reconnect = False
        await clock.advance(deadline - clock.loop.time())
        await connection.wait_closed()
        await client.run_forever(install_signal_handlers=False)


@pytest.mark.clock
@each_protocol
@pytest.mark.parametrize("offset", [0.0, CLOCK_RESOLUTION])
async def test_matching_reply_at_or_after_deadline_cannot_revive_it(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    offset: float,
) -> None:
    peer = clocked_peer
    peer.answer_heartbeats = False
    acknowledged = asyncio.Event()
    async with monitored(peer, on_heartbeat_ack=acknowledged.set) as client:
        await clock.advance(INTERVAL)
        connection, frame = await peer.heartbeats.get()
        active = timer(client)
        # Deliver on the wire at the absolute boundary, before yielding to timers.
        # send() completes without yielding on this writable local socket.
        clock.jump(THRESHOLD - clock.loop.time() + offset)
        # Check the exact-time guard before the timeout callback gets a turn.
        client._handle_heartbeat_response(
            make_message(
                topic=SYSTEM_TOPIC, event=PHXEvent.reply, payload={}, ref=frame.ref
            )
        )
        assert not acknowledged.is_set()
        assert active.when() == THRESHOLD
        await peer.reply_heartbeat(connection, frame)
        await connection.wait_closed()
        await client.run_forever(install_signal_handlers=False)
        assert not acknowledged.is_set()
        assert active.expired()
        assert client._heartbeat_timeout is None
        # An already parsed reply cannot re-enter a cleared timer either.
        client._handle_heartbeat_response(
            make_message(
                topic=SYSTEM_TOPIC, event=PHXEvent.reply, payload={}, ref=frame.ref
            )
        )
        assert not acknowledged.is_set()


@pytest.mark.clock
async def test_public_shutdown_releases_the_waiting_heartbeat(
    clocked_peer: FakePhoenixServer,
) -> None:
    async with monitored(clocked_peer) as client:
        socket = client.connection
        task = client._heartbeat_task
        assert socket is not None
        assert task is not None
        await client.shutdown(STOP_REASON)
        assert task.done()
        assert socket.state is State.CLOSED
        assert client._heartbeat_timeout is None


@pytest.mark.clock
async def test_heartbeat_close_failure_aborts_the_captured_socket(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clocked_peer.answer_heartbeats = False
    async with monitored(clocked_peer) as client:
        socket = client.connection
        assert socket is not None

        async def failed_close(code: int = 1000, reason: str = "") -> None:
            del code, reason
            raise CallbackError

        with monkeypatch.context() as patch:
            patch.setattr(socket, "close", failed_close)
            await clock.advance(THRESHOLD)
            await socket.wait_closed()
            await client.run_forever(install_signal_handlers=False)
        assert socket.state is State.CLOSED
        assert client._heartbeat_timeout is None


@pytest.mark.clock
@pytest.mark.parametrize("operation", [hold_send, hold_close], ids=["send", "close"])
async def test_replacement_waits_for_old_operation_cancellation_to_finish(
    clock: Clock,
    clocked_peer: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
    operation: Callable[
        [PHXChannelsClient, pytest.MonkeyPatch], AbstractContextManager[HeldOperation]
    ],
) -> None:
    peer = clocked_peer
    disconnected = asyncio.Event()
    reconnected = asyncio.Event()

    async def on_disconnect(error: Exception | None) -> None:
        del error
        disconnected.set()

    async def on_reconnect() -> None:
        reconnected.set()

    async with monitored(
        peer,
        auto_reconnect=True,
        reconnect_policy=STOPS_ON_NORMAL_CLOSE,
        on_disconnect=on_disconnect,
        on_reconnect=on_reconnect,
    ) as client:
        old_socket = client.connection
        old_task = client._heartbeat_task
        initial_generation = client._conn_generation
        with operation(client, monkeypatch) as held:
            peer.answer_heartbeats = False
            await clock.advance(INTERVAL if operation is hold_send else THRESHOLD)
            await held.entered.wait()
            if operation is hold_send:
                await peer.close_all_clients()
            await held.cancelled.wait()
            assert client._conn_generation == 1
            assert not disconnected.is_set()
            assert old_task is not None
            assert not old_task.done()
        await disconnected.wait()
        assert old_task.done()
        assert old_socket is not None
        assert old_socket.state is State.CLOSED
        await clock.advance(INTERVAL)
        await reconnected.wait()
        assert client._conn_generation == initial_generation + 1
        assert client.connection is not old_socket
