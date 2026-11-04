from __future__ import annotations

import asyncio
from typing import Any

import pytest

from phoenix_channels_python_client import (
    Channel,
    ChannelError,
    Client,
    Message,
    SocketError,
)
from tests.channel_support import UnansweredLeavesPeer
from tests.fake_server import FakePhoenixServer, WireEvent
from tests.join_support import deadline, paused_writes, push, reply
from tests.scheduling import TaskLifecycleGate
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    TOPIC,
    each_protocol,
    make_client,
)


def test_creation_and_registration_do_not_require_an_event_loop() -> None:
    client = Client("ws://127.0.0.1:1/socket")
    channel = client.channel(TOPIC)

    async def handler(payload: dict[str, Any]) -> None:
        del payload

    assert channel.on(EVENT, handler) is handler
    assert channel.on(EVENT)(handler) is handler
    assert channel.topic == TOPIC
    assert not channel.joined


async def test_disconnected_admission_does_not_retire_the_created_handle(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    channel = client.channel(TOPIC)
    with pytest.raises(SocketError):
        await channel.join()
    async with client, channel:
        assert channel.joined


@each_protocol
async def test_decorator_sees_the_first_event_and_context_exit_leaves(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    payload = {"body": "first"}
    phoenix_server.join_events[TOPIC] = [(EVENT, payload)]
    channel = client.channel(TOPIC)
    received: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    @channel.on(EVENT)
    async def handle(message: dict[str, Any]) -> None:
        received.put_nowait(message)

    assert not phoenix_server.join_topics
    assert not channel.joined
    async with deadline(), channel:
        assert channel.joined
        assert await received.get() == payload
    assert not channel.joined
    await channel.leave()
    with pytest.raises(ChannelError):
        await channel.join()


@each_protocol
async def test_only_one_created_handle_can_claim_a_live_topic(client: Client) -> None:
    first = client.channel(TOPIC)
    second = client.channel(TOPIC)
    async with first:
        with pytest.raises(ChannelError, match="live channel"):
            await second.join()
        assert first.joined
        assert not second.joined
    async with second:
        assert second.joined


@each_protocol
@pytest.mark.parametrize("params", [None, {"token": "static"}])
async def test_join_params_are_sent_as_a_json_map(
    client: Client,
    phoenix_server: FakePhoenixServer,
    params: dict[str, str] | None,
) -> None:
    async with client.channel(TOPIC, params=params):
        _, request = await phoenix_server.joined.get()
        assert request.payload == (params if params is not None else {})


@each_protocol
async def test_callable_params_are_fresh_for_channel_and_socket_rejoins(
    phoenix_server: FakePhoenixServer,
) -> None:
    attempts = 0
    installed: asyncio.Queue[None] = asyncio.Queue()

    def params() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        return {"attempt": attempts}

    async with (
        deadline(),
        make_client(
            phoenix_server,
            reconnect_policy=FAST_RECONNECT,
        ) as client,
    ):
        room = client.channel(
            TOPIC,
            params=params,
            on_join_response=lambda _: installed.put_nowait(None),
        )
        await room.join()
        await installed.get()
        _, initial = await phoenix_server.joined.get()
        assert initial.payload == {"attempt": 1}
        await phoenix_server.crash_channel(TOPIC)
        await installed.get()
        _, recovery = await phoenix_server.joined.get()
        assert recovery.payload == {"attempt": 2}
        await phoenix_server.close_all_clients()
        await installed.get()
        _, reconnect = await phoenix_server.joined.get()
        assert reconnect.payload == {"attempt": 3}
        assert attempts == reconnect.payload["attempt"]
        assert room.joined


@each_protocol
@pytest.mark.parametrize("exception_type", [ValueError, asyncio.CancelledError])
async def test_initial_params_failure_preserves_its_cause_and_sends_nothing(
    client: Client,
    phoenix_server: FakePhoenixServer,
    exception_type: type[BaseException],
) -> None:
    failure = exception_type("private provider failure")

    def params() -> dict[str, Any]:
        raise failure

    channel = client.channel(TOPIC, params=params)
    with pytest.raises(ChannelError) as caught:
        await channel.join()
    assert caught.value.__cause__ is failure
    assert phoenix_server.join_topics == []
    assert not channel.joined
    with pytest.raises(ChannelError):
        await channel.join()
    async with client.channel(TOPIC) as replacement:
        assert replacement.joined


@each_protocol
async def test_params_failure_on_recovery_loses_only_that_channel(
    phoenix_server: FakePhoenixServer,
) -> None:
    attempts = 0
    lost: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

    def params() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            raise ValueError("provider cannot refresh")
        return {"token": "initial"}

    async def on_lost(topic: str, error: Exception) -> None:
        await lost.put((topic, error))

    async with (
        deadline(),
        make_client(
            phoenix_server,
            reconnect_policy=FAST_RECONNECT,
            on_topic_lost=on_lost,
        ) as client,
    ):
        affected = client.channel(TOPIC, params=params)
        neighbor = client.channel(OTHER_TOPIC)
        await affected.join()
        await neighbor.join()
        await phoenix_server.crash_channel(TOPIC)
        topic, error = await lost.get()
        assert topic == TOPIC
        assert isinstance(error, ChannelError)
        assert not affected.joined
        assert neighbor.joined
        assert attempts == phoenix_server.join_topics.count(TOPIC) + 1
        assert phoenix_server.join_topics.count(TOPIC) == 1
        received: asyncio.Queue[Message] = asyncio.Queue()
        neighbor.on_any(received.put)
        await phoenix_server.simulate_server_event(OTHER_TOPIC, EVENT, {"ok": True})
        assert (await received.get()).payload == {"ok": True}


@pytest.mark.parametrize("option", ["join_timeout_s", "leave_timeout_s"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), 0.0, -1.0])
def test_channel_operation_deadlines_must_be_finite_and_positive(
    option: str,
    value: float,
) -> None:
    options: dict[str, Any] = {option: value}
    with pytest.raises(ValueError, match=f"{option} must be"):
        Client("ws://127.0.0.1:1/socket", **options)


async def test_cancelled_context_keeps_cancellation_when_leave_times_out() -> None:
    async with (
        deadline(),
        UnansweredLeavesPeer() as server,
        make_client(server, leave_timeout_s=JOIN_TIMEOUT_S) as client,
    ):
        channel = client.channel(TOPIC)
        entered = asyncio.Event()

        async def scope() -> None:
            async with channel:
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(scope())
        try:
            await entered.wait()
            task.cancel()
            await server.left.get()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not channel.joined
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_shutdown_closes_transport_before_waiting_for_admitted_sends(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with deadline(), make_client(phoenix_server) as client:
        connection = client.connection
        assert connection is not None
        channel = client.channel(TOPIC)
        with paused_writes(connection):
            await channel.join()
            owner = channel.subscription
            assert owner.send_session is not None
            assert owner.send_session.has_pending(owner.current_join_ready)
            await client.shutdown()
        assert not channel.joined
        assert client.connection is None
        assert client.topic_subscriptions == {}
        assert not client.cleanup_tasks


@each_protocol
async def test_leave_before_pending_admission_cannot_resurrect_the_handle(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    channel = client.channel(TOPIC)
    gate = TaskLifecycleGate(Channel.join.__code__)
    async with deadline():
        with gate.installed():
            async with client.topics_lock:
                joining = asyncio.create_task(channel.join())
                await gate.started.wait()
                await channel.leave()
                assert not channel.joined
            try:
                with pytest.raises(ChannelError, match="already joined or left"):
                    await joining
                assert phoenix_server.join_topics == []
            finally:
                joining.cancel()
                await asyncio.gather(joining, return_exceptions=True)


@each_protocol
async def test_duplicate_join_ack_is_consumed_without_reinstalling_or_dispatching(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    installed: list[object] = []
    received: asyncio.Queue[Message] = asyncio.Queue()
    channel = client.channel(TOPIC, on_join_response=installed.append)
    channel.on_any(received.put)
    async with deadline(), channel:
        _, request = await phoenix_server.joined.get()
        await reply(phoenix_server, request)
        await push(phoenix_server, request, payload={"after_duplicate": True})
        message = await received.get()
        assert message.event == EVENT
        assert message.payload == {"after_duplicate": True}
        assert installed == [{}]
        assert received.empty()


@each_protocol
async def test_a_stalled_join_send_is_bounded_even_with_heartbeats_disabled(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with (
        deadline(),
        make_client(
            phoenix_server,
            auto_reconnect=False,
            heartbeat_interval_s=None,
            join_timeout_s=JOIN_TIMEOUT_S,
        ) as client,
    ):
        connection = client.connection
        assert connection is not None
        channel = client.channel(TOPIC)
        with paused_writes(connection):
            await channel.join()
            assert channel.joined
            await client.run_forever(install_signal_handlers=False)
        assert not channel.joined
        assert client.connection is None
        with pytest.raises(ChannelError, match="already joined or left"):
            await channel.join()


@each_protocol
async def test_context_body_failure_still_leaves_its_channel(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    channel = client.channel(TOPIC)
    async with deadline():
        with pytest.raises(ValueError, match="body failed"):
            async with channel:
                raise ValueError("body failed")
        _, leave = await phoenix_server.left.get()
        assert (leave.topic, leave.event, leave.payload) == (TOPIC, WireEvent.LEAVE, {})
        assert not channel.joined
