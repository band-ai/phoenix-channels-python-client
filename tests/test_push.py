from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from pydantic import ValidationError

from phoenix_channels_python_client import (
    ChannelError,
    Client,
    Message,
    Payload,
    ProtocolVersion,
    PushTimeoutError,
    Reply,
    SocketError,
)
from phoenix_channels_python_client.send_session import SessionSends
from tests.clock import Clock
from tests.fake_server import Frame, WireEvent
from tests.join_support import deadline, paused_writes
from tests.push_support import PushPeer, pending_push
from tests.scheduling import TaskLifecycleGate, TimerCallbackGate
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    OTHER_TOPIC,
    TOPIC,
    each_protocol,
    make_client,
)

PUSH_TIMEOUT_S = 0.5
BARRIER_EVENT = "after_reply"


@pytest_asyncio.fixture
async def push_peer(protocol: ProtocolVersion) -> AsyncIterator[PushPeer]:
    async with PushPeer(protocol) as peer:
        yield peer


@each_protocol
@pytest.mark.parametrize("status", ["ok", "error", "accepted"])
async def test_push_returns_the_server_status_and_response(
    push_peer: PushPeer, status: str
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        _, join = await push_peer.joined.get()
        async with pending_push(channel, payload={"body": "request"}) as task:
            request = await push_peer.pushes.get()
            assert request[1].payload == {"body": "request"}
            assert request[1].ref != join.ref
            await push_peer.answer_push(
                request, status=status, response={"body": "response"}
            )
            reply = await task
            assert reply == Reply(status=status, response={"body": "response"})
            with pytest.raises(ValidationError, match="frozen"):
                reply.status = "changed"  # type: ignore[read-only]


@each_protocol
async def test_matched_reply_is_consumed_before_a_later_message(
    push_peer: PushPeer,
) -> None:
    async with deadline(), make_client(push_peer) as client:
        received: asyncio.Queue[Message] = asyncio.Queue()
        channel = client.channel(TOPIC)
        channel.on_any(received.put)
        async with channel, pending_push(channel) as task:
            request = await push_peer.pushes.get()
            await push_peer.answer_push(request)
            assert (await task).status == "ok"
            await push_peer.simulate_channel_push(TOPIC, BARRIER_EVENT, {"after": True})
            message = await received.get()
            assert (message.event, message.payload) == (BARRIER_EVENT, {"after": True})
            assert received.empty()


@each_protocol
async def test_out_of_order_replies_resolve_only_their_own_push(
    push_peer: PushPeer,
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
        pending_push(channel) as first,
        pending_push(channel) as second,
    ):
        one, two = await push_peer.pushes.get(), await push_peer.pushes.get()
        assert one[1].ref != two[1].ref
        await push_peer.answer_push(two, response={"order": 2})
        assert (await second).response == {"order": 2}
        assert not first.done()
        await push_peer.answer_push(one, response={"order": 1})
        assert (await first).response == {"order": 1}


@each_protocol
async def test_a_handler_can_push_on_its_own_topic(push_peer: PushPeer) -> None:
    async with deadline(), make_client(push_peer) as client:
        completed: asyncio.Queue[Reply] = asyncio.Queue()
        channel = client.channel(TOPIC)

        @channel.on(EVENT)
        async def handle(payload: Payload) -> None:
            completed.put_nowait(await channel.push("from_handler", payload))

        async with channel:
            await push_peer.simulate_channel_push(TOPIC, EVENT, {"trigger": True})
            request = await push_peer.pushes.get()
            assert request[1].event == "from_handler"
            await push_peer.answer_push(request, response={"finished": True})
            assert (await completed.get()).response == {"finished": True}


@each_protocol
async def test_push_completion_does_not_wait_for_an_earlier_handler(
    push_peer: PushPeer,
) -> None:
    async with deadline(), make_client(push_peer) as client:
        entered, release = asyncio.Event(), asyncio.Event()
        channel = client.channel(TOPIC)

        @channel.on(EVENT)
        async def handle(payload: Payload) -> None:
            del payload
            entered.set()
            await release.wait()

        async with channel:
            try:
                await push_peer.simulate_channel_push(TOPIC, EVENT, {})
                await entered.wait()
                async with pending_push(channel) as task:
                    request = await push_peer.pushes.get()
                    await push_peer.answer_push(request)
                    assert (await task).status == "ok"
                    assert not release.is_set()
            finally:
                release.set()


@each_protocol
@pytest.mark.parametrize("payload", [{}, {"status": "ok", "response": []}])
async def test_malformed_reply_fails_only_its_push_with_the_validation_cause(
    push_peer: PushPeer, payload: dict[str, object]
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        async with pending_push(channel) as task:
            request = await push_peer.pushes.get()
            await push_peer.reply_payload(request, payload)
            with pytest.raises(ChannelError, match="Malformed") as failure:
                await task
            assert isinstance(failure.value.__cause__, TypeError)
            assert channel.joined
        async with pending_push(channel) as next_push:
            await push_peer.answer_push(await push_peer.pushes.get())
            assert (await next_push).status == "ok"


async def test_not_joined_precedes_socket_or_timeout_checks() -> None:
    channel = Client("ws://127.0.0.1:1/socket").channel(TOPIC)
    with pytest.raises(ChannelError, match="not joined"):
        await channel.push(EVENT, {}, timeout_s=math.nan)


@each_protocol
@pytest.mark.parametrize("timeout_s", [0, -1, math.nan, math.inf])
async def test_invalid_push_timeout_emits_no_application_frame(
    push_peer: PushPeer, timeout_s: float
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        with pytest.raises(ValueError, match="finite"):
            await channel.push(EVENT, {}, timeout_s=timeout_s)
        assert push_peer.pushes.empty()
        assert channel.subscription.pending_pushes == {}


@each_protocol
async def test_unencodable_payload_leaves_no_pending_push(push_peer: PushPeer) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        with pytest.raises(TypeError):
            await channel.push(EVENT, {"unsupported": object()})
        assert channel.subscription.pending_pushes == {}
        async with pending_push(channel) as task:
            request = await push_peer.pushes.get()
            assert request[1].payload == {}
            await push_peer.answer_push(request)
            assert (await task).status == "ok"
        assert push_peer.pushes.empty()


@each_protocol
async def test_cancellation_removes_correlation_and_a_late_reply_routes_normally(
    push_peer: PushPeer,
) -> None:
    async with deadline(), make_client(push_peer) as client:
        received: asyncio.Queue[Message] = asyncio.Queue()
        channel = client.channel(TOPIC)
        channel.on_any(received.put)
        async with channel, pending_push(channel) as task:
            request = await push_peer.pushes.get()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert channel.subscription.pending_pushes == {}
            await push_peer.answer_push(request, response={"late": True})
            message = await received.get()
            assert (message.event, message.ref, message.payload) == (
                WireEvent.REPLY,
                request[1].ref,
                {"status": "ok", "response": {"late": True}},
            )


@each_protocol
async def test_crash_fails_pending_push_before_rejoin_and_never_replays_it(
    push_peer: PushPeer,
) -> None:
    async with (
        deadline(),
        make_client(push_peer, reconnect_policy=FAST_RECONNECT) as client,
    ):
        installed: asyncio.Queue[object] = asyncio.Queue()
        channel = client.channel(TOPIC, on_join_response=installed.put_nowait)
        async with channel:
            await installed.get()
            push_peer.defer_join_targets.add((1, TOPIC))
            async with pending_push(channel, payload={"old": True}) as task:
                old = await push_peer.pushes.get()
                await push_peer.crash_channel(TOPIC)
                with pytest.raises(ChannelError, match="crashed"):
                    await task
                connection, rejoin = await push_peer.deferred_joins.get()
                assert not channel.joined
                with pytest.raises(ChannelError, match="not joined"):
                    await channel.push(EVENT, {})
                await push_peer.release_join(connection, rejoin)
                await installed.get()
            async with pending_push(channel, payload={"fresh": True}) as fresh:
                request = await push_peer.pushes.get()
                assert request[1].payload == {"fresh": True}
                assert request[1].ref != old[1].ref
                await push_peer.answer_push(old, response={"old": True})
                await push_peer.answer_push(request, response={"fresh": True})
                assert (await fresh).response == {"fresh": True}
                assert push_peer.pushes.empty()


@each_protocol
async def test_socket_loss_fails_pending_push_without_replay_on_the_new_connection(
    push_peer: PushPeer,
) -> None:
    async with (
        deadline(),
        make_client(push_peer, reconnect_policy=FAST_RECONNECT) as client,
    ):
        installed: asyncio.Queue[object] = asyncio.Queue()
        channel = client.channel(TOPIC, on_join_response=installed.put_nowait)
        async with channel:
            await installed.get()
            push_peer.defer_join_targets.add((2, TOPIC))
            async with pending_push(channel, payload={"old": True}) as task:
                old = await push_peer.pushes.get()
                await old[0].close()
                with pytest.raises(SocketError):
                    await task
                connection, rejoin = await push_peer.deferred_joins.get()
                await push_peer.release_join(connection, rejoin)
                await installed.get()
            async with pending_push(channel, payload={"fresh": True}) as fresh:
                request = await push_peer.pushes.get()
                assert request[0] is not old[0]
                assert request[1].payload == {"fresh": True}
                # Fault injection: an old ref on the fresh connection must not
                # resolve the fresh operation, even when V1 has no join_ref.
                await push_peer._send(
                    connection,
                    old[1]._replace(
                        event=WireEvent.REPLY,
                        payload={"status": "ok", "response": {"old": True}},
                    ),
                )
                await push_peer.answer_push(request, response={"fresh": True})
                assert (await fresh).response == {"fresh": True}
                assert push_peer.pushes.empty()


@each_protocol
@pytest.mark.parametrize("action", ["close", "leave", "shutdown"])
async def test_channel_retirement_fails_pending_push_immediately(
    push_peer: PushPeer, action: str
) -> None:
    async with deadline(), make_client(push_peer) as client:
        channel = client.channel(TOPIC)
        await channel.join()
        async with pending_push(channel) as task:
            await push_peer.pushes.get()
            match action:
                case "close":
                    await push_peer.close_channel(TOPIC)
                case "leave":
                    await channel.leave()
                case "shutdown":
                    await client.shutdown()
            with pytest.raises(ChannelError):
                await task
            assert not channel.joined
            assert channel.subscription.pending_pushes == {}


@pytest.mark.clock
@each_protocol
async def test_reply_timeout_after_send_completion_is_local_to_the_push(
    clock: Clock, push_peer: PushPeer
) -> None:
    async with (
        deadline(),
        make_client(push_peer, heartbeat_interval_s=None) as client,
        client.channel(TOPIC) as channel,
    ):
        connection = client.connection
        async with pending_push(channel, timeout_s=PUSH_TIMEOUT_S) as task:
            await push_peer.pushes.get()
            owner = channel.subscription
            outcome = next(iter(owner.pending_pushes.values()))
            assert owner.send_session is not None
            await owner.send_session.wait_for_send(outcome)
            await clock.advance(PUSH_TIMEOUT_S)
            with pytest.raises(PushTimeoutError):
                await task
            assert channel.joined
            assert client.connection is connection
            assert owner.pending_pushes == {}
        async with pending_push(channel) as fresh:
            await push_peer.answer_push(await push_peer.pushes.get())
            assert (await fresh).status == "ok"


@pytest.mark.clock
@each_protocol
async def test_a_write_stall_keeps_an_earlier_reply_and_fails_other_pending_pushes(
    clock: Clock, push_peer: PushPeer
) -> None:
    async with (
        deadline(),
        make_client(
            push_peer, heartbeat_interval_s=None, auto_reconnect=False
        ) as client,
    ):
        channel, neighbor = client.channel(TOPIC), client.channel(OTHER_TOPIC)
        await channel.join()
        await neighbor.join()
        connection = client.connection
        assert connection is not None
        with paused_writes(connection):
            async with pending_push(channel, timeout_s=PUSH_TIMEOUT_S) as task:
                request = await push_peer.pushes.get()
                await push_peer.answer_push(request, response={"first": True})
                assert (await task).response == {"first": True}
                async with pending_push(
                    neighbor, timeout_s=2 * PUSH_TIMEOUT_S
                ) as other:
                    await push_peer.pushes.get()
                    await clock.advance(PUSH_TIMEOUT_S)
                    with pytest.raises(SocketError):
                        await other
                    assert (await task).response == {"first": True}
                    await client.run_forever(install_signal_handlers=False)
        assert client.connection is None
        assert not channel.joined
        assert not neighbor.joined


async def test_v2_membership_fences_precede_push_correlation() -> None:
    async with (
        deadline(),
        PushPeer(ProtocolVersion.V2) as peer,
        make_client(peer) as client,
        client.channel(TOPIC) as channel,
        pending_push(channel) as task,
    ):
        connection, request = await peer.pushes.get()
        payload = {"status": "ok", "response": {"member": "current"}}
        await peer._send(
            connection,
            Frame(
                "stale-membership",
                request.ref,
                TOPIC,
                WireEvent.REPLY,
                {"status": "ok", "response": {"member": "stale"}},
            ),
        )
        await peer._send(
            connection, Frame(None, request.ref, TOPIC, WireEvent.REPLY, payload)
        )
        assert (await task).response == {"member": "current"}


@each_protocol
@pytest.mark.parametrize(
    "event", [event for event in WireEvent if event != WireEvent.HEARTBEAT]
)
async def test_protocol_events_cannot_be_pushed_as_application_events(
    push_peer: PushPeer, event: str
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        with pytest.raises(ValueError, match="application event"):
            await channel.push(event, {})
        assert channel.joined
        assert channel.subscription.pending_pushes == {}


@each_protocol
async def test_invalid_payload_type_does_not_submit_a_push(push_peer: PushPeer) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        with pytest.raises(TypeError, match=r"[Pp]ayload"):
            await channel.push(EVENT, [])  # type: ignore[bad-argument-type]
        assert channel.subscription.pending_pushes == {}
        assert push_peer.pushes.empty()


@each_protocol
async def test_cancellation_before_submission_emits_no_push(
    push_peer: PushPeer,
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        gate = TaskLifecycleGate(SessionSends._send.__code__, hold_start=True)
        with gate.installed():
            async with pending_push(channel) as task:
                await gate.started.wait()
                owner = channel.subscription
                outcome = next(iter(owner.pending_pushes.values()))
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert owner.send_session is not None
                await owner.send_session.wait_for_send(outcome)
                assert owner.pending_pushes == {}
                assert push_peer.pushes.empty()
        async with pending_push(channel) as fresh:
            await push_peer.answer_push(await push_peer.pushes.get())
            assert (await fresh).status == "ok"
            assert push_peer.pushes.empty()


@each_protocol
async def test_crash_reaches_a_handler_awaiting_its_own_topic_push(
    push_peer: PushPeer,
) -> None:
    async with deadline(), make_client(push_peer) as client:
        completed: asyncio.Queue[Exception] = asyncio.Queue()
        channel = client.channel(TOPIC)

        @channel.on(EVENT)
        async def handle(payload: Payload) -> None:
            try:
                await channel.push("from_handler", payload)
            except ChannelError as exc:
                completed.put_nowait(exc)

        async with channel:
            push_peer.defer_join_targets.add((1, TOPIC))
            await push_peer.simulate_channel_push(TOPIC, EVENT, {})
            await push_peer.pushes.get()
            await push_peer.crash_channel(TOPIC)
            assert "crashed" in str(await completed.get())
            assert not channel.joined
            connection, rejoin = await push_peer.deferred_joins.get()
            await push_peer.release_join(connection, rejoin)


@pytest.mark.clock
@each_protocol
async def test_write_deadline_times_out_originating_push_and_fails_its_neighbor(
    clock: Clock, push_peer: PushPeer
) -> None:
    async with (
        deadline(),
        make_client(
            push_peer, heartbeat_interval_s=None, auto_reconnect=False
        ) as client,
    ):
        channel, neighbor = client.channel(TOPIC), client.channel(OTHER_TOPIC)
        await channel.join()
        await neighbor.join()
        connection = client.connection
        assert connection is not None
        with paused_writes(connection):
            async with (
                pending_push(channel, timeout_s=PUSH_TIMEOUT_S) as task,
                pending_push(neighbor, timeout_s=2 * PUSH_TIMEOUT_S) as other,
            ):
                await push_peer.pushes.get()
                await push_peer.pushes.get()
                await clock.advance(PUSH_TIMEOUT_S)
                with pytest.raises(PushTimeoutError):
                    await task
                with pytest.raises(SocketError):
                    await other
                await client.run_forever(install_signal_handlers=False)
        assert client.connection is None


@each_protocol
async def test_heartbeat_named_application_event_uses_normal_push_correlation(
    push_peer: PushPeer,
) -> None:
    async with (
        deadline(),
        make_client(push_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        task = asyncio.create_task(
            channel.push(WireEvent.HEARTBEAT, {"application": True})
        )
        try:
            request = await push_peer.pushes.get()
            assert (request[1].topic, request[1].event, request[1].payload) == (
                TOPIC,
                WireEvent.HEARTBEAT,
                {"application": True},
            )
            await push_peer.answer_push(request)
            assert (await task).status == "ok"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.clock
@each_protocol
async def test_dispatched_old_watchdog_cannot_fail_a_push_after_client_reentry(
    clock: Clock, push_peer: PushPeer
) -> None:
    async with (
        deadline(),
        make_client(
            push_peer, heartbeat_interval_s=None, auto_reconnect=False
        ) as client,
    ):
        channel = client.channel(TOPIC)
        await channel.join()
        old_connection, old_session = client.connection, client.send_session
        old_generation = client.conn_generation
        gate = TimerCallbackGate(SessionSends._expire.__code__)
        with gate.installed():
            async with pending_push(channel, timeout_s=PUSH_TIMEOUT_S) as old_push:
                await push_peer.pushes.get()
                timer = await gate.registered.get()
                await clock.advance(PUSH_TIMEOUT_S)
                await timer.ready.wait()
                await client.shutdown()
                with pytest.raises(ChannelError):
                    await old_push

            await client.__aenter__()
            async with (
                client.channel(TOPIC) as fresh,
                pending_push(fresh) as new_push,
            ):
                request = await push_peer.pushes.get()
                assert client.connection is not old_connection
                assert client.send_session is not old_session
                assert client.conn_generation == old_generation
                # Deliberate delayed-dispatch fault: the old timer already fired.
                timer.release()
                await push_peer.answer_push(request, response={"fresh": True})
                assert (await new_push).response == {"fresh": True}
                assert fresh.joined
        assert old_session is not None
        assert old_session.operations == {}


@pytest.mark.clock
@each_protocol
@pytest.mark.parametrize("first_callback", [0, 1])
async def test_equal_write_deadlines_settle_by_processed_callback_order(
    clock: Clock, push_peer: PushPeer, first_callback: int
) -> None:
    async with (
        deadline(),
        make_client(
            push_peer, heartbeat_interval_s=None, auto_reconnect=False
        ) as client,
    ):
        channel, neighbor = client.channel(TOPIC), client.channel(OTHER_TOPIC)
        await channel.join()
        await neighbor.join()
        connection, session = client.connection, client.send_session
        assert connection is not None
        assert session is not None
        gate = TimerCallbackGate(SessionSends._expire.__code__, count=2)
        with paused_writes(connection), gate.installed():
            async with (
                pending_push(channel, timeout_s=PUSH_TIMEOUT_S) as task,
                pending_push(neighbor, timeout_s=PUSH_TIMEOUT_S) as other,
            ):
                timers = [await gate.registered.get(), await gate.registered.get()]
                await push_peer.pushes.get()
                await push_peer.pushes.get()
                assert timers[0].deadline == timers[1].deadline
                await clock.advance(PUSH_TIMEOUT_S)
                await timers[0].ready.wait()
                await timers[1].ready.wait()
                timers[first_callback].release()
                timers[1 - first_callback].release()
                results = await asyncio.gather(task, other, return_exceptions=True)
                assert type(results[first_callback]) is PushTimeoutError
                assert type(results[1 - first_callback]) is SocketError
                await client.run_forever(install_signal_handlers=False)
        assert channel.subscription.pending_pushes == {}
        assert neighbor.subscription.pending_pushes == {}
        assert session.operations == {}
        assert client.connection is None
