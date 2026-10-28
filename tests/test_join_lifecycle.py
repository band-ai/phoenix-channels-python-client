from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, WireEvent
from tests.join_support import (
    JoinProbe,
    ProcessorGate,
    deadline,
    pending_subscribe,
    push,
    reply,
)
from tests.support import (
    FAST_RECONNECT,
    OTHER_TOPIC,
    STOP_REASON,
    TOPIC,
    each_protocol,
    make_client,
)

JOIN_DEADLINE = 10.0


@each_protocol
class TestJoinLifecycle:
    @pytest.mark.parametrize("action", ["cancel", "leave", "shutdown", "disconnect"])
    async def test_pending_admission_is_disposed_and_late_acks_cannot_admit_replacement(
        self, phoenix_server: FakePhoenixServer, action: str
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        lost: asyncio.Queue[str] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            del error
            await lost.put(topic)

        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
                on_topic_lost=on_lost,
            ) as client,
        ):
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            old = JoinProbe(client)
            async with pending_subscribe(client, old) as admission:
                _, request = await phoenix_server.deferred_joins.get()
                match action:
                    case "cancel":
                        admission.cancel()
                    case "leave":
                        await client.unsubscribe_from_topic(TOPIC)
                    case "shutdown":
                        await client.shutdown(STOP_REASON)
                    case "disconnect":
                        await phoenix_server.close_all_clients()
                expected = (
                    asyncio.CancelledError if action == "cancel" else PHXConnectionError
                )
                with pytest.raises(expected):
                    await admission
            assert not client.get_current_subscriptions()
            assert old.order == []
            if action == "shutdown":
                await client.__aenter__()
            if action == "disconnect":
                await reconnects.get()
            current_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((current_id, TOPIC))
            phoenix_server.join_payloads[TOPIC] = {
                "status": "ok",
                "response": {"new": True},
            }
            replacement = JoinProbe(client)
            async with pending_subscribe(client, replacement) as admission:
                connection, current = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await reply(phoenix_server, request, status="error")
                await push(
                    phoenix_server,
                    request,
                    WireEvent.REPLY,
                    {"status": "ok"},
                    ref="obsolete-leave",
                )
                await phoenix_server.release_join(connection, current)
                await admission
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{"new": True}, {"replacement": True}]
            assert lost.empty()

    @pytest.mark.clock
    async def test_initial_deadline_removes_admission_before_reuse(
        self, clock: Clock, clocked_peer: FakePhoenixServer
    ) -> None:
        async with make_client(clocked_peer, join_timeout_s=JOIN_DEADLINE) as client:
            clocked_peer.defer_join_targets.add(
                (clocked_peer.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            async with pending_subscribe(client, probe) as admission:
                connection, request = await clocked_peer.deferred_joins.get()
                await clock.advance(JOIN_DEADLINE)
                with pytest.raises(PHXTopicError, match="Timed out"):
                    await admission
            assert not client.get_current_subscriptions()
            await clocked_peer.release_join(connection, request)
            await client.subscribe_to_topic(TOPIC, on_join_response=probe.install)
            assert probe.order == [{}]

    @pytest.mark.parametrize("event", [WireEvent.ERROR, WireEvent.CLOSE])
    @pytest.mark.parametrize("success_first", [False, True])
    async def test_pending_lifecycle_frames_and_success_follow_wire_order(
        self,
        phoenix_server: FakePhoenixServer,
        event: WireEvent,
        *,
        success_first: bool,
    ) -> None:
        async with (
            deadline(),
            make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client,
        ):
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            async with pending_subscribe(client, probe) as admission:
                _, request = await phoenix_server.deferred_joins.get()
                if success_first:
                    await reply(phoenix_server, request)
                await push(phoenix_server, request, event)
                if not success_first:
                    await reply(phoenix_server, request)
                    error_type = (
                        PHXConnectionError
                        if event == WireEvent.ERROR
                        else PHXTopicError
                    )
                    with pytest.raises(error_type):
                        await admission
                else:
                    await admission
            assert probe.order == ([None] if success_first else [])

    async def test_repeated_cancellation_keeps_captured_cleanup_owned_until_shutdown(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        running, finalizing, release, finished = (asyncio.Event() for _ in range(4))

        async def callback(message: ChannelMessage) -> None:
            del message
            running.set()
            try:
                await asyncio.Event().wait()
            finally:
                finalizing.set()
                await release.wait()
                finished.set()

        async with deadline(), make_client(phoenix_server) as client:
            await client.subscribe_to_topic(TOPIC, callback)
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await running.wait()
            leave = asyncio.create_task(client.unsubscribe_from_topic(TOPIC))
            await phoenix_server.left.get()
            leave.cancel()
            await finalizing.wait()
            leave.cancel()
            with pytest.raises(asyncio.CancelledError):
                await leave
            replacement = JoinProbe(client)
            try:
                await client.subscribe_to_topic(
                    TOPIC, replacement.receive, on_join_response=replacement.install
                )
                _, current = await phoenix_server.joined.get()
                await push(phoenix_server, current)
                await replacement.delivered.get()
                processor = client.get_current_subscriptions()[
                    TOPIC
                ].process_topic_messages_task
                assert processor is not None
                shutdown = asyncio.create_task(client.shutdown(STOP_REASON))
                await asyncio.gather(processor, return_exceptions=True)
                assert not shutdown.done()
                assert not finished.is_set()
            finally:
                release.set()
            await finished.wait()
            await shutdown
            assert not client._cleanup_tasks
            assert not client.get_current_subscriptions()

    @pytest.mark.parametrize("action", ["shutdown", "unsubscribe"])
    async def test_message_callback_lifecycle_unwinds_without_a_cleanup_cycle(
        self, phoenix_server: FakePhoenixServer, action: str
    ) -> None:
        unwound = asyncio.Event()
        async with deadline(), make_client(phoenix_server) as client:

            async def callback(message: ChannelMessage) -> None:
                del message
                try:
                    if action == "shutdown":
                        await client.shutdown(STOP_REASON)
                    else:
                        await client.unsubscribe_from_topic(TOPIC)
                finally:
                    unwound.set()

            await client.subscribe_to_topic(TOPIC, callback)
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await unwound.wait()
            if action == "shutdown":
                await client.shutdown(STOP_REASON)
            else:
                # V1 cannot fence unsolicited lifecycle frames by join_ref.
                # This wire round-trip drains the old leave before re-entry.
                await client.subscribe_to_topic(OTHER_TOPIC)
                await phoenix_server.joined.get()
                probe = JoinProbe(client)
                await client.subscribe_to_topic(
                    TOPIC, probe.receive, on_join_response=probe.install
                )
                _, current = await phoenix_server.joined.get()
                await push(phoenix_server, current)
                await probe.delivered.get()

    async def test_permanent_loss_callback_can_shutdown(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        unwound = asyncio.Event()

        async def lost(topic: str, error: Exception) -> None:
            del topic, error
            try:
                await client.shutdown(STOP_REASON)
            finally:
                unwound.set()

        async with (
            deadline(),
            make_client(phoenix_server, on_topic_lost=lost) as client,
        ):
            await client.subscribe_to_topic(TOPIC)
            await phoenix_server.close_channel(TOPIC)
            await unwound.wait()
            await client.shutdown(STOP_REASON)
            assert not client._cleanup_tasks

    async def test_fresh_client_starts_with_new_snapshot_after_abrupt_wire_loss(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with deadline(), make_client(phoenix_server, auto_reconnect=False) as old:
            probe = JoinProbe(old)
            await old.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request, payload={"old": True})
            await probe.delivered.get()
            assert old.connection is not None
            old.connection.transport.abort()
            phoenix_server.join_payloads[TOPIC] = {
                "status": "ok",
                "response": {"fresh": True},
            }
            async with make_client(phoenix_server) as fresh:
                current = JoinProbe(fresh)
                await fresh.subscribe_to_topic(
                    TOPIC, current.receive, on_join_response=current.install
                )
                _, request = await phoenix_server.joined.get()
                await push(phoenix_server, request, payload={"new": True})
                await current.delivered.get()
                assert current.order == [{"fresh": True}, {"new": True}]

    async def test_never_established_admission_is_not_adopted_while_cleanup_is_held(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        lost: asyncio.Queue[str] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            del error
            await lost.put(topic)

        gate = ProcessorGate(hold_exit=True)
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
                on_topic_lost=on_lost,
            ) as client,
        ):
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            old = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(client, old) as admission:
                    await phoenix_server.deferred_joins.get()
                    admission.cancel()
                    await gate.exiting.wait()
                    await phoenix_server.close_all_clients()
                    await reconnects.get()
                    assert phoenix_server.join_topics == [TOPIC]
                    assert old.order == []
                    assert lost.empty()
                    # A new name owner is usable while old physical cleanup waits.
                    fresh = JoinProbe(client)
                    await client.subscribe_to_topic(
                        TOPIC, fresh.receive, on_join_response=fresh.install
                    )
                    _, request = await phoenix_server.joined.get()
                    _, request = await phoenix_server.joined.get()
                    await push(phoenix_server, request)
                    await fresh.delivered.get()
                    gate.finish.set()
                    with pytest.raises(asyncio.CancelledError):
                        await admission
            assert client.is_topic_joined(TOPIC)
            assert not client._cleanup_tasks

    async def test_repeated_pending_channel_crashes_remain_transient(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        lost: asyncio.Queue[str] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            del error
            await lost.put(topic)

        async with (
            deadline(),
            make_client(
                phoenix_server, reconnect_policy=FAST_RECONNECT, on_topic_lost=on_lost
            ) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await phoenix_server.joined.get()
            await probe.installed.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            await phoenix_server.crash_channel(TOPIC)
            crashes = 3
            obsolete = []
            for _ in range(crashes):
                _, attempt = await phoenix_server.deferred_joins.get()
                obsolete.append(attempt)
                await push(phoenix_server, attempt, WireEvent.ERROR)
                await reply(phoenix_server, attempt)
            connection, current = await phoenix_server.deferred_joins.get()
            for attempt in obsolete:
                await reply(phoenix_server, attempt, status="error")
            await phoenix_server.release_join(connection, current)
            await probe.installed.get()
            await push(phoenix_server, current)
            await probe.delivered.get()
            assert probe.order == [{}, {}, {}]
            assert len(phoenix_server.join_topics) == crashes + 2
            assert lost.empty()
