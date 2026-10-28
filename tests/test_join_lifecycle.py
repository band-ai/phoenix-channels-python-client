"""Join lifecycle invalidation preserves caller and captured-resource ownership."""

from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, ReplyStatus, WireEvent
from tests.join_support import (
    JoinProbe,
    deadline,
    pending_subscribe,
    push,
    reply,
)
from tests.scheduling import TaskTimerObserver, TaskWakeupGate
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
    async def test_pending_cancel_disposes_admission_and_fences_late_acks(
        self, phoenix_server: FakePhoenixServer
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
                admission.cancel()
                expected = asyncio.CancelledError
                with pytest.raises(expected):
                    await admission
            assert not client.get_current_subscriptions()
            assert old.order == []
            current_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((current_id, TOPIC))
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"new": True},
            }
            replacement = JoinProbe(client)
            async with pending_subscribe(client, replacement) as admission:
                connection, current = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await reply(phoenix_server, request, status=ReplyStatus.ERROR)
                await push(
                    phoenix_server,
                    request,
                    WireEvent.REPLY,
                    {"status": ReplyStatus.OK},
                    ref="obsolete-leave",
                )
                await phoenix_server.release_join(connection, current)
                await admission
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{"new": True}, {"replacement": True}]
            assert lost.empty()

    async def test_pending_leave_disposes_admission_and_fences_late_acks(
        self, phoenix_server: FakePhoenixServer
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
                await client.unsubscribe_from_topic(TOPIC)
                expected = PHXConnectionError
                with pytest.raises(expected):
                    await admission
            assert not client.get_current_subscriptions()
            assert old.order == []
            current_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((current_id, TOPIC))
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"new": True},
            }
            replacement = JoinProbe(client)
            async with pending_subscribe(client, replacement) as admission:
                connection, current = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await reply(phoenix_server, request, status=ReplyStatus.ERROR)
                await push(
                    phoenix_server,
                    request,
                    WireEvent.REPLY,
                    {"status": ReplyStatus.OK},
                    ref="obsolete-leave",
                )
                await phoenix_server.release_join(connection, current)
                await admission
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{"new": True}, {"replacement": True}]
            assert lost.empty()

    async def test_pending_shutdown_disposes_admission_and_fences_late_acks(
        self, phoenix_server: FakePhoenixServer
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
                await client.shutdown(STOP_REASON)
                expected = PHXConnectionError
                with pytest.raises(expected):
                    await admission
            assert not client.get_current_subscriptions()
            assert old.order == []
            await client.__aenter__()
            current_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((current_id, TOPIC))
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"new": True},
            }
            replacement = JoinProbe(client)
            async with pending_subscribe(client, replacement) as admission:
                connection, current = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await reply(phoenix_server, request, status=ReplyStatus.ERROR)
                await push(
                    phoenix_server,
                    request,
                    WireEvent.REPLY,
                    {"status": ReplyStatus.OK},
                    ref="obsolete-leave",
                )
                await phoenix_server.release_join(connection, current)
                await admission
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{"new": True}, {"replacement": True}]
            assert lost.empty()

    async def test_pending_disconnect_disposes_admission_and_fences_late_acks(
        self, phoenix_server: FakePhoenixServer
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
                await phoenix_server.close_all_clients()
                expected = PHXConnectionError
                with pytest.raises(expected):
                    await admission
            assert not client.get_current_subscriptions()
            assert old.order == []
            await reconnects.get()
            current_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((current_id, TOPIC))
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"new": True},
            }
            replacement = JoinProbe(client)
            async with pending_subscribe(client, replacement) as admission:
                connection, current = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await reply(phoenix_server, request, status=ReplyStatus.ERROR)
                await push(
                    phoenix_server,
                    request,
                    WireEvent.REPLY,
                    {"status": ReplyStatus.OK},
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

    @pytest.mark.parametrize(
        ("event", "error_type"),
        [(WireEvent.ERROR, PHXConnectionError), (WireEvent.CLOSE, PHXTopicError)],
    )
    async def test_lifecycle_loss_before_success_prevents_installation(
        self,
        phoenix_server: FakePhoenixServer,
        event: WireEvent,
        error_type: type[Exception],
    ) -> None:
        async with deadline(), make_client(phoenix_server) as client:
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            async with pending_subscribe(client, probe) as admission:
                _, request = await phoenix_server.deferred_joins.get()
                await push(phoenix_server, request, event)
                await reply(phoenix_server, request)
                with pytest.raises(error_type):
                    await admission
            assert probe.order == []
            assert not client.is_topic_joined(TOPIC)
            assert not client.get_current_subscriptions()

    async def test_success_then_error_revokes_readiness_and_recovers(
        self, phoenix_server: FakePhoenixServer
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
                await reply(phoenix_server, request)
                await push(phoenix_server, request, WireEvent.ERROR)
                await admission
            connection, recovery = await phoenix_server.deferred_joins.get()
            assert not client.is_topic_joined(TOPIC)
            assert probe.order == [None]
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"recovered": True},
            }
            await phoenix_server.release_join(connection, recovery)
            await probe.installed.get()
            await probe.installed.get()
            await push(phoenix_server, recovery, payload={"delta": True})
            await probe.delivered.get()
            assert probe.order == [None, {"recovered": True}, {"delta": True}]

    async def test_success_then_close_revokes_readiness_and_allows_replacement(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        lost: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            await lost.put((topic, error))

        async with (
            deadline(),
            make_client(phoenix_server, on_topic_lost=on_lost) as client,
        ):
            client_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((client_id, TOPIC))
            probe = JoinProbe(client)
            async with pending_subscribe(client, probe) as admission:
                _, request = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, request)
                await push(phoenix_server, request, WireEvent.CLOSE)
                await admission
            topic, error = await lost.get()
            assert topic == TOPIC
            assert isinstance(error, PHXTopicError)
            assert not client.is_topic_joined(TOPIC)
            assert not client.get_current_subscriptions()
            assert probe.order == [None]
            phoenix_server.defer_join_targets.discard((client_id, TOPIC))
            replacement = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, replacement.receive, on_join_response=replacement.install
            )
            await phoenix_server.joined.get()
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{}, {"replacement": True}]
            assert lost.empty()

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

    async def test_message_callback_shutdown_unwinds_without_cleanup_cycle(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        unwound = asyncio.Event()
        async with deadline(), make_client(phoenix_server) as client:

            async def callback(message: ChannelMessage) -> None:
                del message
                try:
                    await client.shutdown(STOP_REASON)
                finally:
                    unwound.set()

            await client.subscribe_to_topic(TOPIC, callback)
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await unwound.wait()
            await client.shutdown(STOP_REASON)

    async def test_message_callback_unsubscribe_unwinds_without_cleanup_cycle(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        unwound = asyncio.Event()
        async with deadline(), make_client(phoenix_server) as client:

            async def callback(message: ChannelMessage) -> None:
                del message
                try:
                    await client.unsubscribe_from_topic(TOPIC)
                finally:
                    unwound.set()

            await client.subscribe_to_topic(TOPIC, callback)
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await unwound.wait()
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
                "status": ReplyStatus.OK,
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

    async def test_registered_never_established_admission_is_excluded_from_reconnect(
        self, phoenix_server: FakePhoenixServer
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
            required = JoinProbe(client, OTHER_TOPIC)
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            initial = JoinProbe(client)
            async with pending_subscribe(client, initial) as admission:
                await phoenix_server.deferred_joins.get()
                await phoenix_server.joined.get()
                owner = client.get_current_subscriptions()[TOPIC]
                gate = TaskWakeupGate(admission)
                with gate.installed():
                    await phoenix_server.close_all_clients()
                    await gate.held.wait()
                    await reconnects.get()
                    _, request = await phoenix_server.joined.get()
                    assert request.topic == OTHER_TOPIC
                    await push(phoenix_server, request, payload={"required": True})
                    await required.delivered.get()
                    assert client.get_current_subscriptions()[TOPIC] is owner
                    assert not owner.established
                    assert not client.is_topic_joined(TOPIC)
                    assert phoenix_server.join_topics == [
                        OTHER_TOPIC,
                        TOPIC,
                        OTHER_TOPIC,
                    ]
                    assert initial.order == []
                    assert required.order == [{}, {}, {"required": True}]
                    assert lost.empty()
                with pytest.raises(PHXConnectionError):
                    await admission
            assert TOPIC not in client.get_current_subscriptions()
            replacement = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, replacement.receive, on_join_response=replacement.install
            )
            _, current = await phoenix_server.joined.get()
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{}, {"replacement": True}]

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
                await reply(phoenix_server, attempt, status=ReplyStatus.ERROR)
            await phoenix_server.release_join(connection, current)
            await probe.installed.get()
            await push(phoenix_server, current)
            await probe.delivered.get()
            assert probe.order == [{}, {}, {}]
            assert len(phoenix_server.join_topics) == crashes + 2
            assert lost.empty()

    @pytest.mark.clock
    async def test_leave_during_retry_backoff_finishes_recovery(
        self, clocked_peer: FakePhoenixServer
    ) -> None:
        timer = TaskTimerObserver(TopicRuntimeMixin._recover_channel.__code__)
        async with make_client(clocked_peer) as client:
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await clocked_peer.joined.get()
            with timer.installed():
                await clocked_peer.crash_channel(TOPIC)
                recovery = await timer.registered.get()
                assert not client.is_topic_joined(TOPIC)
                await client.unsubscribe_from_topic(TOPIC)
                await clocked_peer.left.get()
                await asyncio.gather(recovery, return_exceptions=True)
            assert recovery.done()
            assert TOPIC not in client.get_current_subscriptions()
            assert clocked_peer.join_topics == [TOPIC]
            assert probe.order == [{}]

    @pytest.mark.clock
    async def test_shutdown_during_retry_backoff_finishes_recovery(
        self, clocked_peer: FakePhoenixServer
    ) -> None:
        timer = TaskTimerObserver(TopicRuntimeMixin._recover_channel.__code__)
        async with make_client(clocked_peer) as client:
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await clocked_peer.joined.get()
            with timer.installed():
                await clocked_peer.crash_channel(TOPIC)
                recovery = await timer.registered.get()
                assert not client.is_topic_joined(TOPIC)
                await client.shutdown(STOP_REASON)
                await asyncio.gather(recovery, return_exceptions=True)
            assert recovery.done()
            assert TOPIC not in client.get_current_subscriptions()
            assert clocked_peer.join_topics == [TOPIC]
            assert probe.order == [{}]
