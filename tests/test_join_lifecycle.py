"""Join lifecycle invalidation preserves caller and captured-resource ownership."""

from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.exceptions import ChannelError, SocketError
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from tests.cleanup_support import HeldCleanup
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, ReplyStatus, WireEvent
from tests.join_support import (
    JoinProbe,
    ProcessorGate,
    deadline,
    pending_subscribe,
    push,
    reply,
)
from tests.scheduling import TaskTimerObserver, TaskWakeupGate
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    STOP_REASON,
    TOPIC,
    channel_for,
    each_protocol,
    is_joined,
    join_channel,
    make_client,
    wait_for_condition,
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
            assert not is_joined(client, TOPIC)
            assert TOPIC in client.topic_subscriptions
            with pytest.raises(ChannelError):
                await client.channel(TOPIC).join()
            await push(phoenix_server, request, WireEvent.CLOSE, ref=request.ref)
            assert await wait_for_condition(
                lambda: TOPIC not in client.topic_subscriptions
            )
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
                await channel_for(client, TOPIC).leave()
                expected = ChannelError
                with pytest.raises(expected):
                    await admission
            assert not is_joined(client, TOPIC)
            assert TOPIC in client.topic_subscriptions
            with pytest.raises(ChannelError):
                await client.channel(TOPIC).join()
            await push(phoenix_server, request, WireEvent.CLOSE, ref=request.ref)
            assert await wait_for_condition(
                lambda: TOPIC not in client.topic_subscriptions
            )
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
                expected = ChannelError
                with pytest.raises(expected):
                    await admission
            assert not client.topic_subscriptions.copy()
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
                expected = SocketError
                with pytest.raises(expected):
                    await admission
            assert not client.topic_subscriptions.copy()
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
                with pytest.raises(ChannelError, match="Timed out"):
                    await admission
            assert not is_joined(client, TOPIC)
            assert TOPIC in client.topic_subscriptions
            with pytest.raises(ChannelError):
                await client.channel(TOPIC).join()
            owner = client.topic_subscriptions[TOPIC]
            await clocked_peer.release_join(connection, request)
            await clocked_peer.close_channel(TOPIC)
            await owner.terminal_received.wait()
            assert TOPIC not in client.topic_subscriptions
            await join_channel(client, TOPIC, on_join_response=probe.install)
            assert probe.order == [{}]

    @pytest.mark.parametrize(
        ("event", "error_type"),
        [(WireEvent.ERROR, ChannelError), (WireEvent.CLOSE, ChannelError)],
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
            assert not is_joined(client, TOPIC)
            assert not client.topic_subscriptions.copy()

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
            assert not is_joined(client, TOPIC)
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
            assert isinstance(error, ChannelError)
            assert not is_joined(client, TOPIC)
            assert not client.topic_subscriptions.copy()
            assert probe.order == [None]
            phoenix_server.defer_join_targets.discard((client_id, TOPIC))
            replacement = JoinProbe(client)
            await join_channel(
                client, TOPIC, replacement.receive, on_join_response=replacement.install
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
        async with (
            deadline(),
            make_client(
                phoenix_server, callback_drain_timeout_s=JOIN_TIMEOUT_S
            ) as client,
        ):
            held = HeldCleanup(client)
            waiters: list[asyncio.Task[None]] = []
            try:
                await join_channel(client, TOPIC, held.receive)
                _, request = await phoenix_server.joined.get()
                await push(phoenix_server, request)
                await held.running.wait()
                leave = asyncio.create_task(channel_for(client, TOPIC).leave())
                waiters.append(leave)
                await phoenix_server.left.get()
                leave.cancel()
                await held.finalizing.wait()
                leave.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await leave
                with pytest.raises(ChannelError):
                    await client.channel(TOPIC).join()
                replacement = JoinProbe(client, OTHER_TOPIC)
                await join_channel(
                    client,
                    OTHER_TOPIC,
                    replacement.receive,
                    on_join_response=replacement.install,
                )
                _, current = await phoenix_server.joined.get()
                await push(phoenix_server, current)
                await replacement.delivered.get()
                processor = client.topic_subscriptions[
                    OTHER_TOPIC
                ].process_topic_messages_task
                assert processor is not None
                shutdown = asyncio.create_task(client.shutdown(STOP_REASON))
                waiters.append(shutdown)
                await asyncio.gather(processor, return_exceptions=True)
                assert not shutdown.done()
                assert not held.finished.is_set()
                held.release.set()
                await held.finished.wait()
                await shutdown
                assert not client.cleanup_tasks
                assert not client.topic_subscriptions.copy()
            finally:
                held.release.set()
                if held.callback is not None:
                    held.callback.cancel()
                    waiters.append(held.callback)
                for waiter in waiters:
                    if not waiter.done():
                        waiter.cancel()
                await asyncio.gather(*waiters, return_exceptions=True)

    async def test_message_callback_shutdown_unwinds_without_cleanup_cycle(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        unwound = asyncio.Event()
        async with deadline(), make_client(phoenix_server) as client:

            async def callback(message: Message) -> None:
                del message
                try:
                    await client.shutdown(STOP_REASON)
                finally:
                    unwound.set()

            run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
            try:
                await join_channel(client, TOPIC, callback)
                _, request = await phoenix_server.joined.get()
                await push(phoenix_server, request)
                await unwound.wait()
                await run
                assert not client.topic_subscriptions.copy()
                assert not client.cleanup_tasks
            finally:
                run.cancel()
                await asyncio.gather(run, return_exceptions=True)

    async def test_message_callback_unsubscribe_unwinds_without_cleanup_cycle(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        unwound = asyncio.Event()
        async with deadline(), make_client(phoenix_server) as client:

            async def callback(message: Message) -> None:
                del message
                try:
                    await channel_for(client, TOPIC).leave()
                finally:
                    unwound.set()

            await join_channel(client, TOPIC, callback)
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await unwound.wait()
            # V1 cannot fence unsolicited lifecycle frames by join_ref.
            # This wire round-trip drains the old leave before re-entry.
            await join_channel(client, OTHER_TOPIC)
            await phoenix_server.joined.get()
            probe = JoinProbe(client)
            await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
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
            run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
            try:
                await join_channel(client, TOPIC)
                await phoenix_server.close_channel(TOPIC)
                await unwound.wait()
                await run
                assert not client.topic_subscriptions.copy()
                assert not client.cleanup_tasks
            finally:
                run.cancel()
                await asyncio.gather(run, return_exceptions=True)

    async def test_fresh_client_starts_with_new_snapshot_after_abrupt_wire_loss(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with deadline(), make_client(phoenix_server, auto_reconnect=False) as old:
            probe = JoinProbe(old)
            await join_channel(
                old, TOPIC, probe.receive, on_join_response=probe.install
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
                await join_channel(
                    fresh, TOPIC, current.receive, on_join_response=current.install
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
            await join_channel(
                client, OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            initial = JoinProbe(client)
            async with pending_subscribe(client, initial) as admission:
                await phoenix_server.deferred_joins.get()
                await phoenix_server.joined.get()
                owner = client.topic_subscriptions.copy()[TOPIC]
                gate = TaskWakeupGate(admission)
                with gate.installed():
                    await phoenix_server.close_all_clients()
                    await gate.held.wait()
                    await reconnects.get()
                    _, request = await phoenix_server.joined.get()
                    assert request.topic == OTHER_TOPIC
                    await push(phoenix_server, request, payload={"required": True})
                    await required.delivered.get()
                    assert TOPIC not in client.topic_subscriptions
                    assert not owner.established
                    assert not is_joined(client, TOPIC)
                    assert phoenix_server.join_topics == [
                        OTHER_TOPIC,
                        TOPIC,
                        OTHER_TOPIC,
                    ]
                    assert initial.order == []
                    assert required.order == [{}, {}, {"required": True}]
                    assert lost.empty()
                with pytest.raises(SocketError):
                    await admission
            assert TOPIC not in client.topic_subscriptions.copy()
            replacement = JoinProbe(client)
            await join_channel(
                client, TOPIC, replacement.receive, on_join_response=replacement.install
            )
            _, current = await phoenix_server.joined.get()
            await push(phoenix_server, current, payload={"replacement": True})
            await replacement.delivered.get()
            assert replacement.order == [{}, {"replacement": True}]

    async def test_socket_loss_after_reader_installation_recovers_the_channel(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        gate = ProcessorGate(hold_start=True)
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
            ) as client,
        ):
            required = JoinProbe(client, OTHER_TOPIC)
            await join_channel(
                client, OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            initial = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(client, initial) as admission:
                    peer, request = await phoenix_server.deferred_joins.get()
                    await phoenix_server.joined.get()
                    await gate.started.wait()
                    await phoenix_server.release_join(peer, request)
                    assert await initial.installed.get() == {}
                    await admission
                    await phoenix_server.close_all_clients()
                gate.start.set()
                await reconnects.get()
            assert is_joined(client, TOPIC)
            assert initial.order == [{}, {}]
            assert phoenix_server.join_topics == [
                OTHER_TOPIC,
                TOPIC,
                OTHER_TOPIC,
                TOPIC,
            ]
            await phoenix_server.simulate_channel_push(TOPIC, EVENT, {"fresh": True})
            await initial.delivered.get()
            assert initial.order == [{}, {}, {"fresh": True}]

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
            await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
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
            await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
            )
            await clocked_peer.joined.get()
            with timer.installed():
                await clocked_peer.crash_channel(TOPIC)
                recovery = await timer.registered.get()
                assert not is_joined(client, TOPIC)
                await channel_for(client, TOPIC).leave()
                assert clocked_peer.left.empty()
                await asyncio.gather(recovery, return_exceptions=True)
            assert recovery.done()
            assert TOPIC not in client.topic_subscriptions.copy()
            assert clocked_peer.join_topics == [TOPIC]
            assert probe.order == [{}]

    @pytest.mark.clock
    async def test_shutdown_during_retry_backoff_finishes_recovery(
        self, clocked_peer: FakePhoenixServer
    ) -> None:
        timer = TaskTimerObserver(TopicRuntimeMixin._recover_channel.__code__)
        async with make_client(clocked_peer) as client:
            probe = JoinProbe(client)
            await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
            )
            await clocked_peer.joined.get()
            with timer.installed():
                await clocked_peer.crash_channel(TOPIC)
                recovery = await timer.registered.get()
                assert not is_joined(client, TOPIC)
                await client.shutdown(STOP_REASON)
                await asyncio.gather(recovery, return_exceptions=True)
            assert recovery.done()
            assert TOPIC not in client.topic_subscriptions.copy()
            assert clocked_peer.join_topics == [TOPIC]
            assert probe.order == [{}]
