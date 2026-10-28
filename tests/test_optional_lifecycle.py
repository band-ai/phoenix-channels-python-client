from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, WireEvent
from tests.join_support import (
    JoinProbe,
    ProcessorGate,
    RecoveryFailures,
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


@each_protocol
class TestOptionalLifecycle:
    async def test_optional_initial_admission_still_belongs_to_its_caller(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with deadline(), make_client(phoenix_server) as client:
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            admission = asyncio.create_task(
                client.subscribe_to_topic(
                    TOPIC,
                    probe.receive,
                    on_join_response=probe.install,
                    blocks_reconnect=False,
                )
            )
            try:
                connection, request = await phoenix_server.deferred_joins.get()
                assert not admission.done()
                assert not client.is_topic_joined(TOPIC)
                await phoenix_server.release_join(connection, request)
                await admission
                await push(phoenix_server, request)
                await probe.delivered.get()
            finally:
                if not admission.done():
                    admission.cancel()
                await asyncio.gather(admission, return_exceptions=True)

    @pytest.mark.parametrize("action", ["leave", "shutdown"])
    async def test_held_optional_recovery_is_awaited_and_cannot_erase_replacement(
        self, phoenix_server: FakePhoenixServer, action: str
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
            ) as client,
        ):
            original = JoinProbe(client)
            required = JoinProbe(client, OTHER_TOPIC)
            await client.subscribe_to_topic(
                TOPIC,
                original.receive,
                on_join_response=original.install,
                blocks_reconnect=False,
            )
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await phoenix_server.joined.get()
            await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id, TOPIC)
            )
            await phoenix_server.close_all_clients()
            await reconnects.get()
            _, ordinary = await phoenix_server.joined.get()
            _, obsolete = await phoenix_server.deferred_joins.get()
            await phoenix_server.joined.get()
            recovery = client.get_current_subscriptions()[TOPIC].recovery_task
            assert recovery is not None
            if action == "leave":
                await client.unsubscribe_from_topic(TOPIC)
                await phoenix_server.left.get()
                await push(phoenix_server, ordinary)
                await required.delivered.get()
            else:
                await client.shutdown(STOP_REASON)
                await client.__aenter__()
            assert recovery.done()
            assert not client._cleanup_tasks
            phoenix_server.defer_join_targets.clear()
            fresh = JoinProbe(client)
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            async with pending_subscribe(
                client, fresh, blocks_reconnect=False
            ) as admission:
                current_connection, request = await phoenix_server.deferred_joins.get()
                await reply(phoenix_server, obsolete)
                await reply(phoenix_server, obsolete, status="error")
                await phoenix_server.release_join(current_connection, request)
                await admission
            await push(phoenix_server, request, payload={"replacement": True})
            await fresh.delivered.get()
            assert fresh.order == [{}, {"replacement": True}]
            assert original.order == [{}]

    async def test_new_disconnect_disposes_old_optional_owner_before_new_generation(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
            ) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC,
                probe.receive,
                on_join_response=probe.install,
                blocks_reconnect=False,
            )
            await phoenix_server.joined.get()
            await probe.installed.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id, TOPIC)
            )
            await phoenix_server.close_all_clients()
            await reconnects.get()
            _, obsolete = await phoenix_server.deferred_joins.get()
            old_owner = client.get_current_subscriptions()[TOPIC].recovery_task
            assert old_owner is not None
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id, TOPIC)
            )
            await phoenix_server.close_all_clients()
            await reconnects.get()
            connection, current = await phoenix_server.deferred_joins.get()
            assert old_owner.done()
            assert (
                client.get_current_subscriptions()[TOPIC].recovery_task is not old_owner
            )
            await reply(phoenix_server, obsolete)
            await reply(phoenix_server, obsolete, status="error")
            phoenix_server.join_payloads[TOPIC] = {
                "status": "ok",
                "response": {"latest": True},
            }
            await phoenix_server.release_join(connection, current)
            await probe.installed.get()
            await push(phoenix_server, current)
            await probe.delivered.get()
            assert probe.order == [{}, {"latest": True}, {}]
            expected_joins = 3
            assert len(phoenix_server.join_topics) == expected_joins

    async def test_late_optional_rejection_replaces_only_from_loss_callback(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        replaced = asyncio.Event()
        losses: list[Exception] = []

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            assert topic not in client.get_current_subscriptions()
            losses.append(error)
            phoenix_server.fail_join_targets.clear()
            await client.subscribe_to_topic(
                topic,
                replacement.receive,
                on_join_response=replacement.install,
                blocks_reconnect=False,
            )
            replaced.set()

        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
                on_topic_lost=on_lost,
            ) as client,
        ):
            original = JoinProbe(client)
            replacement = JoinProbe(client)
            required = JoinProbe(client, OTHER_TOPIC)
            await client.subscribe_to_topic(
                TOPIC, on_join_response=original.install, blocks_reconnect=False
            )
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await phoenix_server.joined.get()
            await phoenix_server.joined.get()
            target = (phoenix_server.next_client_id, TOPIC)
            phoenix_server.defer_join_targets.add(target)
            phoenix_server.fail_join_targets.add(target)
            await phoenix_server.close_all_clients()
            await reconnects.get()
            _, ordinary = await phoenix_server.joined.get()
            connection, rejected = await phoenix_server.deferred_joins.get()
            await phoenix_server.joined.get()
            assert TOPIC in client.get_current_subscriptions()
            assert not client.is_topic_joined(TOPIC)
            await push(phoenix_server, ordinary)
            await required.delivered.get()
            await phoenix_server.release_join(connection, rejected)
            await replaced.wait()
            _, current = await phoenix_server.joined.get()
            await push(phoenix_server, current)
            await replacement.delivered.get()
            assert len(losses) == 1
            assert isinstance(losses[0], PHXTopicError)
            assert original.order == [{}]

    @pytest.mark.clock
    @pytest.mark.parametrize("action", ["recover", "leave", "shutdown"])
    async def test_optional_deadline_and_crashes_share_owned_controlled_backoff(
        self, clock: Clock, clocked_peer: FakePhoenixServer, action: str
    ) -> None:
        join_deadline = 10.0
        disconnects: asyncio.Queue[None] = asyncio.Queue()
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        async with make_client(
            clocked_peer,
            heartbeat_interval_s=None,
            join_timeout_s=join_deadline,
            reconnect_policy=FAST_RECONNECT,
            on_disconnect=lambda _: disconnects.put(None),
            on_reconnect=lambda: reconnects.put(None),
        ) as client:
            optional = JoinProbe(client)
            required = JoinProbe(client, OTHER_TOPIC)
            await client.subscribe_to_topic(
                TOPIC,
                optional.receive,
                on_join_response=optional.install,
                blocks_reconnect=False,
            )
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            await asyncio.gather(
                clocked_peer.joined.get(),
                clocked_peer.joined.get(),
                optional.installed.get(),
            )
            clocked_peer.defer_join_targets.add((clocked_peer.next_client_id, TOPIC))
            with RecoveryFailures().attached(client.logger) as failures:
                await clocked_peer.close_all_clients()
                await disconnects.get()
                await clock.advance(FAST_RECONNECT.max_delay_s)
                await reconnects.get()
                _, ordinary = await clocked_peer.joined.get()
                _, obsolete = await clocked_peer.deferred_joins.get()
                await clock.advance(join_deadline)
                await failures.receipts.get()
                recovery = client.get_current_subscriptions()[TOPIC].recovery_task
                assert recovery is not None
                crashes = 2
                for _ in range(crashes):
                    await clock.advance(FAST_RECONNECT.max_delay_s)
                    _, attempt = await clocked_peer.deferred_joins.get()
                    await reply(clocked_peer, obsolete)
                    await push(clocked_peer, attempt, WireEvent.ERROR)
                    await failures.receipts.get()
                    obsolete = attempt
                if action == "recover":
                    await clock.advance(FAST_RECONNECT.max_delay_s)
                    connection, current = await clocked_peer.deferred_joins.get()
                    await clocked_peer.release_join(connection, current)
                    await optional.installed.get()
                    await push(clocked_peer, current)
                    await optional.delivered.get()
                    assert optional.order == [{}, {}, {}]
                else:
                    before_stop = list(clocked_peer.join_topics)
                    if action == "leave":
                        await client.unsubscribe_from_topic(TOPIC)
                    else:
                        await client.shutdown(STOP_REASON)
                    assert recovery.done()
                    await clock.advance(join_deadline)
                    if action == "leave":
                        await push(clocked_peer, ordinary)
                        await required.delivered.get()
                    assert clocked_peer.join_topics == before_stop
                    assert TOPIC not in client.get_current_subscriptions()
                    assert not client._cleanup_tasks

    async def test_socket_invalidation_prevents_a_queued_initial_snapshot_install(
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
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            _, ordinary = await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            initial = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(
                    client, initial, blocks_reconnect=False
                ) as admission:
                    _, request = await phoenix_server.deferred_joins.get()
                    await gate.started.wait()
                    await reply(phoenix_server, request)
                    await push(phoenix_server, ordinary)
                    await required.delivered.get()
                    await phoenix_server.close_all_clients()
                    with pytest.raises(PHXConnectionError):
                        await admission
                await reconnects.get()
            assert initial.order == []
            assert TOPIC not in client.get_current_subscriptions()
            assert phoenix_server.join_topics == [OTHER_TOPIC, TOPIC, OTHER_TOPIC]

    @pytest.mark.parametrize("event", [WireEvent.ERROR, WireEvent.CLOSE])
    @pytest.mark.parametrize("success_first", [False, True])
    async def test_pending_established_lifecycle_frames_keep_serial_failure_policy(
        self,
        phoenix_server: FakePhoenixServer,
        event: WireEvent,
        *,
        success_first: bool,
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        losses: asyncio.Queue[Exception] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            await losses.put(error)

        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
                on_topic_lost=on_lost,
            ) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC,
                probe.receive,
                on_join_response=probe.install,
                blocks_reconnect=False,
            )
            await probe.installed.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id, TOPIC)
            )
            await phoenix_server.close_all_clients()
            await reconnects.get()
            _, pending = await phoenix_server.deferred_joins.get()
            if success_first:
                await reply(phoenix_server, pending)
            await push(phoenix_server, pending, event)
            if not success_first:
                await reply(phoenix_server, pending)
            expected = [{}, None] if success_first else [{}]
            if event == WireEvent.CLOSE:
                assert isinstance(await losses.get(), PHXTopicError)
                assert TOPIC not in client.get_current_subscriptions()
                assert probe.order == expected
            else:
                connection, retry = await phoenix_server.deferred_joins.get()
                assert not client.is_topic_joined(TOPIC)
                assert probe.order == expected
                assert losses.empty()
                await phoenix_server.release_join(connection, retry)
                # Successful first admission and retries share the same hook queue.
                if success_first:
                    assert await probe.installed.get() is None
                assert await probe.installed.get() == {}
                await push(phoenix_server, retry, payload={"recovered": True})
                await probe.delivered.get()
                assert probe.order == [*expected, {}, {"recovered": True}]
