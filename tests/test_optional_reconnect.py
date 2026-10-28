from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer
from tests.join_support import JoinProbe, deadline, push
from tests.support import FAST_RECONNECT, OTHER_TOPIC, TOPIC, each_protocol, make_client

JOIN_DEADLINE = 10.0


@each_protocol
class TestOptionalReconnect:
    @pytest.mark.parametrize("hold_callback", [False, True])
    async def test_required_delivery_and_reconnect_finish_while_optional_work_is_held(
        self, phoenix_server: FakePhoenixServer, *, hold_callback: bool
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        running, release, finished = (asyncio.Event() for _ in range(3))
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
                callback_drain_timeout_s=JOIN_DEADLINE,
            ) as client,
        ):
            optional = JoinProbe(client)
            required = JoinProbe(client, OTHER_TOPIC)

            async def receive(message: ChannelMessage) -> None:
                if hold_callback and not running.is_set():
                    running.set()
                    try:
                        await release.wait()
                    finally:
                        finished.set()
                else:
                    await optional.receive(message)

            await client.subscribe_to_topic(
                TOPIC,
                receive,
                on_join_response=optional.install,
                blocks_reconnect=False,
            )
            await client.subscribe_to_topic(
                OTHER_TOPIC, required.receive, on_join_response=required.install
            )
            _, initial = await phoenix_server.joined.get()
            await phoenix_server.joined.get()
            await asyncio.gather(optional.installed.get(), required.installed.get())
            if hold_callback:
                await push(phoenix_server, initial)
                await running.wait()
            else:
                phoenix_server.defer_join_targets.add(
                    (phoenix_server.next_client_id, TOPIC)
                )
            try:
                await phoenix_server.close_all_clients()
                await reconnects.get()
                _, request = await phoenix_server.joined.get()
                assert request.topic == OTHER_TOPIC
                await required.installed.get()
                await push(phoenix_server, request, payload={"required": True})
                assert (await required.delivered.get()).payload == {"required": True}
                assert TOPIC in client.get_current_subscriptions()
                assert not client.is_topic_joined(TOPIC)
                if hold_callback:
                    assert not finished.is_set()
                    release.set()
                    await finished.wait()
                else:
                    connection, held = await phoenix_server.deferred_joins.get()
                    await phoenix_server.release_join(connection, held)
                await optional.installed.get()
                _, current = await phoenix_server.joined.get()
                await push(phoenix_server, current, payload={"optional": True})
                await optional.delivered.get()
                assert optional.order == [{}, {}, {"optional": True}]
            finally:
                release.set()

    @pytest.mark.parametrize("with_topic", [False, True])
    async def test_empty_required_pass_handles_no_topics_and_all_optional_topics(
        self, phoenix_server: FakePhoenixServer, *, with_topic: bool
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
            if with_topic:
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
            if with_topic:
                connection, request = await phoenix_server.deferred_joins.get()
                assert not client.is_topic_joined(TOPIC)
                await phoenix_server.release_join(connection, request)
                await probe.installed.get()
                await push(phoenix_server, request)
                await probe.delivered.get()
            else:
                assert not client.get_current_subscriptions()

    async def test_default_required_first_attempts_remain_sequential(
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
            await client.subscribe_to_topic(TOPIC)
            await client.subscribe_to_topic(OTHER_TOPIC)
            await phoenix_server.joined.get()
            await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id, TOPIC)
            )
            await phoenix_server.close_all_clients()
            connection, request = await phoenix_server.deferred_joins.get()
            assert not reconnects.qsize()
            assert phoenix_server.join_topics == [TOPIC, OTHER_TOPIC, TOPIC]
            await phoenix_server.release_join(connection, request)
            await reconnects.get()
            assert phoenix_server.join_topics == [
                TOPIC,
                OTHER_TOPIC,
                TOPIC,
                OTHER_TOPIC,
            ]

    async def test_required_rejection_is_removed_before_global_reconnect(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        seen: asyncio.Queue[set[str]] = asyncio.Queue()

        async def reconnected() -> None:
            await seen.put(set(client.get_current_subscriptions()))

        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=reconnected,
            ) as client,
        ):
            await client.subscribe_to_topic(TOPIC)
            await client.subscribe_to_topic(OTHER_TOPIC)
            phoenix_server.fail_join_targets.add((phoenix_server.next_client_id, TOPIC))
            await phoenix_server.close_all_clients()
            assert await seen.get() == {OTHER_TOPIC}
            assert client.is_topic_joined(OTHER_TOPIC)

    @pytest.mark.clock
    async def test_transient_required_attempt_releases_barrier_before_retry(
        self, clock: Clock, clocked_peer: FakePhoenixServer
    ) -> None:
        disconnects: asyncio.Queue[None] = asyncio.Queue()
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        async with make_client(
            clocked_peer,
            join_timeout_s=JOIN_DEADLINE,
            reconnect_policy=FAST_RECONNECT,
            on_disconnect=lambda _: disconnects.put(None),
            on_reconnect=lambda: reconnects.put(None),
        ) as client:
            probe = JoinProbe(client)
            await client.subscribe_to_topic(TOPIC, on_join_response=probe.install)
            await probe.installed.get()
            clocked_peer.defer_join_targets.add((clocked_peer.next_client_id, TOPIC))
            await clocked_peer.close_all_clients()
            await disconnects.get()
            await clock.advance(FAST_RECONNECT.max_delay_s)
            connection, obsolete = await clocked_peer.deferred_joins.get()
            await clock.advance(JOIN_DEADLINE)
            await reconnects.get()
            assert not client.is_topic_joined(TOPIC)
            await clocked_peer.release_join(connection, obsolete)
            await clock.advance(FAST_RECONNECT.max_delay_s)
            await probe.installed.get()
            assert probe.order == [{}, {}]
