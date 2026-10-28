from __future__ import annotations

import asyncio
from typing import cast

import pytest

from phoenix_channels_python_client import JoinResponseCallback
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer
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
    REJOINED_ONCE,
    TOPIC,
    each_protocol,
    make_client,
)


def throwing_hook(error: BaseException) -> JoinResponseCallback:
    def install(response: object) -> None:
        del response
        raise error

    return install


@each_protocol
class TestJoinInstallation:
    @pytest.mark.parametrize(
        "error",
        [ValueError("invalid snapshot"), asyncio.CancelledError("hook failure")],
    )
    async def test_initial_hook_failure_preserves_cause_and_allows_replacement(
        self, phoenix_server: FakePhoenixServer, error: BaseException
    ) -> None:
        lost: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

        async def on_lost(topic: str, failure: Exception) -> None:
            await lost.put((topic, failure))

        async with (
            deadline(),
            make_client(phoenix_server, on_topic_lost=on_lost) as client,
        ):
            with pytest.raises(PHXTopicError) as caught:
                await client.subscribe_to_topic(
                    TOPIC, on_join_response=throwing_hook(error)
                )
            assert caught.value.__cause__ is error
            assert TOPIC not in client.get_current_subscriptions()
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await phoenix_server.joined.get()
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await probe.delivered.get()
            assert lost.empty()

    @pytest.mark.parametrize("wrapped", [False, True])
    async def test_async_installers_are_never_run(
        self, phoenix_server: FakePhoenixServer, *, wrapped: bool
    ) -> None:
        ran = asyncio.Event()

        async def asynchronous(response: object) -> None:
            del response
            ran.set()

        hook = (lambda response: asynchronous(response)) if wrapped else asynchronous
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(
                    TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                )
            assert not ran.is_set()
            assert not client.get_current_subscriptions()
            assert len(phoenix_server.join_topics) == int(wrapped)

    @pytest.mark.parametrize("external_future", [False, True])
    async def test_non_none_results_fail_without_canceling_external_work(
        self, phoenix_server: FakePhoenixServer, *, external_future: bool
    ) -> None:
        future = asyncio.get_running_loop().create_future()
        result = future if external_future else "invalid return"
        hook = cast("JoinResponseCallback", lambda _: result)
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(TOPIC, on_join_response=hook)
            assert not future.cancelled()
            assert not client.is_topic_joined(TOPIC)

    @pytest.mark.parametrize("socket_reconnect", [False, True])
    async def test_established_installation_failure_is_permanent_once(
        self, phoenix_server: FakePhoenixServer, *, socket_reconnect: bool
    ) -> None:
        lost: asyncio.Queue[Exception] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            await lost.put(error)

        calls = 0

        def install(response: object) -> None:
            nonlocal calls
            del response
            calls += 1
            if calls > 1:
                raise ValueError("invalid recovery snapshot")

        async with (
            deadline(),
            make_client(
                phoenix_server, on_topic_lost=on_lost, reconnect_policy=FAST_RECONNECT
            ) as client,
        ):
            await client.subscribe_to_topic(TOPIC, on_join_response=install)
            if socket_reconnect:
                await phoenix_server.close_all_clients()
            else:
                await phoenix_server.crash_channel(TOPIC)
            assert isinstance(await lost.get(), PHXTopicError)
            assert not client.get_current_subscriptions()
            probe = JoinProbe(client)
            await client.subscribe_to_topic(TOPIC, on_join_response=probe.install)
            assert calls == REJOINED_ONCE
            assert lost.empty()

    @pytest.mark.parametrize("overflow", [False, True])
    async def test_queue_loss_before_installation_fails_closed(
        self, phoenix_server: FakePhoenixServer, *, overflow: bool
    ) -> None:
        capacity = 2
        barrier = asyncio.Event()
        gate = ProcessorGate(hold_start=True)
        async with (
            deadline(),
            make_client(phoenix_server, max_topic_queue_size=capacity) as client,
        ):
            # This independent topic proves the router has consumed the burst.
            async def routed(message: ChannelMessage) -> None:
                del message
                barrier.set()

            await client.subscribe_to_topic(OTHER_TOPIC, routed)
            _, independent = await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(client, probe) as admission:
                    _, request = await phoenix_server.deferred_joins.get()
                    await gate.started.wait()
                    for _ in range(capacity if overflow else capacity - 1):
                        await push(phoenix_server, request)
                    await reply(phoenix_server, request)
                    await push(phoenix_server, independent)
                    await barrier.wait()
                    gate.start.set()
                    if overflow:
                        with pytest.raises(PHXConnectionError, match="Messages lost"):
                            await admission
                        assert probe.order == []
                        assert not client.is_topic_joined(TOPIC)
                    else:
                        await admission
                        await push(phoenix_server, request, payload={"after": True})
                        await probe.delivered.get()
                        assert probe.order == [None, {"after": True}]

    async def test_established_overflow_retries_with_a_new_snapshot(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        capacity = 2
        barrier: asyncio.Queue[None] = asyncio.Queue()
        gate = ProcessorGate(hold_start=True)

        async def routed(message: ChannelMessage) -> None:
            del message
            await barrier.put(None)

        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                max_topic_queue_size=capacity,
            ) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await client.subscribe_to_topic(OTHER_TOPIC, routed)
            await phoenix_server.joined.get()
            _, independent = await phoenix_server.joined.get()
            await probe.installed.get()
            client_id = phoenix_server.next_client_id - 1
            phoenix_server.defer_join_targets.add((client_id, TOPIC))
            with gate.installed():
                await phoenix_server.crash_channel(TOPIC)
                _, request = await phoenix_server.deferred_joins.get()
                await gate.started.wait()
                for _ in range(capacity):
                    await push(phoenix_server, request)
                await reply(phoenix_server, request)
                await push(phoenix_server, independent)
                await barrier.get()
                phoenix_server.defer_join_targets.discard((client_id, TOPIC))
                phoenix_server.join_payloads[TOPIC] = {
                    "status": "ok",
                    "response": {"fresh": True},
                }
                gate.start.set()
                assert await probe.installed.get() == {"fresh": True}
            assert probe.order == [{}, {"fresh": True}]
            # Actual post-recovery delivery, rather than a task-state assertion.
            _, request = await phoenix_server.joined.get()
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request)
            await probe.delivered.get()

    @pytest.mark.clock
    async def test_dropped_success_reply_settles_at_the_controlled_join_deadline(
        self, clock: Clock, clocked_peer: FakePhoenixServer
    ) -> None:
        capacity = 2
        join_deadline = 10.0
        barrier = asyncio.Event()
        gate = ProcessorGate(hold_start=True)

        async def routed(message: ChannelMessage) -> None:
            del message
            barrier.set()

        async with make_client(
            clocked_peer, join_timeout_s=join_deadline, max_topic_queue_size=capacity
        ) as client:
            await client.subscribe_to_topic(OTHER_TOPIC, routed)
            _, independent = await clocked_peer.joined.get()
            clocked_peer.defer_join_targets.add(
                (clocked_peer.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(client, probe) as admission:
                    _, request = await clocked_peer.deferred_joins.get()
                    await gate.started.wait()
                    await reply(clocked_peer, request)
                    for _ in range(capacity):
                        await push(clocked_peer, request)
                    await push(clocked_peer, independent)
                    await barrier.wait()
                    gate.start.set()
                    await clock.advance(join_deadline)
                    with pytest.raises(PHXTopicError, match="Timed out"):
                        await admission
                assert probe.order == []
                assert TOPIC not in client.get_current_subscriptions()
