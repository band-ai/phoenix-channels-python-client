"""Join installation rejects invalid hooks and known pre-installation loss."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Coroutine
from typing import Any, cast

import pytest

from phoenix_channels_python_client import JoinResponseCallback
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, ReplyStatus
from tests.join_hooks import AsyncInstaller
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
        [
            ValueError("private snapshot content"),
            asyncio.CancelledError("private hook details"),
        ],
    )
    async def test_initial_hook_failure_preserves_cause_and_allows_replacement(
        self,
        phoenix_server: FakePhoenixServer,
        error: BaseException,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.ERROR, logger="phoenix_channels_python_client.client")
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
            assert caplog.record_tuples == [
                (
                    "phoenix_channels_python_client.client",
                    logging.ERROR,
                    f"Join response installation failed [{type(error).__name__}]",
                )
            ]
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

    async def test_async_function_is_rejected_before_join(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        ran = asyncio.Event()

        async def asynchronous(response: object) -> None:
            del response
            ran.set()

        hook = asynchronous
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(
                    TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                )
            assert not ran.is_set()
            assert not client.get_current_subscriptions()
            assert len(phoenix_server.join_topics) == 0

    async def test_wrapped_coroutine_is_closed_without_running(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        ran = asyncio.Event()

        async def asynchronous(response: object) -> None:
            del response
            ran.set()

        result = asynchronous(None)

        def hook(response: object) -> Coroutine[Any, Any, None]:
            del response
            return result

        try:
            async with deadline(), make_client(phoenix_server) as client:
                with pytest.raises(PHXTopicError):
                    await client.subscribe_to_topic(
                        TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                    )
                assert inspect.getcoroutinestate(result) == inspect.CORO_CLOSED
                assert not ran.is_set()
                assert not client.get_current_subscriptions()
                assert len(phoenix_server.join_topics) == 1
        finally:
            result.close()

    async def test_async_callable_instance_is_rejected_before_join(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        hook = AsyncInstaller()
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(
                    TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                )
            assert not hook.ran.is_set()
            assert not client.get_current_subscriptions()
            assert phoenix_server.join_topics == []

    async def test_invalid_return_fails_installation(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        hook = cast("JoinResponseCallback", lambda _: "invalid return")
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(TOPIC, on_join_response=hook)
            assert not client.get_current_subscriptions()

    async def test_returned_future_is_not_canceled(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        hook = cast("JoinResponseCallback", lambda _: future)
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(TOPIC, on_join_response=hook)
            assert not future.cancelled()
            assert not client.get_current_subscriptions()

    async def test_returned_task_remains_owned_until_its_owner_releases_it(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        started, release = asyncio.Event(), asyncio.Event()
        result = object()

        async def external_work() -> object:
            started.set()
            await release.wait()
            return result

        task = asyncio.create_task(external_work())
        hook = cast("JoinResponseCallback", lambda _: task)
        try:
            async with deadline(), make_client(phoenix_server) as client:
                await started.wait()
                with pytest.raises(PHXTopicError):
                    await client.subscribe_to_topic(TOPIC, on_join_response=hook)
                assert not task.done()
                assert not client.get_current_subscriptions()
                release.set()
                assert await task is result
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    async def test_socket_rejoin_hook_failure_preserves_cause_and_loses_once(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        lost: asyncio.Queue[Exception] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            await lost.put(error)

        installed: list[object] = []
        failure = ValueError("private snapshot content")

        def install(response: object) -> None:
            if installed:
                raise failure
            installed.append(response)

        async with (
            deadline(),
            make_client(
                phoenix_server, on_topic_lost=on_lost, reconnect_policy=FAST_RECONNECT
            ) as client,
        ):
            await client.subscribe_to_topic(TOPIC, on_join_response=install)
            await phoenix_server.close_all_clients()
            error = await lost.get()
            assert isinstance(error, PHXTopicError)
            assert isinstance(error.__cause__, PHXTopicError)
            assert error.__cause__.__cause__ is failure
            assert not client.get_current_subscriptions()
            probe = JoinProbe(client)
            await client.subscribe_to_topic(TOPIC, on_join_response=probe.install)
            assert installed == [{}]
            assert lost.empty()

    async def test_channel_recovery_hook_failure_preserves_cause_and_loses_once(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        lost: asyncio.Queue[Exception] = asyncio.Queue()

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            await lost.put(error)

        installed: list[object] = []
        failure = ValueError("private snapshot content")

        def install(response: object) -> None:
            if installed:
                raise failure
            installed.append(response)

        async with (
            deadline(),
            make_client(
                phoenix_server, on_topic_lost=on_lost, reconnect_policy=FAST_RECONNECT
            ) as client,
        ):
            await client.subscribe_to_topic(TOPIC, on_join_response=install)
            await phoenix_server.crash_channel(TOPIC)
            error = await lost.get()
            assert isinstance(error, PHXTopicError)
            assert isinstance(error.__cause__, PHXTopicError)
            assert error.__cause__.__cause__ is failure
            assert not client.get_current_subscriptions()
            probe = JoinProbe(client)
            await client.subscribe_to_topic(TOPIC, on_join_response=probe.install)
            assert installed == [{}]
            assert lost.empty()

    async def test_preinstall_queue_at_capacity_installs(
        self, phoenix_server: FakePhoenixServer
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
                    for _ in range(capacity - 1):
                        await push(phoenix_server, request)
                    await reply(phoenix_server, request)
                    await push(phoenix_server, independent)
                    await barrier.wait()
                    gate.start.set()
                    await admission
                    await push(phoenix_server, request, payload={"after": True})
                    await probe.delivered.get()
                    assert probe.order == [None, {"after": True}]

    async def test_preinstall_overflow_fails_closed(
        self, phoenix_server: FakePhoenixServer
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
                    for _ in range(capacity):
                        await push(phoenix_server, request)
                    await reply(phoenix_server, request)
                    await push(phoenix_server, independent)
                    await barrier.wait()
                    gate.start.set()
                    with pytest.raises(PHXConnectionError, match="Messages lost"):
                        await admission
                    assert probe.order == []
                    assert not client.is_topic_joined(TOPIC)

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
                    "status": ReplyStatus.OK,
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
