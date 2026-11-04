"""Join installation validates hooks and precedes application delivery."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Coroutine
from typing import Any, cast

import pytest

from phoenix_channels_python_client import JoinResponseCallback
from phoenix_channels_python_client.exceptions import ChannelError
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from tests.channel_support import UnansweredLeavesPeer
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer, ReplyStatus
from tests.join_hooks import AsyncInstaller
from tests.join_support import (
    JoinProbe,
    ProcessorGate,
    deadline,
    pending_subscribe,
    push,
)
from tests.scheduling import TaskTimerObserver
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    OTHER_TOPIC,
    TOPIC,
    each_protocol,
    is_joined,
    join_channel,
    make_client,
)

LEAVE_DEADLINE = 10.0


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
            with pytest.raises(ChannelError) as caught:
                await join_channel(client, TOPIC, on_join_response=throwing_hook(error))
            assert caught.value.__cause__ is error
            assert caplog.record_tuples == [
                (
                    "phoenix_channels_python_client.client",
                    logging.ERROR,
                    f"Join response installation failed [{type(error).__name__}]",
                )
            ]
            assert TOPIC not in client.topic_subscriptions.copy()
            probe = JoinProbe(client)
            await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
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
            with pytest.raises(ChannelError):
                await join_channel(
                    client, TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                )
            assert not ran.is_set()
            assert not client.topic_subscriptions.copy()
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
                with pytest.raises(ChannelError):
                    await join_channel(
                        client,
                        TOPIC,
                        on_join_response=cast("JoinResponseCallback", hook),
                    )
                assert inspect.getcoroutinestate(result) == inspect.CORO_CLOSED
                assert not ran.is_set()
                assert not client.topic_subscriptions.copy()
                assert len(phoenix_server.join_topics) == 1
        finally:
            result.close()

    async def test_async_callable_instance_is_rejected_before_join(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        hook = AsyncInstaller()
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(ChannelError):
                await join_channel(
                    client, TOPIC, on_join_response=cast("JoinResponseCallback", hook)
                )
            assert not hook.ran.is_set()
            assert not client.topic_subscriptions.copy()
            assert phoenix_server.join_topics == []

    async def test_invalid_return_fails_installation(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        hook = cast("JoinResponseCallback", lambda _: "invalid return")
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(ChannelError):
                await join_channel(client, TOPIC, on_join_response=hook)
            assert not client.topic_subscriptions.copy()

    async def test_returned_future_is_not_canceled(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        hook = cast("JoinResponseCallback", lambda _: future)
        async with deadline(), make_client(phoenix_server) as client:
            with pytest.raises(ChannelError):
                await join_channel(client, TOPIC, on_join_response=hook)
            assert not future.cancelled()
            assert not client.topic_subscriptions.copy()

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
                with pytest.raises(ChannelError):
                    await join_channel(client, TOPIC, on_join_response=hook)
                assert not task.done()
                assert not client.topic_subscriptions.copy()
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
            await join_channel(client, TOPIC, on_join_response=install)
            await phoenix_server.close_all_clients()
            error = await lost.get()
            assert isinstance(error, ChannelError)
            assert isinstance(error.__cause__, ChannelError)
            assert error.__cause__.__cause__ is failure
            assert not client.topic_subscriptions.copy()
            probe = JoinProbe(client)
            await join_channel(client, TOPIC, on_join_response=probe.install)
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
            await join_channel(client, TOPIC, on_join_response=install)
            await phoenix_server.crash_channel(TOPIC)
            error = await lost.get()
            assert isinstance(error, ChannelError)
            assert isinstance(error.__cause__, ChannelError)
            assert error.__cause__.__cause__ is failure
            assert not client.topic_subscriptions.copy()
            probe = JoinProbe(client)
            await join_channel(client, TOPIC, on_join_response=probe.install)
            assert installed == [{}]
            assert lost.empty()

    @pytest.mark.clock
    @pytest.mark.parametrize("socket_loss", [False, True])
    async def test_rejoin_loss_survives_leave_cleanup_failure(
        self, clock: Clock, protocol: ProtocolVersion, *, socket_loss: bool
    ) -> None:
        lost: asyncio.Queue[Exception] = asyncio.Queue()
        reconnected: asyncio.Queue[None] = asyncio.Queue()
        installed: list[object] = []
        failure = ValueError("snapshot installation failed")

        def install(response: object) -> None:
            if installed:
                raise failure
            installed.append(response)

        async def on_lost(topic: str, error: Exception) -> None:
            assert topic == TOPIC
            lost.put_nowait(error)

        backoff = TaskTimerObserver(
            SupervisorMixin._supervisor_loop.__code__
            if socket_loss
            else TopicRuntimeMixin._recover_channel.__code__
        )
        cleanup = TaskTimerObserver(TopicRuntimeMixin._leave_and_cleanup.__code__)
        async with (
            UnansweredLeavesPeer(protocol) as peer,
            make_client(
                peer,
                reconnect_policy=FAST_RECONNECT,
                leave_timeout_s=LEAVE_DEADLINE,
                on_topic_lost=on_lost,
                on_reconnect=lambda: reconnected.put(None),
            ) as client,
        ):
            channel = await join_channel(client, TOPIC, on_join_response=install)
            neighbor = JoinProbe(client, OTHER_TOPIC)
            await join_channel(client, OTHER_TOPIC, neighbor.receive)
            with backoff.installed(), cleanup.installed():
                if socket_loss:
                    await peer.close_all_clients()
                else:
                    await peer.crash_channel(TOPIC)
                await backoff.registered.get()
                await clock.advance(1)
                leave_task = await cleanup.registered.get()
                await peer.left.get()
                connection = client.connection
                await clock.advance(LEAVE_DEADLINE)
                with pytest.raises(ChannelError, match="Leave deadline expired"):
                    await leave_task
            if socket_loss:
                await reconnected.get()
            await asyncio.gather(*client.lifecycle_tasks)
            assert lost.qsize() == 1
            await peer.simulate_channel_push(OTHER_TOPIC, EVENT, {"healthy": True})
            assert (await neighbor.delivered.get()).payload == {"healthy": True}
            error = lost.get_nowait()
            assert isinstance(error, ChannelError)
            assert isinstance(error.__cause__, ChannelError)
            assert error.__cause__.__cause__ is failure
            assert client.connection is connection
            assert not channel.joined
            assert channel.subscription.cleanup_done
            assert client.topic_subscriptions[TOPIC] is channel.subscription
            assert not channel.subscription.terminal_received.is_set()

    async def test_join_ack_installs_outside_a_saturated_application_queue(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        capacity, burst = 2, 4
        barrier = asyncio.Event()
        gate = ProcessorGate(hold_start=True)
        async with (
            deadline(),
            make_client(phoenix_server, max_topic_queue_size=capacity) as client,
        ):

            async def routed(message: Message) -> None:
                del message
                barrier.set()

            await join_channel(client, OTHER_TOPIC, routed)
            _, independent = await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            probe = JoinProbe(client)
            with gate.installed():
                async with pending_subscribe(client, probe) as admission:
                    peer, request = await phoenix_server.deferred_joins.get()
                    await gate.started.wait()
                    await phoenix_server.release_join(peer, request)
                    assert await probe.installed.get() == {}
                    await admission
                    for number in range(burst):
                        await push(phoenix_server, request, payload={"n": number})
                    await push(phoenix_server, independent)
                    await barrier.wait()
                    owner = client.topic_subscriptions[TOPIC]
                    assert owner.dropped_message_count == burst - capacity
                    assert is_joined(client, TOPIC)
                    gate.start.set()
                    received = [await probe.delivered.get() for _ in range(capacity)]
            assert [message.payload for message in received] == [{"n": 2}, {"n": 3}]
            assert probe.order == [{}, {"n": 2}, {"n": 3}]

    async def test_rejoin_snapshot_installs_while_its_application_processor_is_held(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        barrier = asyncio.Event()
        gate = ProcessorGate(hold_start=True)

        async def routed(message: Message) -> None:
            del message
            barrier.set()

        async with (
            deadline(),
            make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client,
        ):
            probe = JoinProbe(client)
            channel = await join_channel(
                client, TOPIC, probe.receive, on_join_response=probe.install
            )
            await phoenix_server.joined.get()
            await probe.installed.get()
            await join_channel(client, OTHER_TOPIC, routed)
            _, independent = await phoenix_server.joined.get()
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"fresh": True},
            }
            with gate.installed():
                await phoenix_server.crash_channel(TOPIC)
                peer, request = await phoenix_server.deferred_joins.get()
                await gate.started.wait()
                await phoenix_server.release_join(peer, request)
                assert await probe.installed.get() == {"fresh": True}
                await push(phoenix_server, request, payload={"after": True})
                await push(phoenix_server, independent)
                await barrier.wait()
                assert channel.joined
                assert probe.order == [{}, {"fresh": True}]
                gate.start.set()
                await probe.delivered.get()
            assert probe.order == [{}, {"fresh": True}, {"after": True}]
