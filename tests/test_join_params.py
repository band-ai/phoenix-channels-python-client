"""Join params validation, ownership, and channel-local failure behavior."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, Never, cast

import pytest

from phoenix_channels_python_client import ChannelError, Client, Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.topic_subscription import JoinParams
from tests.fake_server import FakePhoenixServer
from tests.join_support import deadline
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    OTHER_TOPIC,
    TOPIC,
    each_protocol,
    make_client,
)


class UncopyableParams(dict[str, Any]):
    """Valid dict whose snapshot operation fails through its mapping hooks."""

    def __init__(self, failure: RuntimeError) -> None:
        super().__init__({"token": "initial"})
        self.failure = failure

    def __iter__(self) -> Iterator[str]:
        # Dict copying consults mapping hooks when iteration is overridden.
        return super().__iter__()

    def keys(self) -> Never:
        raise self.failure


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
        assert isinstance(reconnect.payload, dict)
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
@pytest.mark.parametrize("use_provider", [False, True], ids=["direct", "provider"])
async def test_params_snapshot_failure_retires_the_handle_and_preserves_its_cause(
    client: Client,
    phoenix_server: FakePhoenixServer,
    *,
    use_provider: bool,
) -> None:
    failure = RuntimeError("cannot snapshot params")
    values = UncopyableParams(failure)
    params = (lambda: values) if use_provider else values

    async with deadline():
        channel = client.channel(TOPIC, params=params)
        with pytest.raises(ChannelError) as caught:
            await channel.join()
        assert caught.value.__cause__ is failure
        assert phoenix_server.join_topics == []
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


@each_protocol
@pytest.mark.parametrize(
    "params",
    [[], lambda: None, list, {"token": object()}, lambda: {"token": object()}],
    ids=[
        "invalid-direct",
        "provider-none",
        "provider-list",
        "encoding-direct",
        "encoding-provider",
    ],
)
async def test_invalid_params_retire_the_handle_without_sending(
    client: Client,
    phoenix_server: FakePhoenixServer,
    params: object,
) -> None:
    async with deadline():
        channel = client.channel(TOPIC, params=cast("JoinParams", params))
        with pytest.raises(ChannelError) as caught:
            await channel.join()
        assert isinstance(caught.value.__cause__, TypeError)
        assert phoenix_server.join_topics == []
        assert not channel.joined
        with pytest.raises(ChannelError):
            await channel.join()
        async with client.channel(TOPIC) as replacement:
            assert replacement.joined


@each_protocol
async def test_async_params_provider_is_rejected_without_running(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    ran = asyncio.Event()

    async def params() -> dict[str, Any]:
        ran.set()
        return {}

    async with deadline():
        channel = client.channel(TOPIC, params=cast("JoinParams", params))
        with pytest.raises(ChannelError) as caught:
            await channel.join()
        assert isinstance(caught.value.__cause__, TypeError)
        assert not ran.is_set()
        assert phoenix_server.join_topics == []
        with pytest.raises(ChannelError):
            await channel.join()


@each_protocol
async def test_returned_coroutine_is_closed_without_running(
    client: Client,
    phoenix_server: FakePhoenixServer,
) -> None:
    ran = asyncio.Event()

    async def asynchronous() -> dict[str, Any]:
        ran.set()
        return {}

    result = asynchronous()
    try:
        async with deadline():
            channel = client.channel(TOPIC, params=cast("JoinParams", lambda: result))
            with pytest.raises(ChannelError) as caught:
                await channel.join()
            assert isinstance(caught.value.__cause__, TypeError)
            assert inspect.getcoroutinestate(result) == inspect.CORO_CLOSED
            assert not ran.is_set()
            assert phoenix_server.join_topics == []
            with pytest.raises(ChannelError):
                await channel.join()
    finally:
        result.close()


@each_protocol
async def test_returned_future_remains_owned_by_its_creator(
    client: Client, phoenix_server: FakePhoenixServer
) -> None:
    future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
    async with deadline():
        channel = client.channel(TOPIC, params=cast("JoinParams", lambda: future))
        with pytest.raises(ChannelError):
            await channel.join()
        assert phoenix_server.join_topics == []
        with pytest.raises(ChannelError):
            await channel.join()
        assert not future.done()
        result = {"token": "owned"}
        future.set_result(result)
        assert await future is result


@each_protocol
async def test_returned_task_remains_owned_until_its_creator_releases_it(
    client: Client, phoenix_server: FakePhoenixServer
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    result = {"token": "owned"}

    async def external_work() -> dict[str, Any]:
        started.set()
        await release.wait()
        return result

    task = asyncio.create_task(external_work())
    try:
        async with deadline():
            await started.wait()
            channel = client.channel(TOPIC, params=cast("JoinParams", lambda: task))
            with pytest.raises(ChannelError):
                await channel.join()
            assert phoenix_server.join_topics == []
            with pytest.raises(ChannelError):
                await channel.join()
            assert not task.done()
            release.set()
            assert await task is result
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _crash_channel(server: FakePhoenixServer) -> None:
    await server.crash_channel(TOPIC)


@each_protocol
@pytest.mark.parametrize(
    ("invalid", "cause_type"),
    [
        ([], TypeError),
        ({"token": object()}, TypeError),
        (UncopyableParams(RuntimeError("cannot snapshot params")), RuntimeError),
    ],
    ids=["non-dict", "unencodable-dict", "uncopyable-dict"],
)
@pytest.mark.parametrize(
    ("recover", "new_connections"),
    [(_crash_channel, 0), (FakePhoenixServer.close_all_clients, 1)],
    ids=["channel-recovery", "socket-recovery"],
)
async def test_invalid_recovery_params_retire_only_the_affected_channel(
    phoenix_server: FakePhoenixServer,
    invalid: object,
    cause_type: type[Exception],
    recover: Callable[[FakePhoenixServer], Awaitable[None]],
    new_connections: int,
) -> None:
    attempts = 0
    lost: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()
    installed: asyncio.Queue[None] = asyncio.Queue()
    received: asyncio.Queue[Message] = asyncio.Queue()

    def params() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        return (
            {"token": "initial"} if attempts == 1 else cast("dict[str, Any]", invalid)
        )

    async def on_lost(topic: str, error: Exception) -> None:
        lost.put_nowait((topic, error))

    async with (
        deadline(),
        make_client(
            phoenix_server, reconnect_policy=FAST_RECONNECT, on_topic_lost=on_lost
        ) as client,
    ):
        affected = client.channel(TOPIC, params=params)
        neighbor = client.channel(
            OTHER_TOPIC, on_join_response=lambda _: installed.put_nowait(None)
        )
        neighbor.on_any(received.put)
        await affected.join()
        await neighbor.join()
        await installed.get()
        connections = phoenix_server.next_client_id
        await recover(phoenix_server)
        topic, error = await lost.get()
        assert topic == TOPIC
        assert isinstance(error, ChannelError)
        assert isinstance(error.__cause__, ChannelError)
        assert isinstance(error.__cause__.__cause__, cause_type)
        for _ in range(new_connections):
            await installed.get()
        assert not affected.joined
        with pytest.raises(ChannelError):
            await affected.join()
        assert neighbor.joined
        await phoenix_server.simulate_server_event(OTHER_TOPIC, EVENT, {"ok": True})
        assert (await received.get()).payload == {"ok": True}
        assert attempts == phoenix_server.join_topics.count(TOPIC) + 1
        assert phoenix_server.join_topics.count(TOPIC) == 1
        assert phoenix_server.next_client_id == connections + new_connections
        assert lost.empty()
        async with client.channel(TOPIC) as replacement:
            assert replacement.joined


BINARY_PARAMS = b"\x0a\x05hello"


@pytest.mark.parametrize("use_provider", [False, True], ids=["direct", "provider"])
async def test_bytes_join_params_are_sent_as_a_binary_push(
    client: Client, phoenix_server: FakePhoenixServer, *, use_provider: bool
) -> None:
    params = (lambda: BINARY_PARAMS) if use_provider else BINARY_PARAMS
    async with client.channel(TOPIC, params=params) as room:
        _, request = await phoenix_server.joined.get()
        assert request.event == "phx_join"
        assert request.payload == BINARY_PARAMS
        assert room.joined


async def test_bytes_join_params_are_resent_on_recovery(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with (
        deadline(),
        make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client,
    ):
        room = client.channel(TOPIC, params=BINARY_PARAMS)
        await room.join()
        await phoenix_server.joined.get()
        await phoenix_server.crash_channel(TOPIC)
        _, channel_rejoin = await phoenix_server.joined.get()
        assert channel_rejoin.payload == BINARY_PARAMS
        await phoenix_server.close_all_clients()
        _, socket_rejoin = await phoenix_server.joined.get()
        assert socket_rejoin.payload == BINARY_PARAMS


@pytest.mark.parametrize("params", [bytearray(b"x"), "text", lambda: "text"])
async def test_non_dict_non_bytes_params_are_rejected(
    client: Client, phoenix_server: FakePhoenixServer, params: object
) -> None:
    channel = client.channel(TOPIC, params=cast("JoinParams", params))
    with pytest.raises(ChannelError, match="dict or bytes"):
        await channel.join()
    assert phoenix_server.join_topics == []


async def test_bytes_join_params_require_protocol_v2() -> None:
    async with (
        FakePhoenixServer(ProtocolVersion.V1) as server,
        deadline(),
        make_client(server) as client,
    ):
        channel = client.channel(TOPIC, params=BINARY_PARAMS)
        with pytest.raises(ChannelError, match="Join params encoding failed"):
            await channel.join()
        assert server.join_topics == []
