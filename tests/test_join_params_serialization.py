from __future__ import annotations

import asyncio
from typing import Any

import pytest

from phoenix_channels_python_client import ChannelError, Message
from phoenix_channels_python_client.codec import encode_message
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.utils import make_message
from tests.fake_server import FakePhoenixServer
from tests.join_support import deadline
from tests.scheduling import TaskLifecycleGate
from tests.support import (
    EVENT,
    FAST_RECONNECT,
    OTHER_TOPIC,
    TOPIC,
    each_protocol,
    make_client,
)

JSON_RECURSION_DEPTH = 20_000


@pytest.fixture
def deep_params() -> dict[str, Any]:
    params: dict[str, Any] = {}
    nested = params
    # C JSON encoder limits differ from Python's interpreter recursion limit.
    for _ in range(JSON_RECURSION_DEPTH):
        child: dict[str, Any] = {}
        nested["child"] = child
        nested = child
    return params


@pytest.mark.parametrize("protocol", list(ProtocolVersion))
def test_deep_json_payload_preserves_the_serialization_failure(
    deep_params: dict[str, Any], protocol: ProtocolVersion
) -> None:
    message = make_message(event=EVENT, topic=TOPIC, payload=deep_params)
    with pytest.raises(TypeError, match="Cannot serialize message as JSON") as caught:
        encode_message(message, protocol)
    assert isinstance(caught.value.__cause__, RecursionError)


@each_protocol
async def test_deep_initial_params_raise_channel_error_and_retire_the_handle(
    phoenix_server: FakePhoenixServer, deep_params: dict[str, Any]
) -> None:
    async with deadline(), make_client(phoenix_server) as client:
        channel = client.channel(TOPIC, params=deep_params)
        with pytest.raises(ChannelError, match="Join params encoding failed") as caught:
            await channel.join()
        encoding_error = caught.value.__cause__
        assert isinstance(encoding_error, TypeError)
        assert isinstance(encoding_error.__cause__, RecursionError)
        assert not channel.joined
        assert phoenix_server.join_topics == []
        with pytest.raises(ChannelError, match="already joined or left"):
            await channel.join()
        async with client.channel(TOPIC) as replacement:
            assert replacement.joined


@each_protocol
async def test_deep_recovery_params_lose_only_the_affected_channel(
    phoenix_server: FakePhoenixServer, deep_params: dict[str, Any]
) -> None:
    attempts = 0
    lost: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

    def params() -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        return {"token": "initial"} if attempts == 1 else deep_params

    async def on_lost(topic: str, error: Exception) -> None:
        lost.put_nowait((topic, error))

    gate = TaskLifecycleGate(TopicRuntimeMixin._recover_channel.__code__)
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
        connection = client.connection
        connection_attempts = dict(phoenix_server.connection_attempts_by_path)
        with gate.installed():
            await phoenix_server.crash_channel(TOPIC)
            await gate.exiting.wait()
            assert gate.task is not None
            await gate.task
        topic, error = await lost.get()
        assert topic == TOPIC
        assert isinstance(error, ChannelError)
        join_error = error.__cause__
        assert isinstance(join_error, ChannelError)
        encoding_error = join_error.__cause__
        assert isinstance(encoding_error, TypeError)
        assert isinstance(encoding_error.__cause__, RecursionError)
        assert not affected.joined
        with pytest.raises(ChannelError, match="already joined or left"):
            await affected.join()
        received: asyncio.Queue[Message] = asyncio.Queue()
        neighbor.on_any(received.put)
        await phoenix_server.simulate_server_event(OTHER_TOPIC, EVENT, {"ok": True})
        assert (await received.get()).payload == {"ok": True}
        assert neighbor.joined
        assert lost.empty()
        assert attempts == phoenix_server.join_topics.count(TOPIC) + 1
        assert phoenix_server.join_topics == [TOPIC, OTHER_TOPIC]
        assert client.connection is connection
        assert phoenix_server.connection_attempts_by_path == connection_attempts
