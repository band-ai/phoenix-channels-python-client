from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, cast

import pytest
from websockets import ClientConnection

from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectPolicy,
    reconnect_policy_is_invalid,
)
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.topic_subscription import TopicSubscription


@dataclass
class _FakeConnection:
    messages: list[str]

    def __post_init__(self) -> None:
        self._index = 0
        self.sent: list[str] = []

    def __aiter__(self) -> _FakeConnection:
        return self

    async def __anext__(self) -> str:
        if self._index >= len(self.messages):
            raise StopAsyncIteration
        value = self.messages[self._index]
        self._index += 1
        return value

    async def send(self, text: str) -> None:
        self.sent.append(text)


class _QueueRaisesOnDrop(asyncio.Queue[Any]):
    def full(self) -> bool:
        return True

    def get_nowait(self) -> Any:
        raise asyncio.QueueEmpty

    async def put(self, item: Any) -> None:
        self._queue.append(item)


def test_transition_client_state_allows_noop_and_rejects_invalid() -> None:
    assert transition_client_state(ClientState.CLOSED, ClientState.CLOSED) == ClientState.CLOSED
    with pytest.raises(RuntimeError):
        transition_client_state(ClientState.CLOSED, ClientState.CONNECTED)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay_s": -1},
        {"factor": 0},
        {"max_delay_s": -1},
        {"stable_reset_s": 0},
        {"service_restart_min_delay_s": -1},
        {"service_restart_max_delay_s": 0, "service_restart_min_delay_s": 1},
        {"try_again_later_min_delay_s": -1},
        {"try_again_later_max_delay_s": 0, "try_again_later_min_delay_s": 1},
        {"rapid_disconnect_uptime_s": -1},
        {"rapid_window_s": 0},
        {"rapid_first_min_delay_s": -1},
        {"rapid_second_min_delay_s": -1},
        {"rapid_cooldown_base_s": -1},
        {"rapid_cooldown_step_s": -1},
        {"rapid_cooldown_max_s": 0, "rapid_cooldown_base_s": 1},
        {"rapid_suppress_disconnect_count": -1},
        {"rapid_hold_down_jitter_low_ratio": -1},
        {"rapid_hold_down_jitter_high_ratio": 0.1, "rapid_hold_down_jitter_low_ratio": 0.2},
    ],
)
def test_reconnect_policy_is_invalid_covers_all_invalid_guards(kwargs: dict[str, Any]) -> None:
    policy = ReconnectPolicy(**kwargs)
    assert reconnect_policy_is_invalid(policy) is True


def test_reconnect_policy_is_invalid_accepts_defaults() -> None:
    assert reconnect_policy_is_invalid(ReconnectPolicy()) is False


def test_protocol_handler_parse_v2_and_v1_success() -> None:
    v2 = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    msg2 = v2.parse_message(json.dumps(["jr", "r1", "test-topic", "hello", {"k": "v"}]))
    assert msg2.topic == "test-topic"
    assert str(msg2.event) == "hello"
    assert msg2.join_ref == "jr"

    v1 = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1)
    msg1 = v1.parse_message(
        json.dumps({"topic": "test-topic", "event": "hello", "ref": "1", "payload": {"x": 1}})
    )
    assert msg1.topic == "test-topic"
    assert str(msg1.event) == "hello"
    assert msg1.ref == "1"


@pytest.mark.parametrize(
    "handler,raw,expected_exception",
    [
        (PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2), json.dumps({"bad": "shape"}), TypeError),
        (PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2), json.dumps([1, 2, 3]), ValueError),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2),
            json.dumps([None, None, "", "evt", {}]),
            TypeError,
        ),
        (PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1), json.dumps([1, 2, 3]), TypeError),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1),
            json.dumps({"topic": "", "event": "x", "payload": {}}),
            TypeError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1),
            json.dumps({"topic": "t", "event": "", "payload": {}}),
            TypeError,
        ),
    ],
)
def test_protocol_handler_parse_invalid_shapes_raise(
    handler: PHXProtocolHandler, raw: str, expected_exception: type[Exception]
) -> None:
    with pytest.raises(expected_exception):
        handler.parse_message(raw)


def test_protocol_handler_parse_unexpected_exception_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)

    def _boom(_: str | bytes) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr("phoenix_channels_python_client.protocol_handler.json.loads", _boom)
    with pytest.raises(ValueError):
        handler.parse_message("ignored")


def test_protocol_handler_serialize_and_send_message() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    message = handler.parse_message(json.dumps(["jr", "r1", "test-topic", "evt", {"x": 1}]))
    serialized = handler.serialize_message(message)
    assert serialized.startswith("[")

    connection = _FakeConnection(messages=[])
    asyncio.run(handler.send_message(cast(ClientConnection, connection), message))
    assert len(connection.sent) == 1


def test_protocol_handler_serialize_raises_type_error_on_bad_payload() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    message = handler.parse_message(json.dumps(["jr", "r1", "test-topic", "evt", {}]))
    message.payload["bad"] = set([1])  # type: ignore[index]
    with pytest.raises(TypeError):
        handler.serialize_message(message)


@pytest.mark.asyncio
async def test_process_websocket_messages_filters_join_ref() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10)
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="new",
        process_topic_messages_task=None,
        conn_generation=2,
    )

    stale_join_ref = json.dumps(["old", "1", "test-topic", "evt", {"a": 2}])
    good = json.dumps(["new", "1", "test-topic", "evt", {"a": 3}])
    connection = _FakeConnection(messages=[stale_join_ref, good])

    await handler.process_websocket_messages(
        cast(ClientConnection, connection), {"test-topic": sub}, conn_generation=2
    )
    assert queue.qsize() == 1
    only = queue.get_nowait()
    assert only.payload == {"a": 3}


@pytest.mark.asyncio
async def test_process_websocket_messages_filters_stale_generation() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10)
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="jr",
        process_topic_messages_task=None,
        conn_generation=2,
    )
    msg = json.dumps(["jr", "1", "test-topic", "evt", {"x": 1}])
    connection = _FakeConnection(messages=[msg])
    await handler.process_websocket_messages(cast(ClientConnection, connection), {"test-topic": sub}, 1)
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_process_websocket_messages_drop_path_handles_queueempty() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue = _QueueRaisesOnDrop()
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="jr",
        process_topic_messages_task=None,
        conn_generation=1,
    )
    sub.dropped_message_count = 99

    msg = json.dumps(["jr", "1", "test-topic", "evt", {"x": 1}])
    connection = _FakeConnection(messages=[msg])
    await handler.process_websocket_messages(cast(ClientConnection, connection), {"test-topic": sub}, 1)
    assert sub.dropped_message_count == 99
