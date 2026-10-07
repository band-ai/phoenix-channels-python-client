from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from websockets import ClientConnection

from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)

from tests.harness import make_subscription
from tests.support import EVENT, TOPIC

V1 = PhoenixChannelsProtocolVersion.V1
V2 = PhoenixChannelsProtocolVersion.V2


@dataclass
class ScriptedConnection:
    """A websocket that yields ``incoming`` frames, then ends, and records sends."""

    incoming: list[str] = field(default_factory=list)
    sent: list[str] = field(default_factory=list)

    def __aiter__(self) -> ScriptedConnection:
        return self

    async def __anext__(self) -> str:
        if not self.incoming:
            raise StopAsyncIteration
        return self.incoming.pop(0)

    async def send(self, text: str) -> None:
        self.sent.append(text)


class QueueEmptiedWhileFull(asyncio.Queue[Any]):
    """Reports full, but is empty by the time the oldest message is dropped."""

    def full(self) -> bool:
        return True

    def get_nowait(self) -> Any:
        raise asyncio.QueueEmpty

    async def put(self, item: Any) -> None:
        cast(Any, self)._queue.append(item)


def v2_frame(join_ref: str, payload: dict[str, object] | None = None) -> str:
    return json.dumps([join_ref, "1", TOPIC, EVENT, payload or {}])


async def route(
    frames: list[str], queue: asyncio.Queue[Any], *, join_ref: str, generation: int
) -> None:
    """Route ``frames`` to a ``TOPIC`` subscription on connection ``generation``."""
    subscription = make_subscription(join_ref=join_ref, queue=queue, conn_generation=2)
    await PHXProtocolHandler(V2).process_websocket_messages(
        cast(ClientConnection, ScriptedConnection(frames)),
        {TOPIC: subscription},
        conn_generation=generation,
    )


def test_a_v2_frame_parses_into_a_message() -> None:
    message = PHXProtocolHandler(V2).parse_message(
        json.dumps(["jr", "r1", TOPIC, EVENT, {"k": "v"}])
    )

    assert (message.topic, message.event, message.join_ref) == (TOPIC, EVENT, "jr")


def test_a_v1_frame_parses_into_a_message() -> None:
    message = PHXProtocolHandler(V1).parse_message(
        json.dumps({"topic": TOPIC, "event": EVENT, "ref": "1", "payload": {"x": 1}})
    )

    assert (message.topic, message.event, message.ref) == (TOPIC, EVENT, "1")


@pytest.mark.parametrize(
    "protocol,raw,expected_exception",
    [
        (V2, json.dumps({"bad": "shape"}), TypeError),
        (V2, json.dumps([1, 2, 3]), ValueError),
        (V2, json.dumps([None, None, "", EVENT, {}]), TypeError),
        (V2, json.dumps([None, None, TOPIC, "", {}]), TypeError),
        (V1, json.dumps([1, 2, 3]), TypeError),
        (V1, json.dumps({"topic": "", "event": "x", "payload": {}}), TypeError),
        (V1, json.dumps({"topic": "t", "event": "", "payload": {}}), TypeError),
    ],
)
def test_a_malformed_frame_is_rejected(
    protocol: PhoenixChannelsProtocolVersion,
    raw: str,
    expected_exception: type[Exception],
) -> None:
    with pytest.raises(expected_exception):
        PHXProtocolHandler(protocol).parse_message(raw)


@pytest.mark.parametrize(
    "protocol,raw",
    [
        (V2, json.dumps([None, None, TOPIC, EVENT, None])),
        (V1, json.dumps({"topic": TOPIC, "event": EVENT, "payload": "not-a-dict"})),
    ],
)
def test_a_frame_without_an_object_payload_parses_with_an_empty_one(
    protocol: PhoenixChannelsProtocolVersion, raw: str
) -> None:
    assert PHXProtocolHandler(protocol).parse_message(raw).payload == {}


@pytest.mark.parametrize(
    "topic,subtopic", [("room:lobby", "lobby"), ("room:a:b", "a:b"), ("lobby", None)]
)
def test_a_message_subtopic_is_what_follows_the_first_colon(
    topic: str, subtopic: str | None
) -> None:
    message = PHXProtocolHandler(V2).parse_message(
        json.dumps([None, None, topic, EVENT, {}])
    )

    assert message.subtopic == subtopic


def test_an_unexpected_parse_failure_is_reported_as_a_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_: str | bytes) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "phoenix_channels_python_client.protocol_handler.json.loads", fail
    )

    with pytest.raises(ValueError, match="Invalid message format"):
        PHXProtocolHandler(V2).parse_message("ignored")


def test_a_message_serializes_back_to_its_v2_frame() -> None:
    handler = PHXProtocolHandler(V2)
    frame = ["jr", "r1", TOPIC, EVENT, {"x": 1}]

    serialized = handler.serialize_message(handler.parse_message(json.dumps(frame)))

    assert json.loads(serialized) == frame


async def test_sending_a_message_writes_one_frame() -> None:
    handler = PHXProtocolHandler(V2)
    connection = ScriptedConnection()
    message = handler.parse_message(v2_frame("jr"))

    await handler.send_message(cast(ClientConnection, connection), message)

    assert connection.sent == [handler.serialize_message(message)]


def test_an_unserializable_payload_raises_a_type_error() -> None:
    handler = PHXProtocolHandler(V2)
    message = handler.parse_message(v2_frame("jr"))
    message.payload["bad"] = {1}

    with pytest.raises(TypeError):
        handler.serialize_message(message)


async def test_routing_drops_messages_from_an_older_connection() -> None:
    queue: asyncio.Queue[Any] = asyncio.Queue()

    await route([v2_frame("jr")], queue, join_ref="jr", generation=1)

    assert queue.empty()


async def test_routing_into_a_full_queue_that_empties_meanwhile_drops_nothing() -> None:
    queue = QueueEmptiedWhileFull()
    subscription = make_subscription(join_ref="jr", queue=queue)

    await PHXProtocolHandler(V2).process_websocket_messages(
        cast(ClientConnection, ScriptedConnection([v2_frame("jr")])),
        {TOPIC: subscription},
        conn_generation=subscription.conn_generation,
    )

    assert subscription.dropped_message_count == 0
