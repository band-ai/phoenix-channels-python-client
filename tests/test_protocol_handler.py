from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, NoReturn, cast

import pytest
from pydantic import ValidationError
from websockets import ClientConnection

from phoenix_channels_python_client.phx_messages import (
    ChannelMessage,
    Event,
    OutgoingPayload,
    Payload,
)
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.utils import make_message
from tests.harness import make_subscription
from tests.support import EVENT, MAX_BINARY_FIELD_SIZE, TOPIC, json_object

V1 = PhoenixChannelsProtocolVersion.V1
V2 = PhoenixChannelsProtocolVersion.V2

# v1 servers may send refs as JSON numbers.
INTEGER_REF = 1


@dataclass
class ScriptedConnection:
    """A websocket that yields ``incoming`` frames, then ends, and records sends."""

    incoming: list[str] = field(default_factory=list)
    sent: list[str | bytes] = field(default_factory=list)

    def __aiter__(self) -> ScriptedConnection:
        return self

    async def __anext__(self) -> str:
        if not self.incoming:
            raise StopAsyncIteration
        return self.incoming.pop(0)

    async def send(self, frame: str | bytes) -> None:
        self.sent.append(frame)


class QueueEmptiedWhileFull(asyncio.Queue[ChannelMessage]):
    """Reports full, but is empty by the time the oldest message is dropped."""

    def full(self) -> bool:
        return True

    def get_nowait(self) -> NoReturn:
        raise asyncio.QueueEmpty

    async def put(self, item: ChannelMessage) -> None:
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


BINARY_DATA = b"\x01\x02"

BINARY_PUSH = b"\x00\x01\x03\x041topevnt" + BINARY_DATA
BINARY_REPLY = b"\x01\x01\x01\x03\x0212topok" + BINARY_DATA
BINARY_BROADCAST = b"\x02\x03\x04topevnt" + BINARY_DATA
BINARY_PUSH_WITHOUT_JOIN_REF = b"\x00\x00\x03\x04topevnt" + BINARY_DATA
BINARY_REPLY_WITHOUT_REFS = b"\x01\x00\x00\x03\x02topok" + BINARY_DATA
BINARY_PUSH_WITHOUT_DATA = b"\x00\x01\x03\x041topevnt"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (BINARY_PUSH, ("1", None, "top", "evnt", BINARY_DATA)),
        (BINARY_PUSH_WITHOUT_JOIN_REF, (None, None, "top", "evnt", BINARY_DATA)),
        (BINARY_PUSH_WITHOUT_DATA, ("1", None, "top", "evnt", b"")),
        (BINARY_BROADCAST, (None, None, "top", "evnt", BINARY_DATA)),
        (
            BINARY_REPLY,
            ("1", "2", "top", "phx_reply", {"status": "ok", "response": BINARY_DATA}),
        ),
        (
            BINARY_REPLY_WITHOUT_REFS,
            (None, None, "top", "phx_reply", {"status": "ok", "response": BINARY_DATA}),
        ),
    ],
)
def test_a_v2_binary_frame_parses_into_a_message(
    raw: bytes, expected: tuple[object, ...]
) -> None:
    message = PHXProtocolHandler(V2).parse_message(raw)

    assert (
        message.join_ref,
        message.ref,
        message.topic,
        str(message.event),
        message.payload,
    ) == expected


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"\x03",
        b"\x00\x01\x03",
        b"\x00\x01\x03\x041top",
        b"\x00\x01\x03\x041topev",
        b"\x00\x01\x03\x041topev\xff\xfe",
    ],
    ids=[
        "empty",
        "unknown-kind",
        "truncated-sizes",
        "missing-field",
        "short-field",
        "non-utf8-field",
    ],
)
def test_a_malformed_v2_binary_frame_is_rejected(raw: bytes) -> None:
    with pytest.raises(ValueError, match="binary frame"):
        PHXProtocolHandler(V2).parse_message(raw)


# A client push carries the ref too: join_ref, ref, topic, event.
CLIENT_BINARY_PUSH = b"\x00\x01\x01\x03\x0412topevnt" + BINARY_DATA
CLIENT_BINARY_PUSH_WITHOUT_REFS = b"\x00\x00\x00\x03\x04topevnt" + BINARY_DATA


def binary_push(
    *,
    join_ref: str | None = "1",
    ref: str | None = "2",
    topic: str = "top",
    event: str = "evnt",
) -> ChannelMessage:
    return make_message(
        event=Event(event),
        topic=topic,
        ref=ref,
        join_ref=join_ref,
        payload=BINARY_DATA,
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (binary_push(), CLIENT_BINARY_PUSH),
        (binary_push(join_ref=None, ref=None), CLIENT_BINARY_PUSH_WITHOUT_REFS),
    ],
)
def test_a_bytes_payload_serializes_to_a_v2_binary_push(
    message: ChannelMessage, expected: bytes
) -> None:
    assert PHXProtocolHandler(V2).serialize_message(message) == expected


def test_a_binary_header_field_of_the_maximum_size_serializes() -> None:
    topic = "t" * MAX_BINARY_FIELD_SIZE

    serialized = PHXProtocolHandler(V2).serialize_message(binary_push(topic=topic))

    assert serialized == (
        b"\x00\x01\x01\xff\x0412" + topic.encode() + b"evnt" + BINARY_DATA
    )


@pytest.mark.parametrize(
    ("message", "field_name"),
    [
        (binary_push(join_ref="j" * (MAX_BINARY_FIELD_SIZE + 1)), "join_ref"),
        (binary_push(ref="r" * (MAX_BINARY_FIELD_SIZE + 1)), "ref"),
        # A field's size counts UTF-8 bytes, not characters.
        (binary_push(topic="é" * (MAX_BINARY_FIELD_SIZE // 2 + 1)), "topic"),
    ],
)
def test_an_oversized_binary_header_field_is_rejected(
    message: ChannelMessage, field_name: str
) -> None:
    with pytest.raises(ValueError, match=f"binary frame {field_name} must be"):
        PHXProtocolHandler(V2).serialize_message(message)


@pytest.mark.parametrize(
    ("message", "field_name", "problem"),
    [
        (binary_push(topic=""), "topic", "must not be empty"),
        (binary_push(event=""), "event", "must not be empty"),
        # A lone surrogate has no UTF-8 encoding.
        (binary_push(topic="\ud800"), "topic", "is not valid UTF-8 text"),
        (binary_push(ref="\ud800"), "ref", "is not valid UTF-8 text"),
    ],
)
def test_an_unencodable_binary_header_field_is_rejected(
    message: ChannelMessage, field_name: str, problem: str
) -> None:
    with pytest.raises(ValueError, match=f"binary frame {field_name} {problem}"):
        PHXProtocolHandler(V2).serialize_message(message)


def test_v1_rejects_a_bytes_payload() -> None:
    with pytest.raises(TypeError, match="cannot send a binary payload"):
        PHXProtocolHandler(V1).serialize_message(binary_push())


async def test_sending_a_bytes_payload_writes_one_binary_frame() -> None:
    connection = ScriptedConnection()

    await PHXProtocolHandler(V2).send_message(
        cast(ClientConnection, connection), binary_push()
    )

    assert connection.sent == [CLIENT_BINARY_PUSH]


def test_a_v1_binary_frame_is_parsed_as_json() -> None:
    frame = {"topic": TOPIC, "event": EVENT, "ref": "1", "payload": {}}

    message = PHXProtocolHandler(V1).parse_message(json.dumps(frame).encode())

    assert (message.topic, message.event) == (TOPIC, EVENT)


def test_a_v1_frame_parses_into_a_message() -> None:
    message = PHXProtocolHandler(V1).parse_message(
        json.dumps({"topic": TOPIC, "event": EVENT, "ref": "1", "payload": {"x": 1}})
    )

    assert (message.topic, message.event, message.ref) == (TOPIC, EVENT, "1")


def test_an_unknown_version_keeps_the_v1_frame_format() -> None:
    handler = PHXProtocolHandler("unsupported")
    frame = {"topic": TOPIC, "event": EVENT, "ref": "1", "payload": {"ok": True}}

    message = handler.parse_message(json.dumps(frame))

    assert (message.topic, message.event, message.payload) == (
        TOPIC,
        EVENT,
        {"ok": True},
    )
    assert json.loads(handler.serialize_message(message)) == frame


def test_a_v1_frame_with_integer_refs_parses_as_strings() -> None:
    message = PHXProtocolHandler(V1).parse_message(
        json.dumps(
            {
                "topic": TOPIC,
                "event": EVENT,
                "ref": INTEGER_REF,
                "join_ref": INTEGER_REF,
                "payload": {},
            }
        )
    )

    assert (message.ref, message.join_ref) == (str(INTEGER_REF), str(INTEGER_REF))


@pytest.mark.parametrize(
    ("protocol", "raw", "expected_exception"),
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
    ("protocol", "raw"),
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
    ("topic", "subtopic"),
    [("room:lobby", "lobby"), ("room:a:b", "a:b"), ("lobby", None)],
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
    def fail(_: str | bytes) -> NoReturn:
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
    json_object(message.payload)["bad"] = {1}

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


@pytest.mark.parametrize("payload", [bytearray(BINARY_DATA), memoryview(BINARY_DATA)])
def test_a_buffer_payload_is_coerced_to_bytes(payload: OutgoingPayload) -> None:
    message = make_message(event=Event(EVENT), topic=TOPIC, payload=payload)

    assert message.payload == BINARY_DATA
    assert type(message.payload) is bytes


@pytest.mark.parametrize(
    ("version", "payload"),
    [(V1, {}), (V2, {}), (V2, BINARY_DATA)],
    ids=["v1-json", "v2-json", "v2-bytes"],
)
def test_a_payload_the_version_supports_is_sendable(
    version: PhoenixChannelsProtocolVersion, payload: Payload
) -> None:
    PHXProtocolHandler(version).ensure_sendable(payload)


def test_a_bytes_payload_is_not_sendable_on_v1() -> None:
    with pytest.raises(TypeError, match="cannot send a binary payload"):
        PHXProtocolHandler(V1).ensure_sendable(BINARY_DATA)


def test_a_text_payload_is_not_coerced_to_bytes() -> None:
    with pytest.raises(ValidationError):
        make_message(event=Event(EVENT), topic=TOPIC, payload=cast(Any, "text"))
