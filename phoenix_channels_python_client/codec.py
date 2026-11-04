from __future__ import annotations

import json
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, NamedTuple

from phoenix_channels_python_client.messages import (
    HEARTBEAT_EVENT,
    PHOENIX_TOPIC,
    Message,
    SystemEvent,
)
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.utils import make_message

MAX_HEADER_FIELD_BYTES = 255


class BinaryKind(IntEnum):
    PUSH = 0
    REPLY = 1
    BROADCAST = 2


SERVER_HEADER_FIELDS = {
    BinaryKind.PUSH: 3,
    BinaryKind.REPLY: 4,
    BinaryKind.BROADCAST: 2,
}


@dataclass(frozen=True)
class DecodedMessage:
    message: Message
    binary_reply_status: str | None = None


class RawFrame(NamedTuple):
    # Ref coercion belongs to the message model.
    join_ref: Any
    ref: Any
    topic: object
    event: object
    payload: object


def _decode_v1(parsed: object) -> RawFrame:
    match parsed:
        case dict():
            return RawFrame(
                parsed.get("join_ref"),
                parsed.get("ref"),
                parsed.get("topic"),
                parsed.get("event"),
                parsed.get("payload"),
            )
        case _:
            raise TypeError(
                f"Protocol v1 expects object format, got {type(parsed).__name__}"
            )


def _decode_v2(parsed: object) -> RawFrame:
    match parsed:
        case [join_ref, ref, topic, event, payload]:
            return RawFrame(join_ref, ref, topic, event, payload)
        case list():
            raise ValueError(
                "Protocol v2 expects 5-element array "
                "[join_ref, ref, topic, event, payload]"
            )
        case _:
            raise TypeError(
                f"Protocol v2 expects array format, got {type(parsed).__name__}"
            )


def _require_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Message {field} must be a non-empty string")
    return value


def _decode_text(raw: str, protocol_version: ProtocolVersion) -> DecodedMessage:
    parsed = json.loads(raw)
    frame = (
        _decode_v2(parsed)
        if protocol_version == ProtocolVersion.V2
        else _decode_v1(parsed)
    )
    return DecodedMessage(
        make_message(
            topic=_require_text(frame.topic, "topic"),
            event=_require_text(frame.event, "event"),
            ref=frame.ref,
            join_ref=frame.join_ref,
            payload=frame.payload if isinstance(frame.payload, dict) else {},
        )
    )


def _binary_fields(raw: bytes, count: int) -> tuple[list[str], bytes]:
    header_end = 1 + count
    if len(raw) < header_end:
        raise ValueError("Truncated binary frame header")
    sizes = raw[1:header_end]
    if len(raw) < header_end + sum(sizes):
        raise ValueError("Truncated binary frame fields")
    fields: list[str] = []
    offset = header_end
    for size in sizes:
        fields.append(raw[offset : offset + size].decode("utf-8"))
        offset += size
    return fields, raw[offset:]


def _decode_binary(raw: bytes) -> DecodedMessage:
    if not raw:
        raise ValueError("Empty binary frame")
    kind = BinaryKind(raw[0])
    fields, payload = _binary_fields(raw, SERVER_HEADER_FIELDS[kind])
    join_ref: str | None
    ref: str | None
    status: str | None = None
    match kind:
        case BinaryKind.PUSH:
            join_ref, topic, event = fields
            ref = None
        case BinaryKind.REPLY:
            join_ref, ref, topic, status = fields
            event = SystemEvent.reply
        case BinaryKind.BROADCAST:
            topic, event = fields
            join_ref = ref = None
        case _:
            raise ValueError("Unsupported binary frame kind")
    return DecodedMessage(
        make_message(
            topic=_require_text(topic, "topic"),
            event=_require_text(event, "event"),
            ref=ref or None,
            join_ref=join_ref or None,
            payload=payload,
        ),
        binary_reply_status=status,
    )


def decode_message(
    raw: str | bytes, protocol_version: ProtocolVersion
) -> DecodedMessage:
    if isinstance(raw, bytes):
        if protocol_version != ProtocolVersion.V2:
            raise TypeError("Binary frames require protocol V2")
        return _decode_binary(raw)
    if not isinstance(raw, str):
        raise TypeError("Frames must be str or bytes")
    return _decode_text(raw, protocol_version)


def _encode_binary(message: Message, payload: bytes) -> bytes:
    if message.event in {SystemEvent.join, SystemEvent.leave} or (
        message.topic == PHOENIX_TOPIC and message.event == HEARTBEAT_EVENT
    ):
        raise TypeError("Channel control frames require a JSON object payload")
    fields = tuple(
        field.encode("utf-8")
        for field in (
            message.join_ref or "",
            message.ref or "",
            message.topic,
            str(message.event),
        )
    )
    if any(len(field) > MAX_HEADER_FIELD_BYTES for field in fields):
        raise ValueError("Binary frame header fields must fit in 255 UTF-8 bytes")
    return (
        bytes((BinaryKind.PUSH, *(len(field) for field in fields)))
        + b"".join(fields)
        + payload
    )


def encode_message(message: Message, protocol_version: ProtocolVersion) -> str | bytes:
    payload = message.payload
    if isinstance(payload, bytes):
        if protocol_version != ProtocolVersion.V2:
            raise TypeError("Binary frames require protocol V2")
        return _encode_binary(message, payload)
    frame = (
        [
            message.join_ref,
            message.ref,
            message.topic,
            str(message.event),
            message.payload,
        ]
        if protocol_version == ProtocolVersion.V2
        else {
            "topic": message.topic,
            "event": str(message.event),
            "ref": message.ref,
            "payload": message.payload,
        }
    )
    try:
        return json.dumps(frame)
    except (TypeError, ValueError, RecursionError) as exc:
        raise TypeError("Cannot serialize message as JSON") from exc
