from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from enum import IntEnum, StrEnum
from typing import Any, NamedTuple

from websockets import ClientConnection

from phoenix_channels_python_client.diagnostics import ConnectionLogger
from phoenix_channels_python_client.phx_messages import (
    PHOENIX_TOPIC,
    ChannelMessage,
    Event,
    Payload,
    PHXEvent,
)
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import make_message

logger = logging.getLogger(__name__)


class PhoenixChannelsProtocolVersion(StrEnum):
    V1 = "1.0"
    V2 = "2.0"

    @property
    def vsn(self) -> str:
        """The ``vsn`` query parameter that selects this version on the server."""
        return _PROTOCOL_SPECS[self].vsn


DEFAULT_PROTOCOL_VERSION = PhoenixChannelsProtocolVersion.V2

# A full topic queue logs its first dropped message, then every this many.
_DROP_WARNING_INTERVAL = 100


class _RawFrame(NamedTuple):
    """A decoded frame whose fields are not validated yet."""

    # The message model validates the refs.
    join_ref: Any
    ref: Any
    topic: object
    event: object
    payload: object


def _decode_v1(parsed: object, label: str) -> _RawFrame:
    match parsed:
        case dict():
            return _RawFrame(
                join_ref=parsed.get("join_ref"),
                ref=parsed.get("ref"),
                topic=parsed.get("topic"),
                event=parsed.get("event"),
                payload=parsed.get("payload"),
            )
        case _:
            raise TypeError(
                f"Protocol {label} expects object format, got {type(parsed).__name__}"
            )


def _decode_v2(parsed: object, label: str) -> _RawFrame:
    match parsed:
        case [join_ref, ref, topic, event, payload]:
            return _RawFrame(join_ref, ref, topic, event, payload)
        case list():
            raise ValueError(
                f"Protocol {label} expects 5-element array "
                "[join_ref, ref, topic, event, payload]"
            )
        case _:
            raise TypeError(
                f"Protocol {label} expects array format, got {type(parsed).__name__}"
            )


class _BinaryKind(IntEnum):
    PUSH = 0
    REPLY = 1
    BROADCAST = 2


# Header fields per kind (Phoenix.Socket.V2.JSONSerializer):
# push: join_ref, topic, event. reply: join_ref, ref, topic, status.
# broadcast: topic, event.
_BINARY_FIELD_COUNTS = {
    _BinaryKind.PUSH: 3,
    _BinaryKind.REPLY: 4,
    _BinaryKind.BROADCAST: 2,
}

_KIND_SIZE = 1
_FIELD_SIZE_BYTES = 1
_MAX_FIELD_SIZE = (1 << (8 * _FIELD_SIZE_BYTES)) - 1


def _split_binary_fields(
    body: bytes, count: int, label: str
) -> tuple[list[str | None], bytes]:
    """Read ``count`` length-prefixed fields; an empty field is None."""
    header_size = count * _FIELD_SIZE_BYTES
    if len(body) < header_size:
        raise ValueError(f"Protocol {label} binary frame has a truncated header")
    offset = header_size
    fields: list[str | None] = []
    for size in body[:header_size]:
        field = body[offset : offset + size]
        if len(field) < size:
            raise ValueError(f"Protocol {label} binary frame has a truncated field")
        try:
            fields.append(field.decode() or None)
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"Protocol {label} binary frame has a non-UTF-8 field"
            ) from exc
        offset += size
    return fields, body[offset:]


def _decode_v2_binary(raw: bytes, label: str) -> _RawFrame:
    if len(raw) < _KIND_SIZE:
        raise ValueError(f"Protocol {label} binary frame is empty")
    try:
        kind = _BinaryKind(raw[0])
    except ValueError:
        raise ValueError(
            f"Protocol {label} binary frame has unknown kind {raw[0]}"
        ) from None
    fields, data = _split_binary_fields(
        raw[_KIND_SIZE:], _BINARY_FIELD_COUNTS[kind], label
    )
    match kind, fields:
        case _BinaryKind.PUSH, [join_ref, topic, event]:
            return _RawFrame(join_ref, None, topic, event, data)
        case _BinaryKind.REPLY, [join_ref, ref, topic, status]:
            reply = {"status": status, "response": data}
            return _RawFrame(join_ref, ref, topic, PHXEvent.reply.value, reply)
        case _BinaryKind.BROADCAST, [topic, event]:
            return _RawFrame(None, None, topic, event, data)
        case _:
            raise AssertionError(kind)


def _encode_binary_field(
    value: str | None, label: str, field: str, *, required: bool = False
) -> bytes:
    """A header field's UTF-8 bytes; ``None`` is empty, which only some fields allow."""
    if required and not value:
        raise ValueError(f"Protocol {label} binary frame {field} must not be empty")
    try:
        encoded = (value or "").encode()
    except UnicodeEncodeError as exc:
        raise ValueError(
            f"Protocol {label} binary frame {field} is not valid UTF-8 text"
        ) from exc
    if len(encoded) > _MAX_FIELD_SIZE:
        raise ValueError(
            f"Protocol {label} binary frame {field} must be at most "
            f"{_MAX_FIELD_SIZE} bytes, got {len(encoded)}"
        )
    return encoded


def _encode_v2_binary(message: ChannelMessage, data: bytes, label: str) -> bytes:
    """A client push: unlike a server push, its header also carries the ref."""
    fields = [
        _encode_binary_field(message.join_ref, label, "join_ref"),
        _encode_binary_field(message.ref, label, "ref"),
        _encode_binary_field(message.topic, label, "topic", required=True),
        _encode_binary_field(str(message.event), label, "event", required=True),
    ]
    sizes = bytes(len(field) for field in fields)
    return bytes([_BinaryKind.PUSH]) + sizes + b"".join(fields) + data


def _require_text(value: object, label: str, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Protocol {label} message {field} must be a non-empty string")
    return value


def _to_message(label: str, frame: _RawFrame) -> ChannelMessage:
    return make_message(
        topic=_require_text(frame.topic, label, "topic"),
        event=Event(_require_text(frame.event, label, "event")),
        payload=frame.payload if isinstance(frame.payload, dict | bytes) else {},
        ref=frame.ref,
        join_ref=frame.join_ref,
    )


class _ProtocolSpec(NamedTuple):
    """A protocol version's ``vsn`` and how its frames decode and encode."""

    vsn: str
    label: str
    decode: Callable[[object, str], _RawFrame]
    decode_binary: Callable[[bytes, str], _RawFrame] | None = None
    encode_binary: Callable[[ChannelMessage, bytes, str], bytes] | None = None


_PROTOCOL_SPECS = {
    PhoenixChannelsProtocolVersion.V1: _ProtocolSpec("1.0.0", "v1", _decode_v1),
    PhoenixChannelsProtocolVersion.V2: _ProtocolSpec(
        "2.0.0", "v2", _decode_v2, _decode_v2_binary, _encode_v2_binary
    ),
}


class PHXProtocolHandler:
    def __init__(
        self,
        protocol_version: PhoenixChannelsProtocolVersion
        | str = DEFAULT_PROTOCOL_VERSION,
        *,
        diagnostic_logger: logging.Logger | None = None,
    ) -> None:
        # Non-v2 values select v1 for compatibility with the original client.
        self.protocol_version = (
            PhoenixChannelsProtocolVersion.V2
            if protocol_version == PhoenixChannelsProtocolVersion.V2
            else PhoenixChannelsProtocolVersion.V1
        )
        self.logger = diagnostic_logger or ConnectionLogger(
            logger.getChild("ProtocolHandler"), set()
        )
        self.logger.debug(
            "Initialized PHXProtocolHandler for protocol version %s",
            self.protocol_version,
        )

    def parse_message(self, raw_message: str | bytes) -> ChannelMessage:
        self.logger.debug("Parsing raw message: %s", raw_message)
        try:
            return self._decode_message(raw_message)
        except (TypeError, ValueError):
            self.logger.exception("Failed to parse message")
            raise
        except Exception as exc:
            self.logger.exception("Unexpected error parsing message")
            raise ValueError(f"Invalid message format: {exc}") from exc

    def _decode_message(self, raw_message: str | bytes) -> ChannelMessage:
        spec = _PROTOCOL_SPECS[self.protocol_version]
        match raw_message:
            case bytes() if spec.decode_binary is not None:
                frame = spec.decode_binary(raw_message, spec.label)
            case _:
                parsed_data = json.loads(raw_message)
                self.logger.debug("Decoded data: %s", parsed_data)
                frame = spec.decode(parsed_data, spec.label)
        return _to_message(spec.label, frame)

    def ensure_sendable(self, payload: Payload) -> None:
        """Raise ``TypeError`` if this protocol version cannot send ``payload``."""
        spec = _PROTOCOL_SPECS[self.protocol_version]
        if isinstance(payload, bytes) and spec.encode_binary is None:
            raise TypeError(f"Protocol {spec.label} cannot send a binary payload")

    def serialize_message(self, message: ChannelMessage) -> str | bytes:
        """A ``bytes`` payload encodes as a binary frame, anything else as JSON.

        Raises:
            TypeError: The payload can't be sent in this protocol version.
            ValueError: A binary frame's header field is empty, too long or not
                valid text.
        """
        self.logger.debug("Serializing message: %s", message)
        spec = _PROTOCOL_SPECS[self.protocol_version]
        self.ensure_sendable(message.payload)
        match message.payload:
            case bytes() as data if spec.encode_binary is not None:
                return spec.encode_binary(message, data, spec.label)
            case _:
                return self._serialize_json(message)

    def _serialize_json(self, message: ChannelMessage) -> str:
        try:
            if self.protocol_version == PhoenixChannelsProtocolVersion.V2:
                serialized = json.dumps(
                    [
                        message.join_ref,
                        message.ref,
                        message.topic,
                        str(message.event),
                        message.payload,
                    ]
                )
            else:
                serialized = json.dumps(
                    {
                        "topic": message.topic,
                        "event": str(message.event),
                        "ref": message.ref,
                        "payload": message.payload,
                    }
                )
        except Exception as exc:
            self.logger.exception("Failed to serialize message")
            raise TypeError(f"Cannot serialize message: {exc}") from exc
        self.logger.debug("Serialized to: %s", serialized)
        return serialized

    async def send_message(
        self, websocket: ClientConnection, message: ChannelMessage
    ) -> None:
        self.logger.debug(
            "Serializing %s to Phoenix Channels %s format",
            message,
            self.protocol_version,
        )
        frame = self.serialize_message(message)

        self.logger.debug("Sending frame: %s", frame)
        await websocket.send(frame)

    def is_stale_join(self, message: ChannelMessage, current_join_ref: str) -> bool:
        # Phoenix broadcasts have no join_ref to compare against the current join.
        return (
            self.protocol_version == PhoenixChannelsProtocolVersion.V2
            and message.join_ref is not None
            and message.join_ref != current_join_ref
        )

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        topic_subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
        on_heartbeat_response: Callable[[ChannelMessage], None] | None = None,
    ) -> None:
        self.logger.debug(
            "Starting websocket message loop for generation %s", conn_generation
        )
        async for socket_message in connection:
            phx_message = self.parse_message(socket_message)
            self.logger.debug("Processing message - %s", phx_message)
            topic = phx_message.topic

            if topic == PHOENIX_TOPIC:
                if on_heartbeat_response is not None:
                    on_heartbeat_response(phx_message)
                continue

            if topic not in topic_subscriptions:
                continue

            topic_subscription = topic_subscriptions[topic]
            if topic_subscription.conn_generation != conn_generation:
                self.logger.debug(
                    "Dropping message for stale generation on topic %s. routing_gen=%s "
                    "subscription_gen=%s",
                    topic,
                    conn_generation,
                    topic_subscription.conn_generation,
                )
                continue

            if self.is_stale_join(phx_message, topic_subscription.join_ref):
                self.logger.debug(
                    "Dropping message with stale join_ref on topic %s. got=%s "
                    "expected=%s",
                    topic,
                    phx_message.join_ref,
                    topic_subscription.join_ref,
                )
                continue

            if topic_subscription.resolve_push(phx_message):
                continue

            self._drop_oldest_if_full(topic_subscription)
            await topic_subscription.queue.put(phx_message)

    def _drop_oldest_if_full(self, topic_subscription: TopicSubscription) -> None:
        if not topic_subscription.queue.full():
            return
        try:
            topic_subscription.queue.get_nowait()
        except asyncio.QueueEmpty:
            self.logger.debug(
                "Queue became empty before drop on topic %s; skipping drop",
                topic_subscription.name,
            )
            return
        topic_subscription.dropped_message_count += 1
        if (
            topic_subscription.dropped_message_count == 1
            or topic_subscription.dropped_message_count % _DROP_WARNING_INTERVAL == 0
        ):
            self.logger.warning(
                "Dropped %s queued messages for topic %s due to full queue",
                topic_subscription.dropped_message_count,
                topic_subscription.name,
            )
