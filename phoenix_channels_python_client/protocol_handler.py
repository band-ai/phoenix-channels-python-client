from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from enum import StrEnum
from typing import Any, NamedTuple

from websockets import ClientConnection

from phoenix_channels_python_client.phx_messages import (
    PHOENIX_TOPIC,
    ChannelMessage,
    Event,
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


class _RawFrame(NamedTuple):
    """A decoded frame whose fields are not validated yet."""

    # The message model validates the refs.
    join_ref: Any
    ref: Any
    topic: object
    event: object
    payload: object


def _decode_v1(parsed: object) -> _RawFrame:
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
                f"Protocol v1 expects object format, got {type(parsed).__name__}"
            )


def _decode_v2(parsed: object) -> _RawFrame:
    match parsed:
        case [join_ref, ref, topic, event, payload]:
            return _RawFrame(join_ref, ref, topic, event, payload)
        case list():
            raise ValueError(
                "Protocol v2 expects 5-element array "
                "[join_ref, ref, topic, event, payload]"
            )
        case _:
            raise TypeError(
                f"Protocol v2 expects array format, got {type(parsed).__name__}"
            )


def _require_text(value: object, label: str, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Protocol {label} message {field} must be a non-empty string")
    return value


def _to_message(label: str, frame: _RawFrame) -> ChannelMessage:
    return make_message(
        topic=_require_text(frame.topic, label, "topic"),
        event=Event(_require_text(frame.event, label, "event")),
        payload=frame.payload if isinstance(frame.payload, dict) else {},
        ref=frame.ref,
        join_ref=frame.join_ref,
    )


class _ProtocolSpec(NamedTuple):
    """A protocol version's ``vsn`` and how its frames decode."""

    vsn: str
    label: str
    decode: Callable[[object], _RawFrame]


_PROTOCOL_SPECS = {
    PhoenixChannelsProtocolVersion.V1: _ProtocolSpec("1.0.0", "v1", _decode_v1),
    PhoenixChannelsProtocolVersion.V2: _ProtocolSpec("2.0.0", "v2", _decode_v2),
}


class PHXProtocolHandler:
    def __init__(
        self,
        protocol_version: PhoenixChannelsProtocolVersion = DEFAULT_PROTOCOL_VERSION,
    ) -> None:
        self.protocol_version = protocol_version
        self.logger = logger.getChild("ProtocolHandler")
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
        parsed_data = json.loads(raw_message)
        self.logger.debug("Decoded data: %s", parsed_data)
        spec = _PROTOCOL_SPECS[self.protocol_version]
        return _to_message(spec.label, spec.decode(parsed_data))

    def serialize_message(self, message: ChannelMessage) -> str:
        self.logger.debug("Serializing message: %s", message)
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
        text_message = self.serialize_message(message)

        self.logger.debug("Sending as TEXT frame: %s", text_message)
        await websocket.send(text_message)

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

            if (
                self.protocol_version == PhoenixChannelsProtocolVersion.V2
                and topic_subscription.join_ref != phx_message.join_ref
            ):
                self.logger.debug(
                    "Dropping message with stale join_ref on topic %s. got=%s "
                    "expected=%s",
                    topic,
                    phx_message.join_ref,
                    topic_subscription.join_ref,
                )
                continue

            if topic_subscription.queue.full():
                try:
                    topic_subscription.queue.get_nowait()
                    topic_subscription.dropped_message_count += 1
                    if (
                        topic_subscription.dropped_message_count == 1
                        or topic_subscription.dropped_message_count % 100 == 0
                    ):
                        self.logger.warning(
                            "Dropped %s queued messages for topic %s due to full queue",
                            topic_subscription.dropped_message_count,
                            topic,
                        )
                except asyncio.QueueEmpty:
                    self.logger.debug(
                        "Queue became empty before drop on topic %s; skipping drop",
                        topic,
                    )

            await topic_subscription.queue.put(phx_message)
