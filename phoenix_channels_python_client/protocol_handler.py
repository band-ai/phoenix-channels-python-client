from __future__ import annotations

import asyncio
import json
import logging
from enum import Enum

from websockets import ClientConnection

from phoenix_channels_python_client.phx_messages import Message
from phoenix_channels_python_client.topic_subscription import TopicSubscription

logger = logging.getLogger(__name__)


class PhoenixChannelsProtocolVersion(Enum):
    V1 = "1.0"
    V2 = "2.0"


class PHXProtocolHandler:
    def __init__(
        self,
        protocol_version: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
    ):
        self.protocol_version = protocol_version
        self.logger = logger.getChild("ProtocolHandler")
        self.logger.debug(
            "Initialized PHXProtocolHandler for protocol version %s",
            self.protocol_version.value,
        )

    def parse_message(self, raw_message: str | bytes) -> Message:
        self.logger.debug("Parsing raw message: %s", raw_message)
        try:
            parsed_data = json.loads(raw_message)
            self.logger.debug("Decoded data: %s", parsed_data)
            if self.protocol_version == PhoenixChannelsProtocolVersion.V2:
                return Message.from_raw(parsed_data)

            if not isinstance(parsed_data, dict):
                raise TypeError(
                    "Protocol v1 expects object format, "
                    f"got {type(parsed_data).__name__}"
                )

            topic = parsed_data.get("topic")
            event = parsed_data.get("event")
            payload = parsed_data.get("payload", {})
            if not isinstance(topic, str) or not topic:
                raise TypeError("Protocol v1 message topic must be a non-empty string")
            if not isinstance(event, str) or not event:
                raise TypeError("Protocol v1 message event must be a non-empty string")
            if not isinstance(payload, dict):
                payload = {}

            return Message(
                topic=topic,
                event=event,
                payload=payload,
                ref=parsed_data.get("ref"),
                join_ref=parsed_data.get("join_ref"),
            )
        except (TypeError, ValueError):
            self.logger.exception("Failed to parse message")
            raise
        except Exception as exc:
            self.logger.exception("Unexpected error parsing message")
            raise ValueError(f"Invalid message format: {exc}") from exc

    def serialize_message(self, message: Message) -> str:
        self.logger.debug("Serializing message: %s", message)
        try:
            if self.protocol_version == PhoenixChannelsProtocolVersion.V2:
                serialized = json.dumps(message.to_raw())
            else:
                serialized = json.dumps(
                    {
                        "topic": message.topic,
                        "event": str(message.event),
                        "ref": message.ref,
                        "payload": message.payload,
                    }
                )
            self.logger.debug("Serialized to: %s", serialized)
            return serialized
        except Exception as exc:
            self.logger.exception("Failed to serialize message")
            raise TypeError(f"Cannot serialize message: {exc}") from exc

    async def send_message(self, websocket: ClientConnection, message: Message) -> None:
        self.logger.debug("Serialising %s to Phoenix Channels v2 format", message)
        text_message = self.serialize_message(message)

        self.logger.debug("Sending as TEXT frame: %s", text_message)
        await websocket.send(text_message)

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        topic_subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
    ) -> None:
        self.logger.debug("Starting websocket message loop for generation %s", conn_generation)
        async for socket_message in connection:
            phx_message = self.parse_message(socket_message)
            self.logger.debug("Processing message - %s", phx_message)
            topic = phx_message.topic

            if topic not in topic_subscriptions:
                continue

            topic_subscription = topic_subscriptions[topic]
            if topic_subscription.conn_generation != conn_generation:
                self.logger.debug(
                    "Dropping message for stale generation on topic %s. got=%s expected=%s",
                    topic,
                    conn_generation,
                    topic_subscription.conn_generation,
                )
                continue

            if topic_subscription.join_ref != phx_message.join_ref:
                self.logger.debug(
                    "Dropping message with stale join_ref on topic %s. got=%s expected=%s",
                    topic,
                    phx_message.join_ref,
                    topic_subscription.join_ref,
                )
                continue

            if topic_subscription.queue.full():
                try:
                    topic_subscription.queue.get_nowait()
                    topic_subscription.dropped_message_count += 1
                    if topic_subscription.dropped_message_count % 100 == 0:
                        self.logger.warning(
                            "Dropped %s queued messages for topic %s due to full queue",
                            topic_subscription.dropped_message_count,
                            topic,
                        )
                except asyncio.QueueEmpty:
                    self.logger.debug(
                        "Queue became empty before drop on topic %s; skipping drop", topic
                    )

            await topic_subscription.queue.put(phx_message)
