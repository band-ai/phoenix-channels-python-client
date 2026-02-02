import logging
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union
from websockets import ClientConnection

import json
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from phoenix_channels_python_client.utils import make_message
from phoenix_channels_python_client.topic_subscription import TopicSubscription

# Type alias for message handler callbacks that return True if handled, False otherwise
UnhandledMessageCallback = Callable[[ChannelMessage], bool]


class PhoenixChannelsProtocolVersion(Enum):
    """Phoenix Channels protocol versions"""

    V1 = "1.0"
    V2 = "2.0"


class PHXProtocolHandler:
    def __init__(self, protocol_version: PhoenixChannelsProtocolVersion):
        self.protocol_version = protocol_version.value
        self.logger = logging.getLogger(f"{__name__}.ProtocolHandler")

    def parse_message(self, raw_message: Union[str, bytes]) -> ChannelMessage:
        try:
            parsed_data = json.loads(raw_message)

            if self.protocol_version == PhoenixChannelsProtocolVersion.V2.value:
                if not isinstance(parsed_data, list):
                    raise ValueError(
                        f"Protocol v{self.protocol_version} expects array format, got object"
                    )
                if len(parsed_data) != 5:
                    raise ValueError(
                        f"Protocol v{self.protocol_version} expects 5-element array, got {len(parsed_data)}"
                    )

                topic: str = parsed_data[2]
                event_str: str = parsed_data[3]
                ref: Optional[str] = parsed_data[1]
                payload: dict[str, Any] = parsed_data[4] or {}
                join_ref: Optional[str] = parsed_data[0]
                return make_message(
                    event=Event(event_str),
                    topic=topic,
                    ref=ref,
                    payload=payload,
                    join_ref=join_ref,
                )
            else:
                if not isinstance(parsed_data, dict):
                    raise ValueError(
                        f"Protocol v{self.protocol_version} expects object format, got {type(parsed_data).__name__}"
                    )

                required_fields = ["topic", "event", "payload"]
                for field in required_fields:
                    if field not in parsed_data:
                        raise ValueError(f"Missing required field '{field}'")

                topic = str(parsed_data["topic"])
                event_str = str(parsed_data["event"])
                ref = parsed_data.get("ref")
                payload = parsed_data.get("payload", {})
                join_ref = parsed_data.get("join_ref")
                return make_message(
                    event=Event(event_str),
                    topic=topic,
                    ref=ref if ref is None else str(ref),
                    payload=payload if isinstance(payload, dict) else {},
                    join_ref=join_ref if join_ref is None else str(join_ref),
                )

        except Exception as e:
            self.logger.error("Failed to parse message %s: %s", raw_message, e)
            raise ValueError(f"Invalid message format: {e}") from e

    def serialize_message(self, message: ChannelMessage) -> str:
        try:
            if self.protocol_version == PhoenixChannelsProtocolVersion.V2.value:
                join_ref = message.join_ref
                msg_ref = message.ref
                message_array = [
                    join_ref,
                    msg_ref,
                    message.topic,
                    str(message.event),
                    message.payload,
                ]
                serialized = json.dumps(message_array)
            else:
                v1_message = {
                    "topic": message.topic,
                    "event": str(message.event),
                    "ref": message.ref,
                    "payload": message.payload,
                }
                serialized = json.dumps(v1_message)

            return serialized

        except Exception as e:
            self.logger.error("Failed to serialize message %s: %s", message, e)
            raise TypeError(f"Cannot serialize message: {e}") from e

    def get_protocol_version(self) -> str:
        return self.protocol_version

    def set_protocol_version(self, version: str) -> None:
        self.logger.info(
            "Changing protocol version from %s to %s", self.protocol_version, version
        )
        self.protocol_version = version

    async def send_message(
        self, websocket: ClientConnection, message: ChannelMessage
    ) -> None:
        text_message = self.serialize_message(message)
        await websocket.send(text_message)

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        topic_subscriptions: Dict[str, TopicSubscription],
        on_unhandled_message: Optional[UnhandledMessageCallback] = None,
    ) -> None:
        """
        Process incoming WebSocket messages and route them to appropriate handlers.

        Args:
            connection: The WebSocket connection to read messages from.
            topic_subscriptions: Dictionary mapping topic names to their subscriptions.
            on_unhandled_message: Optional callback for messages that don't match any
                subscribed topic (e.g., heartbeat responses on "phoenix" topic).
                The callback should return True if it handled the message, False otherwise.
        """
        async for socket_message in connection:
            phx_message = self.parse_message(socket_message)
            topic = phx_message.topic

            # First, check if this is a message for an unsubscribed topic
            # that should be handled specially (e.g., heartbeat responses)
            if topic not in topic_subscriptions:
                if on_unhandled_message is not None:
                    handled = on_unhandled_message(phx_message)
                    if handled:
                        continue
                # Message for unknown topic, skip it
                continue

            topic_subscription = topic_subscriptions[topic]
            if (
                self.protocol_version == "2.0"
                and topic_subscription.join_ref != phx_message.join_ref
            ):
                self.logger.warning(
                    "Ignoring message for topic %s with mismatched join_ref", topic
                )
                continue
            await topic_subscription.queue.put(phx_message)
