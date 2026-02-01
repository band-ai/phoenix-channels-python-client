import logging
from enum import Enum
from typing import Dict, Union
from websockets import ClientConnection

import json
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.utils import make_message
from phoenix_channels_python_client.topic_subscription import TopicSubscription


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

                message_dict = {
                    "topic": parsed_data[2],
                    "event": parsed_data[3],
                    "ref": parsed_data[1],
                    "payload": parsed_data[4] or {},
                    "join_ref": parsed_data[0],  # join_ref from first element
                }
            else:
                if not isinstance(parsed_data, dict):
                    raise ValueError(
                        f"Protocol v{self.protocol_version} expects object format, got {type(parsed_data).__name__}"
                    )

                message_dict = parsed_data

            required_fields = ["topic", "event", "payload"]
            for field in required_fields:
                if field not in message_dict:
                    raise ValueError(f"Missing required field '{field}'")

            return make_message(**message_dict)

        except Exception as e:
            self.logger.error(f"Failed to parse message {raw_message}: {e}")
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
            self.logger.error(f"Failed to serialize message {message}: {e}")
            raise TypeError(f"Cannot serialize message: {e}") from e

    def get_protocol_version(self) -> str:
        return self.protocol_version

    def set_protocol_version(self, version: str) -> None:
        self.logger.info(
            f"Changing protocol version from {self.protocol_version} to {version}"
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
    ) -> None:
        async for socket_message in connection:
            phx_message = self.parse_message(socket_message)
            topic = phx_message.topic

            if topic in topic_subscriptions:
                topic_subscription = topic_subscriptions[topic]
                if (
                    self.protocol_version == "2.0"
                    and topic_subscription.join_ref != phx_message.join_ref
                ):
                    self.logger.warning(
                        f"Ignoring message for topic {topic} with mismatched join_ref"
                    )
                    continue
                await topic_subscription.queue.put(phx_message)
