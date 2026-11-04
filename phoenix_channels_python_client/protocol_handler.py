from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from websockets import ClientConnection

from phoenix_channels_python_client.codec import decode_message, encode_message
from phoenix_channels_python_client.diagnostics import ConnectionLogger
from phoenix_channels_python_client.messages import PHOENIX_TOPIC, Message
from phoenix_channels_python_client.protocol import (
    DEFAULT_PROTOCOL_VERSION,
    ProtocolVersion,
)
from phoenix_channels_python_client.topic_subscription import TopicSubscription

logger = logging.getLogger(__name__)


class PHXProtocolHandler:
    def __init__(
        self,
        protocol_version: ProtocolVersion | str = DEFAULT_PROTOCOL_VERSION,
        *,
        diagnostic_logger: logging.Logger | None = None,
    ) -> None:
        self.protocol_version = ProtocolVersion(protocol_version)
        self.logger = diagnostic_logger or ConnectionLogger(
            logger.getChild("ProtocolHandler"), set()
        )
        self.logger.debug(
            "Initialized PHXProtocolHandler for protocol version %s",
            self.protocol_version,
        )

    async def send_message(self, websocket: ClientConnection, message: Message) -> None:
        await websocket.send(encode_message(message, self.protocol_version))

    def is_stale_join(self, message: Message, current_join_ref: str) -> bool:
        # Phoenix broadcasts have no join_ref to compare against the current join.
        return (
            self.protocol_version == ProtocolVersion.V2
            and message.join_ref is not None
            and message.join_ref != current_join_ref
        )

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        topic_subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
        on_heartbeat_response: Callable[[Message], None] | None = None,
        on_channel_control: Callable[[TopicSubscription, Message, str | None], bool]
        | None = None,
    ) -> None:
        self.logger.debug(
            "Starting websocket message loop for generation %s", conn_generation
        )
        async for socket_message in connection:
            self.logger.debug(
                "Parsing raw message: %s frame",
                "binary" if isinstance(socket_message, bytes) else "text",
            )
            decoded = decode_message(socket_message, self.protocol_version)
            phx_message = decoded.message
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

            if on_channel_control is not None and on_channel_control(
                topic_subscription, phx_message, decoded.binary_reply_status
            ):
                continue

            await self._enqueue_message(topic_subscription, phx_message)

    async def _enqueue_message(
        self, topic_subscription: TopicSubscription, phx_message: Message
    ) -> None:
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
                        topic_subscription.name,
                    )
            except asyncio.QueueEmpty:
                self.logger.debug(
                    "Queue became empty before drop on topic %s; skipping drop",
                    topic_subscription.name,
                )

        await topic_subscription.queue.put(phx_message)
