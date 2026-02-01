import asyncio
import logging
import signal
from asyncio import AbstractEventLoop, Queue
from types import TracebackType
from typing import Callable, Optional, Type, Awaitable, Dict, Any

from websockets import connect

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import (
    ChannelMessage,
    ChannelEvent,
    PHXEvent,
    PHXEventMessage,
)
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.topic_subscription import (
    TopicSubscription,
    TopicProcessingState,
)
from phoenix_channels_python_client.utils import make_message


class PHXChannelsClient:
    """
    Async Python client for Phoenix Channels WebSocket connections.

    Security Note:
        This client passes the API key as a URL query parameter during the WebSocket
        handshake. While the connection uses WSS (encrypted), the API key may appear
        in server access logs, proxy logs, or network monitoring tools. Ensure your
        infrastructure does not log full URLs in production environments.

        The official Phoenix JS client (v1.8+) supports header-based authentication
        via the `authToken` option, which avoids this issue. This client currently
        uses the older `params` style for compatibility.
    """

    def __init__(
        self,
        websocket_url: str,
        api_key: str,
        event_loop: Optional[AbstractEventLoop] = None,
        protocol_version: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
    ):
        self.logger = logging.getLogger(__name__)

        vsn = (
            "2.0.0"
            if protocol_version == PhoenixChannelsProtocolVersion.V2
            else "1.0.0"
        )
        self.channel_socket_url = f"{websocket_url}?api_key={api_key}&vsn={vsn}"

        self.connection = None
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self._loop = event_loop or asyncio.get_event_loop()
        self._message_routing_task = None
        self._protocol_handler = PHXProtocolHandler(protocol_version)
        self._ref_counter = 0

    async def __aenter__(self) -> "PHXChannelsClient":
        try:
            self.connection = await connect(self.channel_socket_url)
            self.logger.info("Connected to Phoenix WebSocket server")
            self._message_routing_task = self._loop.create_task(
                self._start_processing()
            )
            return self
        except Exception as e:
            self.logger.error(f"Failed to connect to Phoenix WebSocket server: {e}")
            raise PHXConnectionError(
                f"Failed to connect to {self.channel_socket_url}: {e}"
            ) from e

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        await self.shutdown("Client context exiting")

    async def shutdown(
        self,
        reason: str,
    ) -> None:
        """
        Gracefully shutdown the client connection.

        This method will:
        1. Unsubscribe from all topics (with 5 second timeout)
        2. Cancel the message routing task
        3. Close the WebSocket connection

        Args:
            reason: Human-readable reason for shutdown (for logging)

        Note: This method is automatically called by __aexit__ when using
        the async context manager. You can also call it explicitly.
        """
        self.logger.info(f"Shutting down client: {reason}")

        topics_to_unsubscribe = list(self._topic_subscriptions.keys())
        if topics_to_unsubscribe:
            self.logger.info(
                f"Unsubscribing from {len(topics_to_unsubscribe)} topic(s)"
            )
            unsubscribe_tasks = [
                self.unsubscribe_from_topic(topic) for topic in topics_to_unsubscribe
            ]

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*unsubscribe_tasks, return_exceptions=True),
                    timeout=5.0,
                )

                for topic, result in zip(topics_to_unsubscribe, results):
                    if isinstance(result, Exception):
                        self.logger.warning(
                            f"Failed to unsubscribe from topic {topic}: {result}"
                        )
                        self._unregister_topic(topic)
            except asyncio.TimeoutError:
                self.logger.warning("Unsubscribe timed out after 5s, forcing cleanup")
                for topic in topics_to_unsubscribe:
                    self._unregister_topic(topic)

        if self._message_routing_task and not self._message_routing_task.done():
            self._message_routing_task.cancel()
            try:
                await self._message_routing_task
            except asyncio.CancelledError:
                pass

        if self.connection:
            await self.connection.close()
            self.connection = None
            self.logger.info("Connection closed")

    def _set_subscription_ready(self, topic_subscription: TopicSubscription) -> None:
        if not topic_subscription.subscription_ready.done():
            topic_subscription.subscription_ready.set_result(None)

    def _set_subscription_error(
        self, topic_subscription: TopicSubscription, error: Exception
    ) -> None:
        if not topic_subscription.subscription_ready.done():
            topic_subscription.subscription_ready.set_exception(error)

    def _determine_processing_state(
        self, topic: TopicSubscription
    ) -> TopicProcessingState:
        subscription_ready = topic.subscription_ready.done()
        leave_requested = topic.leave_requested.is_set()

        if not subscription_ready:
            return TopicProcessingState.WAITING_FOR_JOIN
        elif leave_requested:
            return TopicProcessingState.PROCESSING_LEAVE
        else:
            return TopicProcessingState.NORMAL_PROCESSING

    async def _process_topic_messages(self, topic_name: str) -> None:
        topic = self._topic_subscriptions[topic_name]

        try:
            while True:
                message = await topic.queue.get()

                current_state = self._determine_processing_state(topic)

                if current_state == TopicProcessingState.WAITING_FOR_JOIN:
                    await self._handle_join_response_mode(topic, message)

                elif current_state == TopicProcessingState.PROCESSING_LEAVE:
                    try:
                        await self._handle_leave_mode(topic, message)
                    except PHXTopicError:
                        break

                elif current_state == TopicProcessingState.NORMAL_PROCESSING:
                    await self._handle_normal_message_mode(topic, message)

        except Exception as e:
            self.logger.error(f"Error in topic processor for {topic.name}: {e}")
            self._unregister_topic(topic.name)

    async def _handle_join_response_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        if not isinstance(message, PHXEventMessage) or message.event != PHXEvent.reply:
            raise PHXTopicError(
                f"Unexpected message type in join response mode: {message}"
            )

        if message.payload.get("status") == "ok":
            self._set_subscription_ready(topic)
            self.logger.info(f"Subscribed to topic: {topic.name}")
        else:
            response = message.payload.get("response", {})
            error_message = (
                response.get("reason", "invalid topic")
                if isinstance(response, dict)
                else "invalid topic"
            )

            error = PHXTopicError(error_message)
            self._set_subscription_error(topic, error)
            self.logger.error(
                f"Failed to subscribe to topic {topic.name}: {error_message}"
            )
            raise error

    def _capture_handlers_atomically(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> tuple:
        message_handler = topic.async_callback
        event_handler = topic.get_event_handler(message.event)
        return message_handler, event_handler

    async def _handle_normal_message_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        message_handler, event_handler = self._capture_handlers_atomically(
            topic, message
        )

        try:
            has_message_handler = message_handler is not None
            has_specific_handler = event_handler is not None

            if has_message_handler:
                topic.current_callback_task = asyncio.create_task(
                    message_handler(message)
                )
                await topic.current_callback_task
                topic.current_callback_task = None

            if has_specific_handler:
                topic.current_callback_task = asyncio.create_task(
                    event_handler(message.payload)
                )
                await topic.current_callback_task

            if not has_message_handler and not has_specific_handler:
                self.logger.warning(
                    f"No handler for event {message.event} on topic {topic.name}"
                )

        except Exception as e:
            self.logger.error(f"Error in topic callback for {topic.name}: {e}")
        finally:
            topic.current_callback_task = None

    async def _handle_leave_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        if not isinstance(message, PHXEventMessage) or message.event != PHXEvent.reply:
            return

        is_leave_success = message.payload.get("status") == "ok"

        if is_leave_success:
            self.logger.info(f"Unsubscribed from topic: {topic.name}")

            if topic.current_callback_task and not topic.current_callback_task.done():
                try:
                    await topic.current_callback_task
                except Exception as e:
                    self.logger.error(
                        f"Error waiting for callback to finish for {topic.name}: {e}"
                    )

            if topic.unsubscribe_completed and not topic.unsubscribe_completed.done():
                topic.unsubscribe_completed.set_result(None)

        else:
            self.logger.error(
                f"Failed to unsubscribe from topic {topic.name}: {message.payload}"
            )
            if topic.unsubscribe_completed and not topic.unsubscribe_completed.done():
                topic.unsubscribe_completed.set_exception(
                    PHXTopicError(f"Failed to unsubscribe: {message.payload}")
                )
            raise PHXTopicError(f"Failed to unsubscribe: {message.payload}")

    def _unregister_topic(self, topic_name: str) -> None:
        if topic_name in self._topic_subscriptions:
            topic_subscription = self._topic_subscriptions[topic_name]
            if topic_subscription.process_topic_messages_task:
                topic_subscription.process_topic_messages_task.cancel()
            del self._topic_subscriptions[topic_name]

    def get_current_subscriptions(self) -> dict[str, TopicSubscription]:
        return self._topic_subscriptions.copy()

    def get_protocol_handler(self) -> PHXProtocolHandler:
        return self._protocol_handler

    def _generate_ref(self) -> str:
        self._ref_counter += 1
        return str(self._ref_counter)

    async def subscribe_to_topic(
        self,
        topic: str,
        async_callback: Optional[Callable[[ChannelMessage], Awaitable[None]]] = None,
    ) -> None:
        if topic in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} already subscribed")

        topic_queue = Queue()
        subscription_ready_future = self._loop.create_future()
        join_ref = self._generate_ref()

        topic_subscription = TopicSubscription(
            name=topic,
            async_callback=async_callback,
            queue=topic_queue,
            subscription_ready=subscription_ready_future,
            join_ref=join_ref,
            process_topic_messages_task=self._loop.create_task(
                self._process_topic_messages(topic)
            ),
        )

        self._topic_subscriptions[topic] = topic_subscription
        topic_join_message = make_message(
            event=PHXEvent.join, topic=topic, ref=join_ref, join_ref=join_ref
        )
        await self._protocol_handler.send_message(self.connection, topic_join_message)

        try:
            await subscription_ready_future
        except Exception as e:
            self.logger.error(f"Failed to subscribe to {topic}: {e}")
            self._unregister_topic(topic)
            raise

    async def unsubscribe_from_topic(self, topic: str) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]

        unsubscribe_completed_future = self._loop.create_future()
        topic_subscription.unsubscribe_completed = unsubscribe_completed_future

        leave_ref = self._generate_ref()
        topic_leave_message = make_message(
            event=PHXEvent.leave,
            topic=topic,
            ref=leave_ref,
            join_ref=topic_subscription.join_ref,
        )
        await self._protocol_handler.send_message(self.connection, topic_leave_message)

        topic_subscription.leave_requested.set()

        try:
            await unsubscribe_completed_future
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"Error unsubscribing from {topic}: {e}")
            raise
        finally:
            self._unregister_topic(topic)

    def add_event_handler(
        self,
        topic: str,
        event: ChannelEvent,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Add or update an event handler for a specific event type on a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.add_event_handler(event, handler)

    def remove_event_handler(self, topic: str, event: ChannelEvent) -> None:
        """Remove an event handler for a specific event type on a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.remove_event_handler(event)

    def get_event_handler(
        self, topic: str, event: ChannelEvent
    ) -> Optional[Callable[[Dict[str, Any]], Awaitable[None]]]:
        """Get the handler for a specific event type on a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.get_event_handler(event)

    def has_event_handler(self, topic: str, event: ChannelEvent) -> bool:
        """Check if a handler exists for a specific event type on a topic."""
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.has_event_handler(event)

    def list_event_handlers(
        self, topic: str
    ) -> Dict[ChannelEvent, Callable[[Dict[str, Any]], Awaitable[None]]]:
        """List all event handlers for a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.event_handlers.copy()

    def set_message_handler(
        self, topic: str, handler: Callable[[ChannelMessage], Awaitable[None]]
    ) -> None:
        """Set or update the message handler for a topic. This handler receives all messages."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.async_callback = handler

    def remove_message_handler(self, topic: str) -> None:
        """Remove the message handler for a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.async_callback = None

    def get_message_handler(
        self, topic: str
    ) -> Optional[Callable[[ChannelMessage], Awaitable[None]]]:
        """Get the current message handler for a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback

    def has_message_handler(self, topic: str) -> bool:
        """Check if a message handler exists for a topic."""
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback is not None

    async def _start_processing(self) -> None:
        await self._protocol_handler.process_websocket_messages(
            self.connection, self._topic_subscriptions
        )

    async def run_forever(self) -> None:
        """
        Run until connection closes or Ctrl+C is pressed.

        This method registers signal handlers for SIGINT (Ctrl+C) and SIGTERM
        to enable graceful shutdown. When a signal is received, the client will:
        1. Send leave messages to all subscribed topics
        2. Wait for server acknowledgments (up to 5 seconds)
        3. Close the connection cleanly

        Note: Signal handlers are automatically cleaned up when this method exits.
        If you need custom signal handling, consider managing signals at the
        application level and calling shutdown() explicitly.

        Raises:
            PHXConnectionError: If client is not connected
            Exception: If the WebSocket connection fails
        """
        if self._message_routing_task is None:
            raise PHXConnectionError(
                "Client is not connected. Use 'async with' context manager."
            )

        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def signal_handler():
            shutdown_event.set()

        # Register asyncio signal handlers for graceful shutdown
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

        try:
            # Wait for either the message routing task to complete or shutdown signal
            await asyncio.wait(
                [
                    self._message_routing_task,
                    asyncio.create_task(shutdown_event.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            # When this returns, either connection closed or Ctrl+C was pressed
            # In both cases, we exit and let __aexit__ handle cleanup via shutdown()
        finally:
            # Remove signal handlers
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)
