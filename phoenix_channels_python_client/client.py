from __future__ import annotations

import asyncio
import logging
import signal
from asyncio import AbstractEventLoop, Queue
from types import TracebackType
from typing import Callable, Type, Awaitable, Dict, Any

from websockets import connect

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import (
    ChannelMessage,
    ChannelEvent,
    Event,
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


DEFAULT_HEARTBEAT_INTERVAL_SECS = 30

DEFAULT_RECONNECT_MAX_ATTEMPTS = 10
DEFAULT_RECONNECT_BACKOFF_BASE = 1.0
DEFAULT_RECONNECT_BACKOFF_MAX = 30.0

# Type alias for reconnection callbacks
ReconnectCallback = Callable[[], Awaitable[None]]
DisconnectCallback = Callable[[Exception | None], Awaitable[None]]


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

    Args:
        websocket_url: The WebSocket URL to connect to.
        api_key: The API key for authentication.
        event_loop: Optional event loop to use (defaults to current running loop).
        protocol_version: Phoenix Channels protocol version (default: V2).
        heartbeat_interval_secs: Interval between heartbeat messages in seconds.
            Set to None to disable heartbeat. Default is 30 seconds, matching
            the Phoenix JS client.
        auto_reconnect: Whether to automatically reconnect on connection loss.
            Default is True.
        reconnect_max_attempts: Maximum number of reconnection attempts before
            giving up. Default is 10. Set to 0 for unlimited attempts.
        reconnect_backoff_base: Base delay in seconds for exponential backoff.
            Default is 1.0 second.
        reconnect_backoff_max: Maximum delay in seconds between reconnection
            attempts. Default is 30 seconds.
        on_reconnect: Optional async callback called after successful reconnection.
        on_disconnect: Optional async callback called when disconnection is detected.
            Receives the exception that caused the disconnect (if any).
    """

    def __init__(
        self,
        websocket_url: str,
        api_key: str,
        event_loop: AbstractEventLoop | None = None,
        protocol_version: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_secs: float | None = DEFAULT_HEARTBEAT_INTERVAL_SECS,
        auto_reconnect: bool = True,
        reconnect_max_attempts: int = DEFAULT_RECONNECT_MAX_ATTEMPTS,
        reconnect_backoff_base: float = DEFAULT_RECONNECT_BACKOFF_BASE,
        reconnect_backoff_max: float = DEFAULT_RECONNECT_BACKOFF_MAX,
        on_reconnect: ReconnectCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
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

        # Heartbeat configuration
        self._heartbeat_interval_secs = heartbeat_interval_secs
        self._heartbeat_task: asyncio.Task | None = None
        self._pending_heartbeat_ref: str | None = None

        # Reconnection configuration
        self._auto_reconnect = auto_reconnect
        self._reconnect_max_attempts = reconnect_max_attempts
        self._reconnect_backoff_base = reconnect_backoff_base
        self._reconnect_backoff_max = reconnect_backoff_max
        self._on_reconnect = on_reconnect
        self._on_disconnect = on_disconnect

        # Reconnection state
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_attempt = 0
        self._is_reconnecting = False
        self._shutdown_requested = False

        # Store subscription info for reconnection (topic -> callback)
        self._subscription_callbacks: Dict[
            str, Callable[[ChannelMessage], Awaitable[None]] | None
        ] = {}
        self._subscription_event_handlers: Dict[
            str, Dict[ChannelEvent, Callable[[Dict[str, Any]], Awaitable[None]]]
        ] = {}

    async def __aenter__(self) -> "PHXChannelsClient":
        await self._connect()
        return self

    async def _connect(self) -> None:
        """
        Establish WebSocket connection and start background tasks.

        This is an internal method used by both initial connection and reconnection.
        """
        try:
            self.connection = await connect(self.channel_socket_url)
            self.logger.info("Connected to Phoenix WebSocket server")
            self._message_routing_task = self._loop.create_task(
                self._start_processing_with_reconnect()
            )
            # Start heartbeat loop if enabled
            if self._heartbeat_interval_secs is not None:
                self._heartbeat_task = self._loop.create_task(self._heartbeat_loop())
                self.logger.debug(
                    "Heartbeat enabled with interval of %s seconds",
                    self._heartbeat_interval_secs,
                )
            # Reset reconnection state on successful connection
            self._reconnect_attempt = 0
            self._is_reconnecting = False
        except Exception as e:
            self.logger.error("Failed to connect to Phoenix WebSocket server: %s", e)
            raise PHXConnectionError(
                f"Failed to connect to {self.channel_socket_url}: {e}"
            ) from e

    async def _start_processing_with_reconnect(self) -> None:
        """
        Wrapper around _start_processing that handles disconnection and triggers reconnection.
        """
        try:
            await self._start_processing()
        except Exception as e:
            # Connection closed or error occurred
            if not self._shutdown_requested:
                self.logger.warning("Connection lost: %s", e)
                await self._handle_disconnection(e)
        finally:
            # If processing ended without exception (clean close)
            if not self._shutdown_requested and not self._is_reconnecting:
                self.logger.warning("Connection closed unexpectedly")
                await self._handle_disconnection(None)

    async def _handle_disconnection(self, error: Exception | None) -> None:
        """
        Handle disconnection by notifying callback and starting reconnection if enabled.
        """
        # Notify disconnect callback
        if self._on_disconnect:
            try:
                await self._on_disconnect(error)
            except Exception as cb_error:
                self.logger.error("Error in on_disconnect callback: %s", cb_error)

        # Clean up current connection state
        await self._cleanup_connection()

        # Start reconnection if enabled and not shutting down
        if self._auto_reconnect and not self._shutdown_requested:
            self._reconnect_task = self._loop.create_task(self._reconnect_loop())

    async def _cleanup_connection(self) -> None:
        """
        Clean up connection-related resources without full shutdown.
        """
        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
            self._pending_heartbeat_ref = None

        # Close connection if still open
        if self.connection:
            try:
                await self.connection.close()
            except Exception:
                pass
            self.connection = None

    async def _reconnect_loop(self) -> None:
        """
        Attempt to reconnect with exponential backoff.
        """
        self._is_reconnecting = True

        while not self._shutdown_requested:
            self._reconnect_attempt += 1

            # Check max attempts (0 = unlimited)
            if (
                self._reconnect_max_attempts > 0
                and self._reconnect_attempt > self._reconnect_max_attempts
            ):
                self.logger.error(
                    "Max reconnection attempts (%d) reached. Giving up.",
                    self._reconnect_max_attempts,
                )
                self._is_reconnecting = False
                return

            # Calculate backoff delay with exponential increase
            delay = min(
                self._reconnect_backoff_base * (2 ** (self._reconnect_attempt - 1)),
                self._reconnect_backoff_max,
            )

            self.logger.info(
                "Reconnection attempt %d/%s in %.1f seconds...",
                self._reconnect_attempt,
                self._reconnect_max_attempts
                if self._reconnect_max_attempts > 0
                else "∞",
                delay,
            )

            await asyncio.sleep(delay)

            if self._shutdown_requested:
                break

            try:
                await self._connect()
                self.logger.info("Reconnected successfully!")

                # Re-subscribe to all topics
                await self._resubscribe_topics()

                # Notify reconnect callback
                if self._on_reconnect:
                    try:
                        await self._on_reconnect()
                    except Exception as cb_error:
                        self.logger.error(
                            "Error in on_reconnect callback: %s", cb_error
                        )

                self._is_reconnecting = False
                return

            except Exception as e:
                self.logger.warning(
                    "Reconnection attempt %d failed: %s", self._reconnect_attempt, e
                )

        self._is_reconnecting = False

    async def _resubscribe_topics(self) -> None:
        """
        Re-subscribe to all topics after reconnection.
        """
        topics_to_resubscribe = list(self._subscription_callbacks.keys())

        if not topics_to_resubscribe:
            return

        self.logger.info("Re-subscribing to %d topic(s)...", len(topics_to_resubscribe))

        subscriptions_snapshot = {
            topic: (
                self._subscription_callbacks.get(topic),
                self._subscription_event_handlers.get(topic, {}).copy(),
            )
            for topic in topics_to_resubscribe
        }

        for topic, (callback, event_handlers) in subscriptions_snapshot.items():
            try:
                if topic in self._topic_subscriptions:
                    self._unregister_topic(topic)

                # Re-subscribe
                await self.subscribe_to_topic(topic, callback)

                # Restore event handlers
                for event, handler in event_handlers.items():
                    self.add_event_handler(topic, event, handler)

                self.logger.info("Re-subscribed to topic: %s", topic)

            except Exception as e:
                self.logger.error("Failed to re-subscribe to topic %s: %s", topic, e)

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        await self.shutdown("Client context exiting")

    async def shutdown(
        self,
        reason: str,
    ) -> None:
        """
        Gracefully shutdown the client connection.

        This method will:
        1. Stop reconnection attempts (if any)
        2. Unsubscribe from all topics (with 5 second timeout)
        3. Cancel the message routing task
        4. Close the WebSocket connection

        Args:
            reason: Human-readable reason for shutdown (for logging)

        Note: This method is automatically called by __aexit__ when using
        the async context manager. You can also call it explicitly.
        """
        self.logger.info("Shutting down client: %s", reason)

        # Signal that shutdown is requested (prevents reconnection)
        self._shutdown_requested = True

        # Cancel reconnection task if running
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        # Clear stored subscription info (won't need it after shutdown)
        self._subscription_callbacks.clear()
        self._subscription_event_handlers.clear()

        topics_to_unsubscribe = list(self._topic_subscriptions.keys())
        if topics_to_unsubscribe:
            self.logger.info(
                "Unsubscribing from %d topic(s)", len(topics_to_unsubscribe)
            )
            unsubscribe_tasks = [
                self.unsubscribe_from_topic(topic) for topic in topics_to_unsubscribe
            ]

            async def gather_unsubscribes() -> list[BaseException | None]:
                return await asyncio.gather(*unsubscribe_tasks, return_exceptions=True)

            try:
                results = await asyncio.wait_for(gather_unsubscribes(), timeout=5.0)

                for topic, result in zip(topics_to_unsubscribe, results):
                    if isinstance(result, Exception):
                        self.logger.warning(
                            "Failed to unsubscribe from topic %s: %s", topic, result
                        )
                        self._unregister_topic(topic)
            except asyncio.TimeoutError:
                self.logger.warning("Unsubscribe timed out after 5s, forcing cleanup")
                for topic in topics_to_unsubscribe:
                    self._unregister_topic(topic)

        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
            self._pending_heartbeat_ref = None

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

    async def _heartbeat_loop(self) -> None:
        """
        Send periodic heartbeat messages to keep the connection alive.

        Phoenix servers expect heartbeat messages on the "phoenix" topic at regular
        intervals (default 30 seconds). The Phoenix server's default timeout is
        typically configured to 60 seconds (2x the heartbeat interval), after which
        it will close connections that haven't sent a heartbeat.

        This loop sends heartbeat messages and tracks pending heartbeat refs.
        Heartbeat responses are handled in _handle_heartbeat_response().

        Note: This task is only created when heartbeat_interval_secs is not None,
        so no None check is needed here.
        """
        # Assert for type checker - this task is only created when interval is not None
        assert self._heartbeat_interval_secs is not None
        interval = self._heartbeat_interval_secs

        while True:
            try:
                await asyncio.sleep(interval)

                if self.connection is None:
                    self.logger.debug("Heartbeat loop stopping: no connection")
                    break

                # Don't send a new heartbeat if we're still waiting for a response.
                # This matches the Phoenix JS client behavior.
                # Note: After clearing the pending ref, if a late response arrives for
                # the old heartbeat, it won't be recognized (the ref won't match).
                # This is intentional and matches JS client behavior - we don't track
                # multiple outstanding heartbeats.
                if self._pending_heartbeat_ref is not None:
                    self.logger.warning(
                        "Heartbeat timeout: no response to heartbeat ref=%s",
                        self._pending_heartbeat_ref,
                    )
                    # Clear the pending ref and continue - the connection might
                    # still be alive, let the next heartbeat try again
                    self._pending_heartbeat_ref = None

                # Generate ref and send heartbeat
                self._pending_heartbeat_ref = self._generate_ref()
                heartbeat_message = make_message(
                    event=Event("heartbeat"),
                    topic="phoenix",
                    ref=self._pending_heartbeat_ref,
                    payload={},
                )

                await self._protocol_handler.send_message(
                    self.connection, heartbeat_message
                )
                self.logger.debug("Sent heartbeat ref=%s", self._pending_heartbeat_ref)

            except asyncio.CancelledError:
                self.logger.debug("Heartbeat loop cancelled")
                raise
            except Exception as e:
                self.logger.warning("Heartbeat failed: %s", e)
                # Connection might be dead, exit the loop
                break

    def _handle_heartbeat_response(self, message: ChannelMessage) -> bool:
        """
        Handle a heartbeat response from the server.

        This method is intentionally synchronous because it only performs simple
        attribute access and comparison - no I/O or blocking operations. Keeping
        it sync avoids unnecessary async overhead for this hot path.

        Args:
            message: The received message

        Returns:
            True if this was a heartbeat response and was handled,
            False if this message is not a heartbeat response.
        """
        # Check if this is a response to our heartbeat
        if (
            message.topic == "phoenix"
            and message.ref is not None
            and message.ref == self._pending_heartbeat_ref
        ):
            self._pending_heartbeat_ref = None
            self.logger.debug("Heartbeat acknowledged ref=%s", message.ref)
            return True
        return False

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
            self.logger.error("Error in topic processor for %s: %s", topic.name, e)
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
            self.logger.info("Subscribed to topic: %s", topic.name)
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
                "Failed to subscribe to topic %s: %s", topic.name, error_message
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
                    "No handler for event %s on topic %s", message.event, topic.name
                )

        except Exception as e:
            self.logger.error("Error in topic callback for %s: %s", topic.name, e)
        finally:
            topic.current_callback_task = None

    async def _handle_leave_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        if not isinstance(message, PHXEventMessage) or message.event != PHXEvent.reply:
            return

        is_leave_success = message.payload.get("status") == "ok"

        if is_leave_success:
            self.logger.info("Unsubscribed from topic: %s", topic.name)

            if topic.current_callback_task and not topic.current_callback_task.done():
                try:
                    await topic.current_callback_task
                except Exception as e:
                    self.logger.error(
                        "Error waiting for callback to finish for %s: %s", topic.name, e
                    )

            if topic.unsubscribe_completed and not topic.unsubscribe_completed.done():
                topic.unsubscribe_completed.set_result(None)

        else:
            self.logger.error(
                "Failed to unsubscribe from topic %s: %s", topic.name, message.payload
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
        async_callback: Callable[[ChannelMessage], Awaitable[None]] | None = None,
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
        if self.connection is None:
            raise PHXConnectionError("Not connected to server")
        await self._protocol_handler.send_message(self.connection, topic_join_message)

        try:
            await subscription_ready_future
            # Store callback for reconnection
            self._subscription_callbacks[topic] = async_callback
            if topic not in self._subscription_event_handlers:
                self._subscription_event_handlers[topic] = {}
        except Exception as e:
            self.logger.error("Failed to subscribe to %s: %s", topic, e)
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
        if self.connection is None:
            raise PHXConnectionError("Not connected to server")
        await self._protocol_handler.send_message(self.connection, topic_leave_message)

        topic_subscription.leave_requested.set()

        try:
            await unsubscribe_completed_future
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error("Error unsubscribing from %s: %s", topic, e)
            raise
        finally:
            self._unregister_topic(topic)
            # Remove stored subscription info
            self._subscription_callbacks.pop(topic, None)
            self._subscription_event_handlers.pop(topic, None)

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

        # Store for reconnection
        if topic not in self._subscription_event_handlers:
            self._subscription_event_handlers[topic] = {}
        self._subscription_event_handlers[topic][event] = handler

    def remove_event_handler(self, topic: str, event: ChannelEvent) -> None:
        """Remove an event handler for a specific event type on a topic."""
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.remove_event_handler(event)

        # Remove from stored handlers
        if topic in self._subscription_event_handlers:
            self._subscription_event_handlers[topic].pop(event, None)

    def get_event_handler(
        self, topic: str, event: ChannelEvent
    ) -> Callable[[Dict[str, Any]], Awaitable[None]] | None:
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
    ) -> Callable[[ChannelMessage], Awaitable[None]] | None:
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
        if self.connection is None:
            raise PHXConnectionError("Not connected to server")
        await self._protocol_handler.process_websocket_messages(
            self.connection,
            self._topic_subscriptions,
            on_unhandled_message=self._handle_heartbeat_response,
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
