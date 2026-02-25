from __future__ import annotations

import asyncio
import logging
from asyncio import Queue
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from websockets import ClientConnection

from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import Event, Message, PHXEvent
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.topic_subscription import (
    TopicProcessingState,
    TopicSubscription,
)


class TopicRuntimeMixin:
    logger: logging.Logger
    connection: ClientConnection | None
    _state: ClientState
    _ref_counter: int
    _conn_generation: int
    _topic_subscriptions: dict[str, TopicSubscription]
    _protocol_handler: PHXProtocolHandler
    _topics_lock: asyncio.Lock
    _shutdown_event: asyncio.Event
    join_timeout_s: float
    leave_timeout_s: float
    max_topic_queue_size: int
    callback_drain_timeout_s: float

    def _set_subscription_ready(self, topic_subscription: TopicSubscription) -> None:
        if not topic_subscription.current_join_ready.done():
            topic_subscription.current_join_ready.set_result(None)

        if not topic_subscription.subscription_ready.done():
            topic_subscription.subscription_ready.set_result(None)

    def _set_subscription_error(
        self, topic_subscription: TopicSubscription, error: Exception
    ) -> None:
        if not topic_subscription.current_join_ready.done():
            topic_subscription.current_join_ready.set_exception(error)

        if not topic_subscription.subscription_ready.done():
            topic_subscription.subscription_ready.set_exception(error)

    def _is_current_join_ready(self, topic: TopicSubscription) -> bool:
        if not topic.current_join_ready.done():
            return False

        if topic.current_join_ready.cancelled():
            return False

        return topic.current_join_ready.exception() is None

    def _determine_processing_state(self, topic: TopicSubscription) -> TopicProcessingState:
        current_join_ready = self._is_current_join_ready(topic)
        leave_requested = topic.leave_requested.is_set()

        if not current_join_ready:
            return TopicProcessingState.WAITING_FOR_JOIN
        if leave_requested:
            return TopicProcessingState.PROCESSING_LEAVE
        return TopicProcessingState.NORMAL_PROCESSING

    async def _process_topic_messages(self, topic_name: str) -> None:
        topic = self._topic_subscriptions.get(topic_name)
        if topic is None:
            return

        self.logger.debug("Starting topic message processor for %s", topic.name)

        try:
            while True:
                message = await topic.queue.get()

                if message.join_ref != topic.join_ref:
                    self.logger.debug(
                        "Dropping stale queued message for topic %s. got=%s expected=%s",
                        topic.name,
                        message.join_ref,
                        topic.join_ref,
                    )
                    continue

                current_state = self._determine_processing_state(topic)
                self.logger.debug(
                    "Processing message for topic %s in state %s: %s",
                    topic.name,
                    current_state.value,
                    message,
                )

                if current_state == TopicProcessingState.WAITING_FOR_JOIN:
                    await self._handle_join_response_mode(topic, message)
                    continue

                if current_state == TopicProcessingState.PROCESSING_LEAVE:
                    await self._handle_leave_mode(topic, message)
                    if topic.unsubscribe_completed.done():
                        break
                    continue

                await self._handle_normal_message_mode(topic, message)

        except asyncio.CancelledError:
            self.logger.debug("Topic message processor cancelled for %s", topic.name)
            raise
        except Exception as exc:
            self.logger.exception("Error in topic message processor for %s", topic.name)
            await self._unregister_topic(topic.name, error=exc)

    async def _handle_join_response_mode(self, topic: TopicSubscription, message: Message) -> None:
        self.logger.debug("Handling join response for topic %s: %s", topic.name, message)

        if message.event != PHXEvent.reply:
            self.logger.debug("Ignoring non-reply message while waiting for join on %s", topic.name)
            return

        if message.payload.get("status") == "ok":
            self._set_subscription_ready(topic)
            self.logger.info("Successfully subscribed to topic %s", topic.name)
            return

        response = message.payload.get("response", {})
        error_message = (
            response.get("reason", "invalid topic")
            if isinstance(response, dict)
            else "invalid topic"
        )

        error = PHXTopicError(error_message)
        self._set_subscription_error(topic, error)
        self.logger.error("Failed to subscribe to topic %s: %s", topic.name, error_message)

    def _capture_handlers_atomically(self, topic: TopicSubscription, message: Message) -> tuple:
        message_handler = topic.async_callback
        event_handler = topic.get_event_handler(message.event)
        return message_handler, event_handler

    async def _handle_normal_message_mode(self, topic: TopicSubscription, message: Message) -> None:
        self.logger.debug("Processing normal message for topic %s: %s", topic.name, message)

        message_handler, event_handler = self._capture_handlers_atomically(topic, message)

        try:
            has_message_handler = message_handler is not None
            has_specific_handler = event_handler is not None

            if has_message_handler:
                topic.current_callback_task = asyncio.create_task(message_handler(message))
                await topic.current_callback_task

            if has_specific_handler:
                topic.current_callback_task = asyncio.create_task(event_handler(message.payload))
                await topic.current_callback_task

            if not has_message_handler and not has_specific_handler:
                self.logger.warning(
                    "No handler found for event %s on topic %s",
                    message.event,
                    topic.name,
                )

        except Exception:
            self.logger.exception("Error in topic callback for %s", topic.name)
        finally:
            topic.current_callback_task = None

    async def _handle_leave_mode(self, topic: TopicSubscription, message: Message) -> None:
        self.logger.debug("Processing message during leave for topic %s: %s", topic.name, message)

        if message.event != PHXEvent.reply:
            self.logger.debug("Ignoring queued message for leaving topic %s", topic.name)
            return

        is_leave_success = message.payload.get("status") == "ok"

        if is_leave_success:
            self.logger.info("Successfully unsubscribed from topic %s", topic.name)
            if not topic.unsubscribe_completed.done():
                topic.unsubscribe_completed.set_result(None)
            return

        error = PHXTopicError(f"Failed to unsubscribe: {message.payload}")
        self.logger.error("Failed to unsubscribe from topic %s: %s", topic.name, message.payload)
        if not topic.unsubscribe_completed.done():
            topic.unsubscribe_completed.set_exception(error)

    def _complete_pending_futures(
        self,
        topic_subscription: TopicSubscription,
        error: Exception,
    ) -> None:
        self._set_future_exception(topic_subscription.current_join_ready, error)
        self._set_future_exception(topic_subscription.subscription_ready, error)

        if topic_subscription.leave_requested.is_set():
            self._set_future_exception(topic_subscription.unsubscribe_completed, error)

    def _set_future_exception(
        self,
        future: asyncio.Future[None],
        error: Exception,
    ) -> None:
        if future.done():
            return
        future.set_exception(error)
        future.add_done_callback(lambda done_future: done_future.exception())

    async def _unregister_topic(self, topic_name: str, error: Exception | None = None) -> None:
        async with self._topics_lock:
            topic_subscription = self._topic_subscriptions.pop(topic_name, None)

        if topic_subscription is None:
            self.logger.debug("Topic %s not found in _topic_subscriptions", topic_name)
            return

        unregister_error = error or PHXConnectionError(f"Topic {topic_name} was unregistered")
        self._complete_pending_futures(topic_subscription, unregister_error)

        task = topic_subscription.process_topic_messages_task
        if task and not task.done() and asyncio.current_task() is not task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self.logger.info("Unregistered topic %s", topic_name)

    def get_current_subscriptions(self) -> dict[str, TopicSubscription]:
        return self._topic_subscriptions.copy()

    def get_protocol_handler(self) -> PHXProtocolHandler:
        return self._protocol_handler

    def _generate_ref(self) -> str:
        self._ref_counter += 1
        return str(self._ref_counter)

    def _ensure_can_send(self, operation: str) -> None:
        if self._state != ClientState.CONNECTED or self.connection is None:
            raise PHXConnectionError(
                f"Cannot {operation} while client is {self._state.value}. Wait for reconnection."
            )

    async def subscribe_to_topic(
        self,
        topic: str,
        async_callback: Callable[[Message], Awaitable[None]] | None = None,
    ) -> None:
        self._ensure_can_send("subscribe")

        topic_queue: Queue[Message] = Queue(maxsize=self.max_topic_queue_size)
        join_ref = self._generate_ref()

        topic_subscription = TopicSubscription(
            name=topic,
            async_callback=async_callback,
            queue=topic_queue,
            join_ref=join_ref,
            process_topic_messages_task=None,
            conn_generation=self._conn_generation,
        )

        async with self._topics_lock:
            if topic in self._topic_subscriptions:
                raise PHXTopicError(f"Topic {topic} already subscribed")
            self._topic_subscriptions[topic] = topic_subscription

        topic_subscription.process_topic_messages_task = asyncio.create_task(
            self._process_topic_messages(topic)
        )

        topic_join_message = Message(
            topic=topic,
            event=PHXEvent.join,
            payload={},
            ref=join_ref,
            join_ref=join_ref,
        )

        try:
            assert self.connection is not None
            await self._protocol_handler.send_message(self.connection, topic_join_message)
            await asyncio.wait_for(
                topic_subscription.current_join_ready, timeout=self.join_timeout_s
            )
        except asyncio.TimeoutError as exc:
            await self._unregister_topic(
                topic,
                error=PHXTopicError(f"Timed out waiting to subscribe to {topic}"),
            )
            raise PHXTopicError(f"Timed out waiting to subscribe to {topic}") from exc
        except Exception:
            await self._unregister_topic(topic)
            raise

    async def unsubscribe_from_topic(
        self, topic: str, *, _allow_disconnected: bool = False
    ) -> None:
        async with self._topics_lock:
            topic_subscription = self._topic_subscriptions.get(topic)

        if topic_subscription is None:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        is_connected = self._state == ClientState.CONNECTED and self.connection is not None
        if not is_connected and not _allow_disconnected:
            self._ensure_can_send("unsubscribe")
            return

        topic_subscription.leave_requested.set()

        try:
            if is_connected and self.connection is not None:
                leave_ref = self._generate_ref()
                topic_leave_message = Message(
                    topic=topic,
                    event=PHXEvent.leave,
                    payload={},
                    ref=leave_ref,
                    join_ref=topic_subscription.join_ref,
                )
                await self._protocol_handler.send_message(self.connection, topic_leave_message)
            elif not _allow_disconnected:
                self._ensure_can_send("unsubscribe")
            else:
                if not topic_subscription.unsubscribe_completed.done():
                    topic_subscription.unsubscribe_completed.set_result(None)

            await asyncio.wait_for(
                topic_subscription.unsubscribe_completed,
                timeout=self.leave_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            raise PHXTopicError(f"Timed out waiting to unsubscribe from {topic}") from exc
        finally:
            await self._unregister_topic(topic)

    def add_event_handler(
        self, topic: str, event: Event, handler: Callable[[dict[str, Any]], Awaitable[None]]
    ) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.add_event_handler(event, handler)
        self.logger.debug("Added event handler for %s on topic %s", event, topic)

    def remove_event_handler(self, topic: str, event: Event) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.remove_event_handler(event)
        self.logger.debug("Removed event handler for %s on topic %s", event, topic)

    def get_event_handler(
        self, topic: str, event: Event
    ) -> Callable[[dict[str, Any]], Awaitable[None]] | None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.get_event_handler(event)

    def has_event_handler(self, topic: str, event: Event) -> bool:
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.has_event_handler(event)

    def list_event_handlers(
        self, topic: str
    ) -> dict[Event, Callable[[dict[str, Any]], Awaitable[None]]]:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.event_handlers.copy()

    def set_message_handler(
        self, topic: str, handler: Callable[[Message], Awaitable[None]]
    ) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.async_callback = handler
        self.logger.debug("Set message handler for topic %s", topic)

    def remove_message_handler(self, topic: str) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.async_callback = None
        self.logger.debug("Removed message handler for topic %s", topic)

    def get_message_handler(self, topic: str) -> Callable[[Message], Awaitable[None]] | None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback

    def has_message_handler(self, topic: str) -> bool:
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback is not None

    async def _rejoin_topics(self, generation: int) -> None:
        async with self._topics_lock:
            subscriptions = list(self._topic_subscriptions.items())

        loop = asyncio.get_running_loop()

        for topic_name, topic_subscription in subscriptions:
            if topic_subscription.leave_requested.is_set():
                continue

            previous_task = topic_subscription.process_topic_messages_task
            callback_task = topic_subscription.current_callback_task
            if callback_task and not callback_task.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(callback_task),
                        timeout=self.callback_drain_timeout_s,
                    )
                except asyncio.TimeoutError:
                    self.logger.warning(
                        "Callback for topic %s did not finish before reconnect; cancelling",
                        topic_name,
                    )
                    callback_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await callback_task

            if previous_task and not previous_task.done():
                previous_task.cancel()
                with suppress(asyncio.CancelledError):
                    await previous_task

            topic_subscription.conn_generation = generation
            topic_subscription.join_ref = self._generate_ref()
            topic_subscription.current_join_ready = loop.create_future()
            self._drain_topic_queue(topic_subscription)
            topic_subscription.process_topic_messages_task = asyncio.create_task(
                self._process_topic_messages(topic_name)
            )

            join_message = Message(
                topic=topic_subscription.name,
                event=PHXEvent.join,
                payload={},
                ref=topic_subscription.join_ref,
                join_ref=topic_subscription.join_ref,
            )

            try:
                if self.connection is None:
                    raise PHXConnectionError("Connection unavailable while rejoining topics")

                await self._protocol_handler.send_message(self.connection, join_message)
                await asyncio.wait_for(
                    topic_subscription.current_join_ready,
                    timeout=self.join_timeout_s,
                )
            except Exception as exc:
                if self._shutdown_event.is_set() or self._state == ClientState.SHUTTING_DOWN:
                    self.logger.debug(
                        "Ignoring rejoin failure for topic %s during shutdown: %s",
                        topic_name,
                        exc,
                    )
                    continue
                if isinstance(exc, PHXTopicError):
                    self.logger.error("Failed to rejoin topic %s: %s", topic_name, exc)
                    await self._unregister_topic(
                        topic_name,
                        error=PHXTopicError(f"Failed to rejoin topic {topic_name}: {exc}"),
                    )
                    continue

                self.logger.warning(
                    "Transient rejoin failure for topic %s, will retry on next reconnect: %s",
                    topic_name,
                    exc,
                )

    def _drain_topic_queue(self, topic_subscription: TopicSubscription) -> None:
        dropped = 0
        while True:
            try:
                topic_subscription.queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break

        if dropped:
            self.logger.debug(
                "Drained %s queued messages for topic %s during reconnect",
                dropped,
                topic_subscription.name,
            )
