from __future__ import annotations

import asyncio
import logging
from asyncio import Queue
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Protocol, cast

from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import (
    ChannelEvent,
    ChannelMessage,
    PHXEvent,
)
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.topic_subscription import (
    TopicProcessingState,
    TopicSubscription,
)
from phoenix_channels_python_client.utils import cancel_and_wait, make_message


class _TopicRuntimeDeps(Protocol):
    async def _invoke_callback_safely(
        self, label: str, callback: Callable[..., Awaitable[None]], *args: object
    ) -> None: ...
    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None: ...
    def _channel_rejoin_delay(self, attempt: int) -> float: ...


async def _cancel_unless_current(task: asyncio.Task[None] | None) -> None:
    # A task can't wait for its own cancellation.
    if task and not task.done() and asyncio.current_task() is not task:
        await cancel_and_wait(task)


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
    _on_topic_lost: Callable[[str, Exception], Awaitable[None]] | None
    _lifecycle_tasks: set[asyncio.Task[None]]

    @property
    def _runtime_deps(self) -> _TopicRuntimeDeps:
        return cast(_TopicRuntimeDeps, self)

    def _set_subscription_ready(self, topic_subscription: TopicSubscription) -> None:
        if not topic_subscription.current_join_ready.done():
            topic_subscription.current_join_ready.set_result(None)

    def _is_current_join_ready(self, topic: TopicSubscription) -> bool:
        if not topic.current_join_ready.done():
            return False

        if topic.current_join_ready.cancelled():
            return False

        return topic.current_join_ready.exception() is None

    def _determine_processing_state(
        self, topic: TopicSubscription
    ) -> TopicProcessingState:
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

                if (
                    self._protocol_handler.protocol_version
                    == PhoenixChannelsProtocolVersion.V2
                    and message.join_ref != topic.join_ref
                ):
                    self.logger.debug(
                        "Dropping stale queued message for topic %s. got=%s "
                        "expected=%s",
                        topic.name,
                        message.join_ref,
                        topic.join_ref,
                    )
                    continue

                current_state = self._determine_processing_state(topic)
                self.logger.debug(
                    "Processing message for topic %s in state %s: %s",
                    topic.name,
                    current_state,
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

                await self._dispatch_normal_message(topic, message)

        except asyncio.CancelledError:
            self.logger.debug("Topic message processor cancelled for %s", topic.name)
            raise
        except Exception as exc:
            self.logger.exception("Error in topic message processor for %s", topic.name)
            await self._unregister_topic(topic.name, error=exc)

    async def _handle_join_response_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        self.logger.debug(
            "Handling join response for topic %s: %s", topic.name, message
        )

        if message.event != PHXEvent.reply:
            self.logger.debug(
                "Ignoring non-reply message while waiting for join on %s", topic.name
            )
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
        self._set_future_exception(topic.current_join_ready, error)
        self.logger.error(
            "Failed to subscribe to topic %s: %s", topic.name, error_message
        )

    async def _dispatch_normal_message(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        match message.event:
            case PHXEvent.error:
                self._mark_channel_errored(topic)
                self._start_channel_recovery(topic)
            case _:
                await self._handle_normal_message_mode(topic, message)

    def _mark_channel_errored(self, topic: TopicSubscription) -> None:
        # Waiting for a join again, the topic ignores repeated errors and runs
        # no callback until the rejoin succeeds.
        topic.current_join_ready = asyncio.get_running_loop().create_future()
        self._set_future_exception(
            topic.current_join_ready,
            PHXTopicError(f"Channel for topic {topic.name} crashed"),
        )

    def _is_on_current_connection(self, topic: TopicSubscription) -> bool:
        # The state stays CONNECTED while _cleanup_connection runs, but it
        # clears the connection at once.
        return (
            self._state == ClientState.CONNECTED
            and self.connection is not None
            and topic.conn_generation == self._conn_generation
        )

    def _is_recovering(self, topic: TopicSubscription) -> bool:
        return topic.recovery_task is not None and not topic.recovery_task.done()

    def _track_lifecycle_task(
        self, work: Coroutine[Any, Any, None]
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(work)
        self._lifecycle_tasks.add(task)
        task.add_done_callback(self._lifecycle_tasks.discard)
        return task

    def _start_channel_recovery(self, topic: TopicSubscription) -> None:
        # Off the current connection, the socket rejoin owns the topic.
        if not self._is_on_current_connection(topic) or self._is_recovering(topic):
            return
        self.logger.info("Recovering channel for topic %s", topic.name)
        topic.recovery_task = self._track_lifecycle_task(self._recover_channel(topic))

    def _start_topic_loss(self, topic: TopicSubscription, error: Exception) -> None:
        if topic.recovery_task is not None:
            topic.recovery_task.cancel()
        self._track_lifecycle_task(self._lose_topic(topic, error))

    def _can_recover(self, topic: TopicSubscription) -> bool:
        return (
            not self._shutdown_event.is_set()
            and self._should_rejoin(topic)
            and self._is_on_current_connection(topic)
        )

    async def _recover_channel(self, topic: TopicSubscription) -> None:
        attempt = 0
        # Socket cleanup fails the pending join and cancels this task in one
        # tick; on Python 3.11 the join's failure can arrive instead of the
        # cancel, so both checks must end the loop.
        while self._can_recover(topic):
            await self._runtime_deps._wait_for_shutdown_or_timeout(
                self._runtime_deps._channel_rejoin_delay(attempt)
            )
            if not self._can_recover(topic):
                return
            attempt += 1
            try:
                await self._rejoin_topic(topic, self._conn_generation)
            except PHXTopicError as exc:
                await self._lose_topic(topic, self._rejoin_rejected(topic, exc))
                return
            except (TimeoutError, PHXConnectionError) as exc:
                self.logger.warning(
                    "Rejoin attempt %s for topic %s failed: %s: %s",
                    attempt,
                    topic.name,
                    type(exc).__name__,
                    exc,
                )
                continue
            if self._should_rejoin(topic):
                self.logger.info(
                    "Recovered channel for topic %s after %s attempt(s)",
                    topic.name,
                    attempt,
                )
            return

    def _rejoin_rejected(
        self, topic: TopicSubscription, error: PHXTopicError
    ) -> PHXTopicError:
        return PHXTopicError(f"Failed to rejoin topic {topic.name}: {error}")

    async def _lose_topic(self, topic: TopicSubscription, error: Exception) -> None:
        # A topic already unsubscribed or replaced is not ours to report.
        if self._topic_subscriptions.get(topic.name) is not topic:
            return
        await self._unregister_topic(topic.name, error=error)
        self.logger.warning("Lost topic %s: %s", topic.name, error)
        if self._on_topic_lost is not None:
            await self._runtime_deps._invoke_callback_safely(
                "on_topic_lost", self._on_topic_lost, topic.name, error
            )

    def _snapshot_handlers(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> tuple[
        Callable[[ChannelMessage], Awaitable[None]] | None,
        Callable[[dict[str, Any]], Awaitable[None]] | None,
    ]:
        message_handler = topic.async_callback
        event_handler = topic.get_event_handler(message.event)
        return message_handler, event_handler

    async def _handle_normal_message_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        self.logger.debug(
            "Processing normal message for topic %s: %s", topic.name, message
        )

        message_handler, event_handler = self._snapshot_handlers(topic, message)

        try:
            has_message_handler = message_handler is not None
            has_specific_handler = event_handler is not None

            if has_message_handler:
                topic.current_callback_task = asyncio.ensure_future(
                    message_handler(message)
                )
                await topic.current_callback_task

            if has_specific_handler:
                topic.current_callback_task = asyncio.ensure_future(
                    event_handler(message.payload)
                )
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

    async def _handle_leave_mode(
        self, topic: TopicSubscription, message: ChannelMessage
    ) -> None:
        self.logger.debug(
            "Processing message during leave for topic %s: %s", topic.name, message
        )

        if message.event != PHXEvent.reply:
            self.logger.debug(
                "Ignoring queued message for leaving topic %s", topic.name
            )
            return

        is_leave_success = message.payload.get("status") == "ok"

        if is_leave_success:
            self.logger.info("Successfully unsubscribed from topic %s", topic.name)
            self._set_unsubscribe_completed(topic)
            return

        error = PHXTopicError(f"Failed to unsubscribe: {message.payload}")
        self.logger.error(
            "Failed to unsubscribe from topic %s: %s", topic.name, message.payload
        )
        if not topic.unsubscribe_completed.done():
            topic.unsubscribe_completed.set_exception(error)

    def _set_unsubscribe_completed(self, topic: TopicSubscription) -> None:
        if not topic.unsubscribe_completed.done():
            topic.unsubscribe_completed.set_result(None)

    def _complete_pending_futures(
        self,
        topic_subscription: TopicSubscription,
        error: Exception,
    ) -> None:
        self._set_future_exception(topic_subscription.current_join_ready, error)

        if topic_subscription.leave_requested.is_set():
            self._set_future_exception(topic_subscription.unsubscribe_completed, error)

    def _fail_pending_joins(self, error: Exception) -> None:
        for topic_subscription in self._topic_subscriptions.values():
            self._set_future_exception(topic_subscription.current_join_ready, error)

    def _set_future_exception(
        self,
        future: asyncio.Future[None],
        error: Exception,
    ) -> None:
        if future.done():
            return
        future.set_exception(error)
        future.add_done_callback(lambda done_future: done_future.exception())

    async def _unregister_topic(
        self, topic_name: str, error: Exception | None = None
    ) -> None:
        async with self._topics_lock:
            topic_subscription = self._topic_subscriptions.pop(topic_name, None)

        if topic_subscription is None:
            self.logger.debug("Topic %s not found in _topic_subscriptions", topic_name)
            return

        unregister_error = error or PHXConnectionError(
            f"Topic {topic_name} was unregistered"
        )
        self._complete_pending_futures(topic_subscription, unregister_error)

        # First, so the recovery can't restart the topic task stopped next.
        await _cancel_unless_current(topic_subscription.recovery_task)
        await self._stop_topic_task(topic_subscription)
        self.logger.info("Unregistered topic %s", topic_name)

    def get_current_subscriptions(self) -> dict[str, TopicSubscription]:
        return self._topic_subscriptions.copy()

    def is_topic_joined(self, topic: str) -> bool:
        """Whether ``topic``'s channel is joined now, not just subscribed."""
        subscription = self._topic_subscriptions.get(topic)
        return (
            subscription is not None
            and self._is_on_current_connection(subscription)
            and self._is_current_join_ready(subscription)
        )

    def get_protocol_handler(self) -> PHXProtocolHandler:
        return self._protocol_handler

    def _generate_ref(self) -> str:
        self._ref_counter += 1
        return str(self._ref_counter)

    def _ensure_can_send(self, operation: str) -> None:
        if self._state != ClientState.CONNECTED or self.connection is None:
            raise PHXConnectionError(
                f"Cannot {operation} while client is {self._state}. Wait for "
                "reconnection."
            )

    async def _send(
        self, connection: ClientConnection, message: ChannelMessage
    ) -> None:
        """Send ``message``; a socket already closing raises PHXConnectionError."""
        try:
            await self._protocol_handler.send_message(connection, message)
        except ConnectionClosed as exc:
            raise PHXConnectionError(
                f"Connection lost before {message.event} could be sent"
            ) from exc

    async def _join(self, topic_subscription: TopicSubscription) -> None:
        """Send the topic's join on its current join_ref and wait for the reply."""
        connection = self.connection
        if connection is None:
            raise PHXConnectionError("Connection lost before join could be sent")

        join_message = make_message(
            topic=topic_subscription.name,
            event=PHXEvent.join,
            payload={},
            ref=topic_subscription.join_ref,
            join_ref=topic_subscription.join_ref,
        )
        await self._send(connection, join_message)
        await asyncio.wait_for(
            topic_subscription.current_join_ready, timeout=self.join_timeout_s
        )

    async def subscribe_to_topic(
        self,
        topic: str,
        async_callback: Callable[[ChannelMessage], Awaitable[None]] | None = None,
    ) -> None:
        self._ensure_can_send("subscribe")

        topic_queue: Queue[ChannelMessage] = Queue(maxsize=self.max_topic_queue_size)
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

        try:
            await self._join(topic_subscription)
        except TimeoutError as exc:
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

        if not _allow_disconnected:
            self._ensure_can_send("unsubscribe")
        connection = self.connection if self._state == ClientState.CONNECTED else None
        # A crashed channel's leave reply can't be told from its rejoin's, so a
        # leave during recovery completes without one, as Phoenix JS does.
        complete_locally = connection is None or self._is_recovering(topic_subscription)

        topic_subscription.leave_requested.set()

        try:
            if connection is not None:
                leave_ref = self._generate_ref()
                topic_leave_message = make_message(
                    topic=topic,
                    event=PHXEvent.leave,
                    payload={},
                    ref=leave_ref,
                    join_ref=topic_subscription.join_ref,
                )
                await self._send(connection, topic_leave_message)
            if complete_locally:
                self._set_unsubscribe_completed(topic_subscription)

            await asyncio.wait_for(
                topic_subscription.unsubscribe_completed,
                timeout=self.leave_timeout_s,
            )
        except TimeoutError as exc:
            raise PHXTopicError(
                f"Timed out waiting to unsubscribe from {topic}"
            ) from exc
        finally:
            await self._unregister_topic(topic)

    def add_event_handler(
        self,
        topic: str,
        event: ChannelEvent,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.add_event_handler(event, handler)
        self.logger.debug("Added event handler for %s on topic %s", event, topic)

    def remove_event_handler(self, topic: str, event: ChannelEvent) -> None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        topic_subscription.remove_event_handler(event)
        self.logger.debug("Removed event handler for %s on topic %s", event, topic)

    def get_event_handler(
        self, topic: str, event: ChannelEvent
    ) -> Callable[[dict[str, Any]], Awaitable[None]] | None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.get_event_handler(event)

    def has_event_handler(self, topic: str, event: ChannelEvent) -> bool:
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.has_event_handler(event)

    def list_event_handlers(
        self, topic: str
    ) -> dict[ChannelEvent, Callable[[dict[str, Any]], Awaitable[None]]]:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.event_handlers.copy()

    def set_message_handler(
        self, topic: str, handler: Callable[[ChannelMessage], Awaitable[None]]
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

    def get_message_handler(
        self, topic: str
    ) -> Callable[[ChannelMessage], Awaitable[None]] | None:
        if topic not in self._topic_subscriptions:
            raise PHXTopicError(f"Topic {topic} not subscribed")

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback

    def has_message_handler(self, topic: str) -> bool:
        if topic not in self._topic_subscriptions:
            return False

        topic_subscription = self._topic_subscriptions[topic]
        return topic_subscription.async_callback is not None

    async def _drain_callback(self, topic_subscription: TopicSubscription) -> None:
        callback_task = topic_subscription.current_callback_task
        if callback_task is None or callback_task.done():
            return

        # The topic task owns how the callback ends, so don't re-raise it here.
        _, still_running = await asyncio.wait(
            {callback_task}, timeout=self.callback_drain_timeout_s
        )
        if still_running:
            self.logger.warning(
                "Callback for topic %s did not finish before reconnect; cancelling",
                topic_subscription.name,
            )
            await cancel_and_wait(callback_task)

    async def _rejoin_topics(self, generation: int) -> None:
        async with self._topics_lock:
            subscriptions = list(self._topic_subscriptions.values())

        for topic_subscription in subscriptions:
            try:
                await self._rejoin_topic(topic_subscription, generation)
            except Exception as exc:  # noqa: BLE001  # handled per topic
                self._handle_rejoin_failure(topic_subscription, exc)

    async def _rejoin_topic(
        self, topic_subscription: TopicSubscription, generation: int
    ) -> None:
        if not self._should_rejoin(topic_subscription):
            return

        await self._drain_callback(topic_subscription)
        await self._stop_topic_task(topic_subscription)
        # The topic may be left or unregistered during any await before the restart.
        if not self._should_rejoin(topic_subscription):
            return

        self._restart_topic(topic_subscription, generation)
        await self._join(topic_subscription)

    def _should_rejoin(self, topic_subscription: TopicSubscription) -> bool:
        is_registered = (
            self._topic_subscriptions.get(topic_subscription.name) is topic_subscription
        )
        return is_registered and not topic_subscription.leave_requested.is_set()

    async def _stop_topic_task(self, topic_subscription: TopicSubscription) -> None:
        await _cancel_unless_current(topic_subscription.process_topic_messages_task)

    def _restart_topic(
        self, topic_subscription: TopicSubscription, generation: int
    ) -> None:
        topic_subscription.conn_generation = generation
        topic_subscription.join_ref = self._generate_ref()
        topic_subscription.current_join_ready = (
            asyncio.get_running_loop().create_future()
        )
        self._drain_topic_queue(topic_subscription)
        topic_subscription.process_topic_messages_task = asyncio.create_task(
            self._process_topic_messages(topic_subscription.name)
        )

    def _handle_rejoin_failure(self, topic: TopicSubscription, exc: Exception) -> None:
        if self._shutdown_event.is_set() or self._state == ClientState.SHUTTING_DOWN:
            self.logger.debug(
                "Ignoring rejoin failure for topic %s during shutdown: %s",
                topic.name,
                exc,
            )
            return

        if isinstance(exc, PHXTopicError):
            # A task of its own, so a slow on_topic_lost can't hold up the
            # rejoin of the other topics or on_reconnect.
            self._start_topic_loss(topic, self._rejoin_rejected(topic, exc))
            return

        self.logger.warning(
            "Transient rejoin failure for topic %s, will retry on next reconnect: %s",
            topic.name,
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
