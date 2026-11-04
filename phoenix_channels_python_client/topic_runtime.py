from __future__ import annotations

import asyncio
import inspect
import logging
import math
from asyncio import Queue
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Protocol, TypeVar, cast

from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.channel import Channel
from phoenix_channels_python_client.client_types import (
    ClientState,
    JoinResponseCallback,
)
from phoenix_channels_python_client.exceptions import (
    ChannelError,
    PushTimeoutError,
    SocketError,
)
from phoenix_channels_python_client.messages import (
    CLIENT_LIFECYCLE_EVENTS,
    HEARTBEAT_EVENT,
    ChannelEvent,
    Message,
    SystemEvent,
)
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.replies import Reply, reply_from_message
from phoenix_channels_python_client.send_session import SessionSends
from phoenix_channels_python_client.topic_subscription import (
    ChannelCause,
    ChannelState,
    JoinParams,
    TopicSubscription,
)
from phoenix_channels_python_client.utils import cancel_and_wait, make_message

T = TypeVar("T")


class TopicRuntimeDeps(Protocol):
    async def _invoke_callback_safely(
        self, label: str, callback: Callable[..., Awaitable[None]], *args: object
    ) -> None: ...
    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None: ...
    def _channel_rejoin_delay(self, attempt: int) -> float: ...
    def _transition_state(self, new_state: ClientState) -> None: ...
    async def _force_close_connection(
        self, connection: ClientConnection, reason: str
    ) -> None: ...


async def _cancel_unless_current(task: asyncio.Task[None] | None) -> None:
    # A task can't wait for its own cancellation.
    if task and not task.done() and asyncio.current_task() is not task:
        await cancel_and_wait(task)


async def _run_handlers(
    message: Message,
    message_handler: Callable[[Message], Awaitable[None]] | None,
    event_handler: Callable[[dict[str, Any]], Awaitable[None]] | None,
) -> None:
    if message_handler is not None:
        await message_handler(message)
    if event_handler is not None:
        await event_handler(message.payload)


def _channel_failure(topic: TopicSubscription, event: ChannelEvent) -> Exception:
    match event:
        case SystemEvent.error:
            return ChannelError(f"Channel for topic {topic.name} crashed")
        case SystemEvent.close:
            return ChannelError(f"Channel for topic {topic.name} closed by the server")
        case _:
            raise ValueError("Expected a channel lifecycle event")


class TopicRuntimeMixin:
    logger: logging.Logger
    connection: ClientConnection | None
    state: ClientState
    ref_counter: int
    conn_generation: int
    topic_subscriptions: dict[str, TopicSubscription]
    protocol_handler: PHXProtocolHandler
    topics_lock: asyncio.Lock
    shutdown_event: asyncio.Event
    join_timeout_s: float
    leave_timeout_s: float
    max_topic_queue_size: int
    callback_drain_timeout_s: float
    on_topic_lost: Callable[[str, Exception], Awaitable[None]] | None
    lifecycle_tasks: set[asyncio.Task[None]]
    cleanup_tasks: set[asyncio.Task[None]]
    send_session: SessionSends | None

    @property
    def runtime_deps(self) -> TopicRuntimeDeps:
        return cast(TopicRuntimeDeps, self)

    def channel(
        self,
        topic: str,
        *,
        params: JoinParams = None,
        on_join_response: JoinResponseCallback | None = None,
        blocks_reconnect: bool = True,
    ) -> Channel:
        subscription = TopicSubscription(
            name=topic,
            async_callback=None,
            queue=Queue(maxsize=self.max_topic_queue_size),
            join_ref="",
            process_topic_messages_task=None,
            params=params,
            on_join_response=on_join_response,
            blocks_reconnect=blocks_reconnect,
        )
        return Channel(self, subscription)

    def _channel_joined(self, topic: TopicSubscription) -> bool:
        return (
            topic.state == ChannelState.JOINED
            and self._is_registered(topic)
            and self._is_on_current_connection(topic)
            and self._is_current_join_ready(topic)
            and not topic.leave_requested.is_set()
        )

    def _transition_channel(
        self,
        topic: TopicSubscription,
        cause: ChannelCause,
        error: Exception | None = None,
    ) -> None:
        topic.cause = cause
        if cause == ChannelCause.ACCEPTED:
            if topic.state == ChannelState.LEFT or topic.leave_requested.is_set():
                return
            topic.state = ChannelState.JOINED
            topic.established = True
            if not topic.current_join_ready.done():
                topic.current_join_ready.set_result(None)
            return
        recoverable = cause in {
            ChannelCause.CRASH,
            ChannelCause.TRANSPORT,
            ChannelCause.TIMEOUT,
        }
        topic.state = (
            ChannelState.REJOINING
            if topic.established
            and recoverable
            and not topic.leave_requested.is_set()
            and not self.shutdown_event.is_set()
            else ChannelState.LEFT
        )
        if topic.state == ChannelState.LEFT:
            topic.leave_requested.set()
        failure = error or ChannelError("Channel is not joined")
        self._fail_current_join(topic, failure)
        for outcome in topic.pending_pushes.values():
            self._set_future_exception(outcome, failure)
        topic.pending_pushes.clear()

    def _set_subscription_ready(self, topic_subscription: TopicSubscription) -> None:
        self._transition_channel(topic_subscription, ChannelCause.ACCEPTED)

    def _is_current_join_ready(self, topic: TopicSubscription) -> bool:
        if not topic.current_join_ready.done():
            return False

        if topic.current_join_ready.cancelled():
            return False

        return topic.current_join_ready.exception() is None

    async def _process_topic_messages(self, topic_name: str) -> None:
        topic = self.topic_subscriptions.get(topic_name)
        if topic is None:
            return

        self.logger.debug("Starting topic message processor for %s", topic.name)

        try:
            while topic.state != ChannelState.LEFT:
                message = await topic.queue.get()
                if topic.restart_requested:
                    self.logger.debug(
                        "Topic processor for %s stopping for a rejoin; dropping %s",
                        topic.name,
                        message.event,
                    )
                    return

                if self.protocol_handler.is_stale_join(message, topic.join_ref):
                    self.logger.debug(
                        "Dropping stale queued message for topic %s. got=%s "
                        "expected=%s",
                        topic.name,
                        message.join_ref,
                        topic.join_ref,
                    )
                    continue

                if self._channel_joined(topic):
                    await self._handle_normal_message_mode(topic, message)

        except asyncio.CancelledError:
            self.logger.debug("Topic message processor cancelled for %s", topic.name)
            raise
        except Exception as exc:
            self.logger.exception("Error in topic message processor for %s", topic.name)
            # Same loss as a server close. Awaiting it here would run
            # on_topic_lost on this processor, which shutdown does not cancel.
            self._start_topic_loss(topic, exc)

    def _handle_channel_control(
        self, topic: TopicSubscription, message: Message
    ) -> bool:
        if not self._is_registered(topic) or not self._is_on_current_connection(topic):
            return True
        if message.event in CLIENT_LIFECYCLE_EVENTS:
            self._handle_channel_lifecycle(topic, message)
            return True
        if message.event != SystemEvent.reply:
            return topic.state != ChannelState.JOINED
        if message.ref == topic.leave_ref and topic.leave_ref is not None:
            self._handle_leave_response(topic, message)
            return True
        if message.ref == topic.join_ref:
            self._handle_join_response_mode(topic, message)
            return True
        return (
            self._resolve_push_reply(topic, message)
            or topic.state != ChannelState.JOINED
        )

    def _resolve_push_reply(self, topic: TopicSubscription, message: Message) -> bool:
        outcome = topic.pending_pushes.pop(message.ref, None) if message.ref else None
        if outcome is None:
            return False
        if not outcome.done():
            try:
                outcome.set_result(reply_from_message(message))
            except (TypeError, ValueError) as exc:
                error = ChannelError("Malformed push reply")
                error.__cause__ = exc
                self._set_future_exception(outcome, error)
        return True

    def _handle_channel_lifecycle(
        self, topic: TopicSubscription, message: Message
    ) -> None:
        if message.ref is not None and message.ref != topic.join_ref:
            return
        topic.terminal_received.set()
        if topic.state == ChannelState.LEFT:
            self._release_reservation(topic)
            return
        error = _channel_failure(topic, message.event)
        cause = (
            ChannelCause.CRASH
            if message.event == SystemEvent.error
            else ChannelCause.CLOSE
        )
        self._transition_channel(topic, cause, error)
        if topic.established:
            if cause == ChannelCause.CRASH:
                self._start_channel_recovery(topic)
            else:
                self._start_topic_loss(topic, error)
        return

    def _handle_join_response_mode(
        self, topic: TopicSubscription, message: Message
    ) -> None:
        self.logger.debug(
            "Handling join response for topic %s: %s", topic.name, message
        )

        if message.event in CLIENT_LIFECYCLE_EVENTS:
            self._handle_channel_lifecycle(topic, message)
            return

        if (
            message.event != SystemEvent.reply
            or message.ref != topic.join_ref
            or not self._is_registered(topic)
            or not self._is_on_current_connection(topic)
        ):
            return

        if (
            topic.current_join_ready.done()
            or topic.leave_requested.is_set()
            or self.shutdown_event.is_set()
        ):
            if (
                topic.state == ChannelState.LEFT
                and isinstance(message.payload, dict)
                and message.payload.get("status") == "error"
            ):
                topic.terminal_received.set()
                self._release_reservation(topic)
            return

        if not isinstance(message.payload, dict):
            error = ChannelError("Join acknowledgement must contain a JSON object")
            self._transition_channel(topic, ChannelCause.REJECTED, error)
            return
        if message.payload.get("status") == "ok":
            try:
                self._install_join_response(topic, message.payload.get("response"))
            except (Exception, asyncio.CancelledError) as exc:
                error = ChannelError("Join response installation failed")
                self.logger.exception(str(error))
                error.__cause__ = exc
                self._transition_channel(topic, ChannelCause.CONFIGURATION, error)
                return
            self._set_subscription_ready(topic)
            self.logger.info("Successfully subscribed to topic %s", topic.name)
            return

        response = message.payload.get("response", {})
        error_message = (
            response.get("reason", "invalid topic")
            if isinstance(response, dict)
            else "invalid topic"
        )

        error = ChannelError(error_message)
        topic.terminal_received.set()
        self._transition_channel(topic, ChannelCause.REJECTED, error)
        self.logger.error(
            "Failed to subscribe to topic %s: %s", topic.name, error_message
        )

    def _install_join_response(
        self, topic: TopicSubscription, response: object
    ) -> None:
        if topic.on_join_response is None:
            return
        result = topic.on_join_response(response)
        if (
            inspect.iscoroutine(result)
            and inspect.getcoroutinestate(result) == inspect.CORO_CREATED
        ):
            result.close()
        if result is not None:
            raise TypeError("on_join_response must return None")

    def _fail_current_join(self, topic: TopicSubscription, error: Exception) -> None:
        # A new future: the successful join is already done, so failing it in
        # place would leave the topic looking joined.
        if not topic.current_join_ready.done():
            self._set_future_exception(topic.current_join_ready, error)
            return
        topic.current_join_ready = asyncio.get_running_loop().create_future()
        self._set_future_exception(topic.current_join_ready, error)

    def _is_connected(self) -> bool:
        # The state stays CONNECTED while _cleanup_connection runs, but it
        # clears the connection at once.
        return self.state == ClientState.CONNECTED and self.connection is not None

    def _is_on_current_connection(self, topic: TopicSubscription) -> bool:
        return (
            self._is_connected()
            and topic.conn_generation == self.conn_generation
            and (topic.connection is None or topic.connection is self.connection)
        )

    def _is_recovering(self, topic: TopicSubscription) -> bool:
        return topic.recovery_task is not None and not topic.recovery_task.done()

    def _channel_recoveries(self) -> list[asyncio.Task[None]]:
        return [
            topic.recovery_task
            for topic in self.topic_subscriptions.values()
            if topic.recovery_task is not None
        ]

    def _track_lifecycle_task(
        self, work: Coroutine[Any, Any, None]
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(work)
        self.lifecycle_tasks.add(task)
        task.add_done_callback(self._finish_lifecycle_task)
        return task

    def _finish_lifecycle_task(self, task: asyncio.Task[None]) -> None:
        self.lifecycle_tasks.discard(task)
        # Nothing awaits these tasks, so an unexpected failure is reported here.
        if not task.cancelled() and (error := task.exception()):
            self.logger.error("Channel lifecycle task failed", exc_info=error)

    def _start_channel_recovery(
        self, topic: TopicSubscription, *, generation: int | None = None
    ) -> None:
        # A socket rejoin supplies its new generation for an immediate attempt.
        recovery_generation = self.conn_generation if generation is None else generation
        if (
            self._is_recovering(topic)
            or not self._can_recover(topic, recovery_generation)
            or (generation is None and topic.conn_generation != recovery_generation)
        ):
            return
        self.logger.info("Recovering channel for topic %s", topic.name)
        topic.recovery_task = self._track_lifecycle_task(
            self._recover_channel(
                topic,
                generation=recovery_generation,
                immediate=generation is not None,
            )
        )

    def _start_topic_loss(self, topic: TopicSubscription, error: Exception) -> None:
        if topic.state != ChannelState.LEFT:
            if not isinstance(error, ChannelError):
                failure = ChannelError("Channel processor failed")
                failure.__cause__ = error
                error = failure
            self._transition_channel(topic, ChannelCause.CLOSE, error)
        if not topic.established:
            self._start_leave_task(topic, asyncio.current_task())
            return
        self._track_lifecycle_task(self._lose_topic(topic, error))

    def _can_recover(self, topic: TopicSubscription, generation: int) -> bool:
        return (
            not self.shutdown_event.is_set()
            and self._should_rejoin(topic)
            and self._is_connected()
            and generation == self.conn_generation
        )

    async def _recover_channel(
        self,
        topic: TopicSubscription,
        *,
        generation: int,
        immediate: bool = False,
    ) -> None:
        attempt = 0
        # Socket cleanup fails the pending join and cancels this task in one
        # tick; on Python 3.11 the join's failure can arrive instead of the
        # cancel, so both checks must end the loop.
        while self._can_recover(topic, generation):
            if immediate:
                immediate = False
            else:
                await self.runtime_deps._wait_for_shutdown_or_timeout(
                    self.runtime_deps._channel_rejoin_delay(attempt)
                )
                attempt += 1
            if not self._can_recover(topic, generation):
                return
            try:
                await self._rejoin_topic(topic, generation)
            except (ChannelError, TimeoutError, SocketError) as exc:
                if topic.cause in {ChannelCause.REJECTED, ChannelCause.CONFIGURATION}:
                    self._start_topic_loss(topic, self._rejoin_rejected(topic, exc))
                    return
                if not self._can_recover(topic, generation):
                    return
                self.logger.warning(
                    "Rejoin attempt %s failed for topic %s [%s]",
                    attempt,
                    topic.name,
                    type(exc).__name__,
                )
                continue
            # The channel can crash again before this task resumes from the join
            # reply; then the error is already handled and the loop goes on.
            if self._is_current_join_ready(topic):
                self.logger.info(
                    "Recovered channel for topic %s after %s attempt(s)",
                    topic.name,
                    attempt,
                )
                return

    def _rejoin_rejected(
        self, topic: TopicSubscription, error: Exception
    ) -> ChannelError:
        failure = ChannelError(f"Failed to rejoin topic {topic.name}: {error}")
        failure.__cause__ = error
        return failure

    async def _lose_topic(self, topic: TopicSubscription, error: Exception) -> None:
        if await self._unregister_topic(topic.name, error=error, only=topic) is None:
            return
        await self._report_topic_lost(topic, error)

    async def _report_topic_lost(
        self, topic: TopicSubscription, error: Exception
    ) -> None:
        if topic.loss_reported:
            return
        topic.loss_reported = True
        self.logger.warning("Lost topic %s: %s", topic.name, error)
        if self.on_topic_lost is not None:
            await self.runtime_deps._invoke_callback_safely(
                "on_topic_lost", self.on_topic_lost, topic.name, error
            )

    def _snapshot_handlers(
        self, topic: TopicSubscription, message: Message
    ) -> tuple[
        Callable[[Message], Awaitable[None]] | None,
        Callable[[dict[str, Any]], Awaitable[None]] | None,
    ]:
        message_handler = topic.async_callback
        event_handler = topic.get_event_handler(message.event)
        return message_handler, event_handler

    async def _handle_normal_message_mode(
        self, topic: TopicSubscription, message: Message
    ) -> None:
        self.logger.debug(
            "Processing normal message for topic %s: %s", topic.name, message
        )

        message_handler, event_handler = self._snapshot_handlers(topic, message)
        if message_handler is None and event_handler is None:
            self.logger.warning(
                "No handler found for event %s on topic %s", message.event, topic.name
            )
            return

        try:
            # One task for both handlers, so a rejoin's drain waits for the whole
            # message.
            topic.current_callback_task = asyncio.create_task(
                _run_handlers(message, message_handler, event_handler)
            )
            await topic.current_callback_task
        except Exception:
            self.logger.exception("Error in topic callback for %s", topic.name)
        finally:
            topic.current_callback_task = None

    def _handle_leave_response(
        self, topic: TopicSubscription, message: Message
    ) -> None:
        if message.event != SystemEvent.reply or message.ref != topic.leave_ref:
            return
        if topic.unsubscribe_completed.done():
            return
        if isinstance(message.payload, dict) and message.payload.get("status") == "ok":
            self._set_unsubscribe_completed(topic)
            return
        self._set_future_exception(
            topic.unsubscribe_completed, ChannelError("Leave acknowledgement rejected")
        )

    def _set_unsubscribe_completed(self, topic: TopicSubscription) -> None:
        if not topic.unsubscribe_completed.done():
            topic.unsubscribe_completed.set_result(None)

    def _fail_pending_joins(self, error: Exception) -> None:
        for topic in list(self.topic_subscriptions.values()):
            if topic.connection is not None and topic.connection is not self.connection:
                continue
            if topic.state == ChannelState.LEFT:
                self._set_future_exception(topic.unsubscribe_completed, error)
                continue
            self._transition_channel(topic, ChannelCause.TRANSPORT, error)
            if topic.leave_requested.is_set():
                self._set_future_exception(topic.unsubscribe_completed, error)
            if topic.state == ChannelState.LEFT and topic.leave_task is None:
                self._start_leave_task(topic, asyncio.current_task())

    def _retire_channels(self, error: Exception) -> None:
        for topic in list(self.topic_subscriptions.values()):
            self._transition_channel(topic, ChannelCause.STOP, error)
            if topic.connection is not self.connection:
                topic.terminal_received.set()
                self._start_leave_task(topic, asyncio.current_task())
                self._release_reservation(topic)

    def _set_future_exception(
        self,
        future: asyncio.Future[Any],
        error: Exception,
    ) -> None:
        if future.done():
            return
        future.set_exception(error)
        future.add_done_callback(lambda done_future: done_future.exception())

    def _release_reservation(self, topic: TopicSubscription) -> None:
        if not self._is_registered(topic) or topic.state != ChannelState.LEFT:
            return
        if not topic.cleanup_done and (
            topic.connection is self.connection or topic.leave_task is None
        ):
            return
        if topic.join_submitted and not topic.terminal_received.is_set():
            return
        if topic.send_session is not None and any(
            topic.send_session.has_pending(outcome) for outcome in topic.send_outcomes
        ):
            return
        self.topic_subscriptions.pop(topic.name, None)

    async def _unregister_topic(
        self,
        topic_name: str,
        error: Exception | None = None,
        *,
        only: TopicSubscription | None = None,
    ) -> TopicSubscription | None:
        topic = self.topic_subscriptions.get(topic_name)
        if topic is None or (only is not None and topic is not only):
            return None
        if topic.state != ChannelState.LEFT:
            self._transition_channel(
                topic, ChannelCause.CLOSE, error or ChannelError("Channel retired")
            )
        try:
            await self._leave_channel(topic)
        except (ChannelError, SocketError):
            self.logger.exception(
                "Permanent topic loss cleanup failed for %s", topic.name
            )
        return topic

    async def _cleanup_subscription(
        self, topic: TopicSubscription, initiator: asyncio.Task[Any] | None
    ) -> None:
        if topic.recovery_task is not initiator:
            await _cancel_unless_current(topic.recovery_task)
        if topic.process_topic_messages_task is not initiator:
            if (
                topic.cause == ChannelCause.LEAVE
                and topic.current_callback_task is not initiator
            ):
                await self._drain_callback(topic)
            await self._stop_topic_task(topic)
        if topic.send_session is not None:
            for outcome in topic.send_outcomes:
                await topic.send_session.wait_for_send(outcome)
        topic.cleanup_done = True
        self._release_reservation(topic)

    def _finish_cleanup_task(self, task: asyncio.Task[None]) -> None:
        self.cleanup_tasks.discard(task)
        if not task.cancelled() and (error := task.exception()):
            self.logger.error("Topic cleanup failed", exc_info=error)

    def _generate_ref(self) -> str:
        self.ref_counter += 1
        return str(self.ref_counter)

    def _ensure_can_send(self, operation: str) -> None:
        if not self._is_connected():
            raise SocketError(
                f"Cannot {operation} while client is {self.state}. Wait for "
                "reconnection."
            )

    def _session_for(self, connection: ClientConnection) -> SessionSends:
        current = getattr(self, "send_session", None)
        if current is not None and current.connection is connection:
            return current
        generation = self.conn_generation
        session = SessionSends(
            connection,
            generation,
            is_current=lambda: (
                self.connection is connection
                and self.conn_generation == generation
                and getattr(self, "send_session", None) is session
            ),
            on_failure=lambda error: self._fail_send_session(session, error),
        )
        self.send_session = session
        return session

    def _fail_send_session(self, session: SessionSends, error: Exception) -> None:
        if (
            self.connection is not session.connection
            or getattr(self, "send_session", None) is not session
        ):
            return
        self._fail_pending_joins(error)
        if self.state == ClientState.CONNECTED:
            self.runtime_deps._transition_state(ClientState.RECONNECTING)
        self._track_lifecycle_task(self._close_send_session(session))

    async def _close_send_session(self, session: SessionSends) -> None:
        if (
            self.connection is session.connection
            and getattr(self, "send_session", None) is session
        ):
            await self.runtime_deps._force_close_connection(
                session.connection, "Channel send failed or stalled"
            )

    async def _send_channel_operation(
        self,
        topic: TopicSubscription,
        message: Message,
        outcome: asyncio.Future[T],
        *,
        timeout_s: float,
        timeout_error: Callable[[], Exception],
    ) -> T:
        connection = topic.connection
        leaving = message.event == SystemEvent.leave
        joining = message.event == SystemEvent.join
        if connection is None or connection is not self.connection:
            raise SocketError("Connection lost before channel operation")
        captured_connection: ClientConnection = connection
        # Freeze the frame before scheduling any send continuation.
        frame = self.protocol_handler.serialize_message(message)
        session = topic.send_session = self._session_for(connection)
        topic.send_outcomes = {
            existing
            for existing in topic.send_outcomes
            if session.has_pending(existing)
        }
        topic.send_outcomes.add(outcome)

        def eligible() -> bool:
            return (
                self._is_registered(topic)
                and self.connection is connection
                and (
                    leaving
                    or (
                        (joining or self._channel_joined(topic))
                        and topic.state != ChannelState.LEFT
                        and not self.shutdown_event.is_set()
                    )
                )
                and message.join_ref == topic.join_ref
            )

        async def send() -> None:
            if joining:
                topic.join_submitted = True
            try:
                await captured_connection.send(frame)
            except ConnectionClosed as exc:
                raise SocketError("Connection lost during channel send") from exc

        return await session.send_and_wait(
            send,
            outcome,
            timeout_s=timeout_s,
            timeout_error=timeout_error,
            eligible=eligible,
            ineligible_error=lambda: ChannelError(
                "Channel operation no longer current"
            ),
        )

    async def _push_channel(
        self,
        topic: TopicSubscription,
        event: ChannelEvent,
        payload: dict[str, Any],
        *,
        timeout_s: float,
    ) -> Reply:
        if not self._channel_joined(topic):
            raise ChannelError("Channel is not joined")
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("timeout_s must be finite and > 0")
        if not isinstance(payload, dict):
            raise TypeError("Push payload must be a map")
        if event in {*SystemEvent, HEARTBEAT_EVENT}:
            raise ValueError("push requires an application event")
        ref = self._generate_ref()
        message = make_message(
            event=event,
            topic=topic.name,
            ref=ref,
            join_ref=topic.join_ref,
            payload=payload,
        )
        outcome: asyncio.Future[Reply] = asyncio.get_running_loop().create_future()
        topic.pending_pushes[ref] = outcome
        try:
            return await self._send_channel_operation(
                topic,
                message,
                outcome,
                timeout_s=timeout_s,
                timeout_error=lambda: PushTimeoutError("Push reply deadline expired"),
            )
        finally:
            if topic.pending_pushes.get(ref) is outcome:
                topic.pending_pushes.pop(ref)

    def _resolve_join_params(self, topic: TopicSubscription) -> dict[str, Any]:
        provider = topic.params
        if provider is None:
            return {}
        try:
            value = provider() if callable(provider) else provider
            if isinstance(value, dict):
                return dict(value)
        except (Exception, asyncio.CancelledError) as exc:
            raise ChannelError("Join params resolution failed") from exc
        if (
            inspect.iscoroutine(value)
            and inspect.getcoroutinestate(value) == inspect.CORO_CREATED
        ):
            value.close()
        raise ChannelError("Join params must resolve to a dict") from TypeError(
            "params must be a dict or synchronous dict provider"
        )

    def _validate_join_hook(self, topic: TopicSubscription) -> None:
        hook = topic.on_join_response
        if inspect.iscoroutinefunction(hook) or inspect.iscoroutinefunction(
            hook.__call__ if callable(hook) else None
        ):
            raise ChannelError("on_join_response must be synchronous")

    async def _join(self, topic: TopicSubscription) -> None:
        try:
            join_message = make_message(
                topic=topic.name,
                event=SystemEvent.join,
                payload=self._resolve_join_params(topic),
                ref=topic.join_ref,
                join_ref=topic.join_ref,
            )
            await self._send_channel_operation(
                topic,
                join_message,
                topic.current_join_ready,
                timeout_s=self.join_timeout_s,
                timeout_error=TimeoutError,
            )
        except (ChannelError, TypeError, ValueError) as exc:
            if topic.cause not in {
                ChannelCause.CRASH,
                ChannelCause.TRANSPORT,
                ChannelCause.LEAVE,
                ChannelCause.STOP,
                ChannelCause.CLOSE,
                ChannelCause.REJECTED,
                ChannelCause.CONFIGURATION,
            }:
                if isinstance(exc, ChannelError):
                    self._transition_channel(topic, ChannelCause.CONFIGURATION, exc)
                    raise
                error = ChannelError("Join params encoding failed")
                self._transition_channel(topic, ChannelCause.CONFIGURATION, error)
                raise error from exc
            raise
        except TimeoutError:
            self._transition_channel(
                topic, ChannelCause.TIMEOUT, ChannelError("Join deadline expired")
            )
            raise

    async def _join_channel(self, topic: TopicSubscription) -> None:
        async with self.topics_lock:
            if topic.state != ChannelState.CREATED:
                raise ChannelError("Channel has already joined or left")
            self._ensure_can_send("join")
            if topic.name in self.topic_subscriptions:
                raise ChannelError(f"Topic {topic.name} already has a live channel")
            topic.state = ChannelState.JOINING
            topic.conn_generation = self.conn_generation
            topic.connection = self.connection
            topic.join_ref = self._generate_ref()
            self.topic_subscriptions[topic.name] = topic
        topic.process_topic_messages_task = asyncio.create_task(
            self._process_topic_messages(topic.name)
        )
        try:
            self._validate_join_hook(topic)
            await self._join(topic)
        except TimeoutError as exc:
            await self._finish_failed_join(topic)
            raise ChannelError(f"Timed out joining {topic.name}") from exc
        except (Exception, asyncio.CancelledError):
            await self._finish_failed_join(topic)
            raise

    async def _finish_failed_join(self, topic: TopicSubscription) -> None:
        try:
            await self._leave_channel(topic)
        except Exception:
            self.logger.exception("Failed join cleanup for %s", topic.name)

    async def _leave_channel(
        self, topic: TopicSubscription, *, initiator: asyncio.Task[Any] | None = None
    ) -> None:
        if topic.state == ChannelState.CREATED:
            topic.state = ChannelState.LEFT
            topic.cause = ChannelCause.LEAVE
            topic.leave_requested.set()
            return
        task = self._start_leave_task(topic, initiator or asyncio.current_task())
        if task.done():
            return
        await asyncio.shield(task)

    def _start_leave_task(
        self, topic: TopicSubscription, initiator: asyncio.Task[Any] | None
    ) -> asyncio.Task[None]:
        if topic.leave_task is None:
            if topic.state != ChannelState.LEFT:
                self._transition_channel(
                    topic, ChannelCause.LEAVE, ChannelError("Channel was left")
                )
            topic.leave_task = asyncio.create_task(
                self._leave_and_cleanup(topic, initiator)
            )
            self.cleanup_tasks.add(topic.leave_task)
            topic.leave_task.add_done_callback(self._finish_cleanup_task)
        return topic.leave_task

    async def _leave_and_cleanup(
        self, topic: TopicSubscription, initiator: asyncio.Task[Any] | None
    ) -> None:
        try:
            if (
                topic.join_submitted
                and not topic.terminal_received.is_set()
                and topic.connection is self.connection
                and self._is_connected()
            ):
                topic.leave_ref = self._generate_ref()
                message = make_message(
                    topic=topic.name,
                    event=SystemEvent.leave,
                    payload={},
                    ref=topic.leave_ref,
                    join_ref=topic.join_ref,
                )
                await self._send_channel_operation(
                    topic,
                    message,
                    topic.unsubscribe_completed,
                    timeout_s=self.leave_timeout_s,
                    timeout_error=lambda: ChannelError("Leave deadline expired"),
                )
            elif topic.connection is not self.connection or self.connection is None:
                topic.terminal_received.set()
        finally:
            await self._cleanup_subscription(topic, initiator)

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
        async with self.topics_lock:
            subscriptions = list(self.topic_subscriptions.values())

        for topic_subscription in subscriptions:
            if not topic_subscription.blocks_reconnect:
                continue
            try:
                await self._rejoin_topic(topic_subscription, generation)
            except Exception as exc:  # noqa: BLE001  # handled per topic
                await self._handle_rejoin_failure(topic_subscription, exc)

        for topic_subscription in subscriptions:
            if not topic_subscription.blocks_reconnect:
                self._start_channel_recovery(topic_subscription, generation=generation)

    async def _rejoin_topic(
        self, topic_subscription: TopicSubscription, generation: int
    ) -> None:
        if not self._should_rejoin(topic_subscription):
            return

        if topic_subscription.state == ChannelState.JOINED:
            self._transition_channel(
                topic_subscription,
                ChannelCause.TRANSPORT,
                SocketError("Connection replaced before channel rejoin"),
            )
        topic_subscription.restart_requested = True
        await self._drain_callback(topic_subscription)
        await self._stop_topic_task(topic_subscription)
        # The topic may be left or unregistered during any await before the restart.
        if not self._should_rejoin(topic_subscription):
            return

        self._restart_topic(topic_subscription, generation)
        await self._join(topic_subscription)

    def _is_registered(self, topic_subscription: TopicSubscription) -> bool:
        return (
            self.topic_subscriptions.get(topic_subscription.name) is topic_subscription
        )

    def _should_rejoin(self, topic_subscription: TopicSubscription) -> bool:
        return (
            self._is_registered(topic_subscription)
            and topic_subscription.established
            and not topic_subscription.leave_requested.is_set()
            and topic_subscription.state != ChannelState.LEFT
        )

    async def _stop_topic_task(self, topic_subscription: TopicSubscription) -> None:
        await _cancel_unless_current(topic_subscription.process_topic_messages_task)

    def _restart_topic(
        self, topic_subscription: TopicSubscription, generation: int
    ) -> None:
        topic_subscription.conn_generation = generation
        topic_subscription.connection = self.connection
        topic_subscription.state = ChannelState.REJOINING
        topic_subscription.cause = None
        topic_subscription.join_submitted = False
        topic_subscription.terminal_received.clear()
        topic_subscription.restart_requested = False
        topic_subscription.join_ref = self._generate_ref()
        topic_subscription.current_join_ready = (
            asyncio.get_running_loop().create_future()
        )
        self._drain_topic_queue(topic_subscription)
        topic_subscription.process_topic_messages_task = asyncio.create_task(
            self._process_topic_messages(topic_subscription.name)
        )

    async def _handle_rejoin_failure(
        self, topic: TopicSubscription, exc: Exception
    ) -> None:
        if self.shutdown_event.is_set() or self.state == ClientState.SHUTTING_DOWN:
            self.logger.debug(
                "Ignoring rejoin failure for topic %s during shutdown: %s",
                topic.name,
                exc,
            )
            return

        if topic.cause in {ChannelCause.REJECTED, ChannelCause.CONFIGURATION}:
            error = self._rejoin_rejected(topic, exc)
            # Gone before this returns, so on_reconnect does not see a topic
            # the server has already rejected. The callback stays its own task.
            removed = await self._unregister_topic(topic.name, error=error, only=topic)
            if removed is None:
                return
            self._track_lifecycle_task(self._report_topic_lost(topic, error))
            return

        self.logger.warning(
            "Transient rejoin failure for topic %s: %s: %s",
            topic.name,
            type(exc).__name__,
            exc,
        )
        self._start_channel_recovery(topic)

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
