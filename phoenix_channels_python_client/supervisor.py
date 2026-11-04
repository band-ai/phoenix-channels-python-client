from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import NamedTuple, Protocol, cast

from websockets import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from phoenix_channels_python_client.client_types import (
    ClientState,
    Connected,
    ConnectFailed,
    Connecting,
    Disconnected,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.exceptions import SocketError
from phoenix_channels_python_client.messages import (
    HEARTBEAT_EVENT,
    PHOENIX_TOPIC,
    Message,
)
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.shutdown_signals import handle_shutdown_signals
from phoenix_channels_python_client.topic_subscription import (
    ChannelState,
    TopicSubscription,
)
from phoenix_channels_python_client.utils import cancel_and_wait, make_message

# RFC 6455 §7.4.2 private-use range (4000-4999, unregistrable). The decider
# never sees it: a forced close is decided with close_code=None.
FORCED_CLOSE_CODE = 4000

# RFC 6455 §5.5.1: control frames (including Close) cap at 125 bytes total;
# 2 of those bytes are the close code, leaving 123 for the reason.
MAX_CLOSE_REASON_BYTES = 123


def _truncate_close_reason(reason: str) -> str:
    encoded = reason.encode("utf-8")
    if len(encoded) <= MAX_CLOSE_REASON_BYTES:
        return reason
    return encoded[:MAX_CLOSE_REASON_BYTES].decode("utf-8", errors="ignore")


class CloseDetails(NamedTuple):
    code: int | None
    reason: str
    forced: bool


class SupervisorRuntimeDeps(Protocol):
    def redact_diagnostic(self, text: str) -> str: ...

    def _generate_ref(self) -> str: ...
    async def _rejoin_topics(self, generation: int) -> None: ...
    def _fail_pending_joins(self, error: Exception) -> None: ...
    def _retire_channels(self, error: Exception) -> None: ...
    def _handle_channel_control(
        self,
        topic: TopicSubscription,
        message: Message,
        binary_reply_status: str | None = None,
    ) -> bool: ...
    def _release_reservation(self, topic: TopicSubscription) -> None: ...
    def _channel_recoveries(self) -> list[asyncio.Task[None]]: ...
    def _extract_close_details(
        self, *, connection: ClientConnection, routing_error: Exception | None
    ) -> tuple[int | None, str]: ...
    def _decide(self, event: ReconnectEvent) -> ReconnectDecision: ...
    def _transition_state(self, new_state: ClientState) -> None: ...
    async def shutdown(self, reason: str) -> None: ...


class SupervisorMixin:
    logger: logging.Logger
    channel_socket_url: str
    channel_socket_url_redacted: str
    additional_headers: dict[str, str]
    auto_reconnect: bool
    connection: ClientConnection | None
    topic_subscriptions: dict[str, TopicSubscription]
    protocol_handler: PHXProtocolHandler
    shutdown_event: asyncio.Event
    connected_event: asyncio.Event
    conn_generation: int
    state: ClientState
    supervisor_task: asyncio.Task[None] | None
    message_routing_task: asyncio.Task[None] | None
    initial_connection_future: asyncio.Future[None] | None
    terminal_error: Exception | None
    heartbeat_dead_threshold_s: float | None
    heartbeat_deadline: float | None
    heartbeat_timeout: asyncio.Timeout | None
    heartbeat_interval_s: float | None
    heartbeat_task: asyncio.Task[None] | None
    pending_heartbeat_ref: str | None
    on_reconnect: Callable[[], Awaitable[None]] | None
    on_disconnect: Callable[[Exception | None], Awaitable[None]] | None
    on_heartbeat_ack: Callable[[], None] | None
    forced_close_pending: bool

    @property
    def deps(self) -> SupervisorRuntimeDeps:
        return cast(SupervisorRuntimeDeps, self)

    async def _start_processing(
        self, connection: ClientConnection, conn_generation: int
    ) -> None:
        await self.protocol_handler.process_websocket_messages(
            connection,
            self.topic_subscriptions,
            conn_generation,
            on_heartbeat_response=self._handle_heartbeat_response,
            on_channel_control=self.deps._handle_channel_control,
        )

    def _handle_heartbeat_response(self, message: Message) -> None:
        if message.ref is not None and message.ref == self.pending_heartbeat_ref:
            if self.heartbeat_dead_threshold_s is not None:
                timer = self.heartbeat_timeout
                now = asyncio.get_running_loop().time()
                if (
                    timer is None
                    or timer.expired()
                    or (deadline := timer.when()) is None
                    or now >= deadline
                ):
                    return
                timer.reschedule(now + self.heartbeat_dead_threshold_s)
            self.logger.debug("Heartbeat acknowledged (ref=%s)", message.ref)
            self.pending_heartbeat_ref = None
            if self.on_heartbeat_ack is not None:
                try:
                    result = self.on_heartbeat_ack()
                except Exception:
                    self.logger.exception("Error in on_heartbeat_ack callback")
                    return
                if inspect.iscoroutine(result):
                    result.close()
                    self.logger.error(
                        "on_heartbeat_ack must be synchronous; an async "
                        "function was passed and its body never ran"
                    )

    async def _invoke_callback_safely(
        self, label: str, callback: Callable[..., Awaitable[None]], *args: object
    ) -> None:
        try:
            await callback(*args)
        except Exception:
            self.logger.exception("Error in %s callback", label)

    async def close_connection(self, reason: str) -> None:
        """Force-close the connection; disconnect handling decides on a reconnect.

        No-op if not currently connected or the connection is already closing.
        """
        connection = self.connection
        if connection is None:
            return
        await self._force_close_connection(connection, reason)

    async def _force_close_connection(
        self, connection: ClientConnection, reason: str
    ) -> None:
        if connection.state is not State.OPEN:
            self.logger.debug(
                "Not forcing a close; connection is already %s", connection.state.name
            )
            return
        reason = _truncate_close_reason(reason)
        self.logger.info("Forcing connection close: %s", reason)
        self.forced_close_pending = True
        try:
            await connection.close(code=FORCED_CLOSE_CODE, reason=reason)
        except (Exception, asyncio.CancelledError):
            connection.transport.abort()
            raise

    async def _heartbeat_loop(
        self, connection: ClientConnection, deadline: float | None = None
    ) -> None:
        timer = asyncio.timeout_at(deadline)
        try:
            async with timer:
                self.heartbeat_timeout = timer
                try:
                    await self._send_heartbeats(connection)
                finally:
                    self.heartbeat_timeout = None
        except TimeoutError:
            if timer.expired():
                self.logger.warning("Heartbeat acknowledgement deadline expired")
            elif self.heartbeat_dead_threshold_s is not None:
                self.logger.exception("Heartbeat send timed out")
        except Exception:
            if self.heartbeat_dead_threshold_s is None:
                self.logger.debug("Failed to send heartbeat; connection likely closing")
                return
            self.logger.exception("Failed to send heartbeat")
        else:
            return

        if self.heartbeat_dead_threshold_s is not None:
            try:
                await self._force_close_connection(connection, "Heartbeat failed")
            except Exception:
                self.logger.exception("Failed to close heartbeat connection")

    async def _send_heartbeats(self, connection: ClientConnection) -> None:
        interval = self.heartbeat_interval_s
        if interval is None:
            return
        while not self.shutdown_event.is_set():
            await self._wait_for_shutdown_or_timeout(interval)
            if self.shutdown_event.is_set():
                return
            if self.pending_heartbeat_ref is not None:
                if self.heartbeat_dead_threshold_s is not None:
                    continue
                self.logger.warning(
                    "Heartbeat response not received for ref=%s; server may be "
                    "unresponsive",
                    self.pending_heartbeat_ref,
                )
            ref = self.deps._generate_ref()
            self.pending_heartbeat_ref = ref
            await self.protocol_handler.send_message(
                connection,
                make_message(
                    topic=PHOENIX_TOPIC, event=HEARTBEAT_EVENT, payload={}, ref=ref
                ),
            )
            self.logger.debug("Sent heartbeat (ref=%s)", ref)

    async def _supervisor_loop(self) -> None:
        try:
            while not self.shutdown_event.is_set():
                if self.auto_reconnect and self._proceeds(Connecting()) is None:
                    break
                try:
                    connection = await self._connect()
                except Exception as exc:  # noqa: BLE001  # each failure is retried
                    decision = self._retry_after_connect_failure(exc)
                    if decision is None:
                        break
                    await self._wait_for_shutdown_or_timeout(decision.delay_s)
                    continue

                if not await self._adopt_connection(connection):
                    break
                routing = await self._start_session(connection)
                routing_error = await self._await_routing(routing)
                close = await self._end_session(connection, routing_error)
                decision = self._decide_reconnect(close)
                if decision is None:
                    break
                self.deps._transition_state(ClientState.RECONNECTING)
                await self._wait_for_shutdown_or_timeout(decision.delay_s)
        finally:
            self._settle_supervisor_exit()

    async def _connect(self) -> ClientConnection:
        return await connect(
            self.channel_socket_url,
            additional_headers=self.additional_headers,
            logger=self.logger,
        )

    def _retry_after_connect_failure(self, exc: Exception) -> ReconnectDecision | None:
        """The decision to retry after ``exc``, or ``None`` to stop."""
        if not self.auto_reconnect:
            self._settle_initial_connection(
                SocketError(
                    self.deps.redact_diagnostic(
                        "Failed to connect to "
                        f"{self.channel_socket_url_redacted}: {exc}"
                    )
                )
            )
            self.logger.error("Connection failed and auto_reconnect=False: %s", exc)
            return None
        # A requested stop outranks a refusal's terminal error.
        if self.shutdown_event.is_set():
            self.logger.debug(
                "Ignoring a connect failure after shutdown began: %s", exc
            )
            return None
        decision = self._proceeds(ConnectFailed(exc))
        if decision is not None:
            self.deps._transition_state(ClientState.RECONNECTING)
        return decision

    async def _adopt_connection(self, connection: ClientConnection) -> bool:
        self.connection = connection
        if self.shutdown_event.is_set():
            # shutdown() began during the handshake; its own cleanup closes
            # this socket.
            return False
        self.conn_generation += 1
        if (
            self.auto_reconnect
            and self._proceeds(Connected(self.conn_generation)) is None
        ):
            # Refused before any topic rejoins on it.
            await self._cleanup_connection()
            return False
        self.heartbeat_deadline = (
            asyncio.get_running_loop().time() + self.heartbeat_dead_threshold_s
            if self.heartbeat_interval_s is not None
            and self.heartbeat_dead_threshold_s is not None
            else None
        )
        self.connected_event.set()
        self.deps._transition_state(ClientState.CONNECTED)
        return True

    async def _start_session(self, connection: ClientConnection) -> asyncio.Task[None]:
        generation = self.conn_generation
        self.pending_heartbeat_ref = None
        routing = self.message_routing_task = asyncio.create_task(
            self._start_processing(connection, generation)
        )

        # Join replies arrive only through this task. It can stop while
        # a rejoin is still waiting on one, before cleanup runs.
        def routing_stopped(task: asyncio.Task[None]) -> None:
            if self.connection is connection and self.message_routing_task is task:
                self._fail_joins_when_routing_stops(task)

        routing.add_done_callback(routing_stopped)
        if self.heartbeat_interval_s is not None:
            self.heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(connection, self.heartbeat_deadline)
            )

        try:
            await self.deps._rejoin_topics(generation)
        except Exception:
            self.logger.exception("Unexpected error while rejoining topics")

        self._settle_initial_connection(None)
        if (
            generation > 1
            and self.on_reconnect is not None
            and not self.shutdown_event.is_set()
        ):
            await self._invoke_callback_safely("on_reconnect", self.on_reconnect)
        return routing

    async def _await_routing(self, routing: asyncio.Task[None]) -> Exception | None:
        try:
            await routing
        except ConnectionClosed as exc:
            self.logger.info(
                "Message routing stopped due to websocket close code=%s reason=%s",
                exc.rcvd.code if exc.rcvd is not None else None,
                exc.rcvd.reason if exc.rcvd is not None else "",
            )
            return exc
        except Exception as exc:
            self.logger.exception("Message routing task failed")
            return exc
        return None

    async def _end_session(
        self, connection: ClientConnection, routing_error: Exception | None
    ) -> CloseDetails:
        code, reason = self.deps._extract_close_details(
            connection=connection, routing_error=routing_error
        )
        close = CloseDetails(code, reason, forced=self.forced_close_pending)

        await self._cleanup_connection()

        if self.on_disconnect is not None:
            await self._invoke_callback_safely(
                "on_disconnect", self.on_disconnect, routing_error
            )
        return close

    def _decide_reconnect(self, close: CloseDetails) -> ReconnectDecision | None:
        if self.shutdown_event.is_set() or not self.auto_reconnect:
            return None

        # websockets reports the close code it received, not the one we sent,
        # so a forced close carries no code to decide on.
        event = (
            Disconnected(close_code=None, close_reason="")
            if close.forced
            else Disconnected(close_code=close.code, close_reason=close.reason)
        )
        return self._proceeds(event)

    def _proceeds(self, event: ReconnectEvent) -> ReconnectDecision | None:
        """The decision to go on after ``event``, or ``None`` when it stops."""
        decision = self.deps._decide(event)
        if decision.should_reconnect:
            return decision
        if decision.terminal_error is not None:
            self._record_terminal_error(decision.terminal_error)
        else:
            self.logger.info("Reconnect stopped by the reconnect policy")
        return None

    def _record_terminal_error(self, error: SocketError) -> None:
        self.terminal_error = error
        # Log records stay static: the cause reaches the caller as the raised error.
        self.logger.error("Reconnect stopped by a terminal error")
        self._settle_initial_connection(error)

    def _settle_initial_connection(self, error: Exception | None) -> None:
        """Resolve ``__aenter__``'s wait, unless it already ended or was cancelled."""
        future = self.initial_connection_future
        if future is None or future.done():
            return
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    def _settle_supervisor_exit(self) -> None:
        self._settle_initial_connection(
            SocketError("Connection supervisor stopped before connecting")
        )
        self.connected_event.clear()
        if self.state != ClientState.SHUTTING_DOWN:
            if self.topic_subscriptions:
                self.deps._retire_channels(SocketError("Connection supervisor stopped"))
            self.deps._transition_state(ClientState.CLOSED)

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self.shutdown_event.wait(), timeout=delay_s)
        except TimeoutError:
            return

    def _fail_joins_when_routing_stops(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            self._fail_pending_joins_for_lost_connection(task.exception())

    def _fail_pending_joins_for_lost_connection(
        self, cause: BaseException | None = None
    ) -> None:
        error = SocketError("Connection lost while waiting for a channel operation")
        error.__cause__ = cause
        self.deps._fail_pending_joins(error)

    async def _cleanup_connection(self) -> None:
        self._fail_pending_joins_for_lost_connection()
        connection = self.connection
        send_session = getattr(self, "send_session", None)
        # The socket rejoin after the reconnect takes over channel recoveries.
        recoveries = self.deps._channel_recoveries()
        running = [
            task
            for task in (self.heartbeat_task, self.message_routing_task, *recoveries)
            if task is not None and not task.done()
        ]
        self.connection = None
        self.heartbeat_task = None
        self.heartbeat_deadline = None
        self.message_routing_task = None
        self.pending_heartbeat_ref = None
        self.connected_event.clear()
        self.forced_close_pending = False

        try:
            await cancel_and_wait(*running)
        finally:
            # Close the socket even if the caller is cancelled meanwhile.
            if connection is not None:
                await self._close_websocket(connection)
            if send_session is not None:
                await send_session.finish()
                if getattr(self, "send_session", None) is send_session:
                    self.send_session = None
            for topic in list(self.topic_subscriptions.values()):
                if topic.connection is connection:
                    topic.terminal_received.set()
                    if topic.state == ChannelState.LEFT:
                        self.deps._release_reservation(topic)

    async def _close_websocket(self, connection: ClientConnection) -> None:
        try:
            await connection.close()
        except asyncio.CancelledError:
            # websockets enforces close_timeout only in the task awaiting close().
            self.logger.debug("Close cancelled before the handshake ended; aborting")
            connection.transport.abort()
            raise
        except Exception:
            self.logger.exception("Failed while closing websocket connection")

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        """Wait until the client stops, then raise why if it failed.

        With ``install_signal_handlers`` (the default), SIGTERM and SIGINT shut
        the client down. The handlers in place before the call are restored once
        the last waiting ``run_forever()`` stops waiting, so a second signal
        during the shutdown reaches them. A host handler registered with
        ``loop.add_signal_handler`` before the call also fires; one registered
        during it replaces ours. Hosts that own their process signals pass
        ``False`` and schedule ``shutdown()`` on the client's loop from their
        own handler.
        """
        supervisor = self.supervisor_task
        if supervisor is None:
            raise SocketError(
                "Client is not connected. Use 'async with' context manager."
            )

        signaled = asyncio.Event()
        signals = (
            handle_shutdown_signals(signaled)
            if install_signal_handlers
            else nullcontext()
        )
        with signals:
            signal_wait = asyncio.create_task(signaled.wait())
            try:
                await asyncio.wait(
                    {supervisor, signal_wait}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                signal_wait.cancel()

        # No await since leaving the `with`, so this sees `signaled` exactly as
        # handle_shutdown_signals judged it consumed.
        if signaled.is_set():
            await self.deps.shutdown("Signal received")
        shutdown = getattr(self, "shutdown_task", None)
        if self.shutdown_event.is_set() and shutdown is not None:
            await asyncio.shield(shutdown)
        if self.terminal_error is not None:
            raise self.terminal_error
        if supervisor.cancelled() and self.shutdown_event.is_set():
            return  # a requested stop
        supervisor.result()
