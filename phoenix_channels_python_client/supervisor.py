from __future__ import annotations

import asyncio
import inspect
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import NamedTuple, Protocol, cast

from websockets import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectDecision,
    ReconnectPolicy,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import (
    HEARTBEAT_EVENT,
    PHOENIX_TOPIC,
    ChannelMessage,
)
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.shutdown_signals import handle_shutdown_signals
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import cancel_and_wait, make_message

# RFC 6455 §7.4.2 private-use range (4000-4999, unregistrable); outside
# _classify_disconnect's specially-handled codes, so a forced close
# reconnects like any other unclassified disconnect.
_FORCED_CLOSE_CODE = 4000

# RFC 6455 §5.5.1: control frames (including Close) cap at 125 bytes total;
# 2 of those bytes are the close code, leaving 123 for the reason.
_MAX_CLOSE_REASON_BYTES = 123


def _truncate_close_reason(reason: str) -> str:
    encoded = reason.encode("utf-8")
    if len(encoded) <= _MAX_CLOSE_REASON_BYTES:
        return reason
    return encoded[:_MAX_CLOSE_REASON_BYTES].decode("utf-8", errors="ignore")


class _CloseDetails(NamedTuple):
    code: int | None
    reason: str
    forced: bool


class _SupervisorRuntimeDeps(Protocol):
    def _generate_ref(self) -> str: ...
    async def _rejoin_topics(self, generation: int) -> None: ...
    def _fail_pending_joins(self, error: Exception) -> None: ...
    def _record_disconnect(self, connection_uptime_s: float) -> None: ...
    def _should_suppress_reconnect(self) -> bool: ...
    def _compute_reconnect_delay(self, attempt: int) -> float: ...
    def _extract_close_details(
        self, *, connection: ClientConnection, routing_error: Exception | None
    ) -> tuple[int | None, str]: ...
    def _classify_disconnect(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision: ...
    def _apply_disconnect_delay_override(
        self, computed_delay_s: float, decision: ReconnectDecision
    ) -> float: ...
    def _transition_state(self, new_state: ClientState) -> None: ...
    async def shutdown(self, reason: str) -> None: ...


class SupervisorMixin:
    logger: logging.Logger
    channel_socket_url: str
    channel_socket_url_redacted: str
    additional_headers: dict[str, str]
    auto_reconnect: bool
    reconnect_policy: ReconnectPolicy
    connection: ClientConnection | None
    _topic_subscriptions: dict[str, TopicSubscription]
    _protocol_handler: PHXProtocolHandler
    _shutdown_event: asyncio.Event
    _connected_event: asyncio.Event
    _conn_generation: int
    _state: ClientState
    _supervisor_task: asyncio.Task[None] | None
    _message_routing_task: asyncio.Task[None] | None
    _initial_connection_future: asyncio.Future[None] | None
    _rapid_disconnects: deque[float]
    _terminal_error: Exception | None
    _heartbeat_interval_s: float | None
    _heartbeat_task: asyncio.Task[None] | None
    _pending_heartbeat_ref: str | None
    _on_reconnect: Callable[[], Awaitable[None]] | None
    _on_disconnect: Callable[[Exception | None], Awaitable[None]] | None
    _on_heartbeat_ack: Callable[[], None] | None
    _forced_close_pending: bool

    @property
    def _deps(self) -> _SupervisorRuntimeDeps:
        return cast(_SupervisorRuntimeDeps, self)

    async def _start_processing(
        self, connection: ClientConnection, conn_generation: int
    ) -> None:
        await self._protocol_handler.process_websocket_messages(
            connection,
            self._topic_subscriptions,
            conn_generation,
            on_heartbeat_response=self._handle_heartbeat_response,
        )

    def _handle_heartbeat_response(self, message: ChannelMessage) -> None:
        if message.ref is not None and message.ref == self._pending_heartbeat_ref:
            self.logger.debug("Heartbeat acknowledged (ref=%s)", message.ref)
            self._pending_heartbeat_ref = None
            if self._on_heartbeat_ack is not None:
                try:
                    result = self._on_heartbeat_ack()
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
        if connection.state is not State.OPEN:
            self.logger.debug(
                "Not forcing a close; connection is already %s", connection.state.name
            )
            return
        reason = _truncate_close_reason(reason)
        self.logger.info("Forcing connection close: %s", reason)
        self._forced_close_pending = True
        await connection.close(code=_FORCED_CLOSE_CODE, reason=reason)

    async def _heartbeat_loop(self, connection: ClientConnection) -> None:
        if self._heartbeat_interval_s is None:
            return

        self.logger.debug(
            "Starting heartbeat loop (interval=%ss)", self._heartbeat_interval_s
        )

        try:
            while not self._shutdown_event.is_set():
                await self._wait_for_shutdown_or_timeout(self._heartbeat_interval_s)
                if self._shutdown_event.is_set():
                    break

                if self._pending_heartbeat_ref is not None:
                    self.logger.warning(
                        "Heartbeat response not received for ref=%s; server may be "
                        "unresponsive",
                        self._pending_heartbeat_ref,
                    )

                ref = self._deps._generate_ref()
                self._pending_heartbeat_ref = ref

                heartbeat_message = make_message(
                    topic=PHOENIX_TOPIC,
                    event=HEARTBEAT_EVENT,
                    payload={},
                    ref=ref,
                )

                try:
                    await self._protocol_handler.send_message(
                        connection, heartbeat_message
                    )
                    self.logger.debug("Sent heartbeat (ref=%s)", ref)
                except Exception:  # noqa: BLE001  # any send failure ends the loop
                    self.logger.debug(
                        "Failed to send heartbeat; connection likely closing"
                    )
                    break
        except asyncio.CancelledError:
            self.logger.debug("Heartbeat loop cancelled")
            raise

    async def _supervisor_loop(self) -> None:
        attempt = 0
        try:
            while not self._shutdown_event.is_set():
                try:
                    connection = await self._connect()
                except Exception as exc:  # noqa: BLE001  # each failure is retried
                    if not self._retry_after_connect_failure(exc):
                        break
                    delay = self._deps._compute_reconnect_delay(attempt=attempt)
                    attempt += 1
                    await self._wait_for_shutdown_or_timeout(delay)
                    continue

                if not self._adopt_connection(connection):
                    break
                connected_since = asyncio.get_running_loop().time()
                routing = await self._start_session(connection)
                routing_error = await self._await_routing(routing)
                close = await self._end_session(connection, routing_error)
                decision = self._decide_reconnect(close)
                if decision is None:
                    break

                uptime = asyncio.get_running_loop().time() - connected_since
                if self._suppressed_after_disconnect(uptime):
                    break
                if uptime >= self.reconnect_policy.stable_reset_s:
                    attempt = 0
                    self._rapid_disconnects.clear()
                else:
                    attempt += 1

                self._deps._transition_state(ClientState.RECONNECTING)
                delay = self._deps._compute_reconnect_delay(attempt=attempt)
                delay = self._deps._apply_disconnect_delay_override(delay, decision)
                await self._wait_for_shutdown_or_timeout(delay)
        finally:
            self._settle_supervisor_exit()

    async def _connect(self) -> ClientConnection:
        return await connect(
            self.channel_socket_url, additional_headers=self.additional_headers
        )

    def _retry_after_connect_failure(self, exc: Exception) -> bool:
        if not self.auto_reconnect:
            self._settle_initial_connection(
                PHXConnectionError(
                    f"Failed to connect to {self.channel_socket_url_redacted}: {exc}"
                )
            )
            self.logger.error("Connection failed and auto_reconnect=False: %s", exc)
            return False
        if self._suppressed_after_disconnect(0.0):
            return False
        if self._state != ClientState.SHUTTING_DOWN:
            self._deps._transition_state(ClientState.RECONNECTING)
        return True

    def _adopt_connection(self, connection: ClientConnection) -> bool:
        self.connection = connection
        if self._shutdown_event.is_set():
            # shutdown() began during the handshake; its own cleanup closes
            # this socket.
            return False
        self._conn_generation += 1
        self._connected_event.set()
        self._deps._transition_state(ClientState.CONNECTED)
        return True

    async def _start_session(self, connection: ClientConnection) -> asyncio.Task[None]:
        generation = self._conn_generation
        self._pending_heartbeat_ref = None
        routing = self._message_routing_task = asyncio.create_task(
            self._start_processing(connection, generation)
        )
        if self._heartbeat_interval_s is not None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(connection))

        try:
            await self._deps._rejoin_topics(generation)
        except Exception:
            self.logger.exception("Unexpected error while rejoining topics")

        self._settle_initial_connection(None)
        if (
            generation > 1
            and self._on_reconnect is not None
            and not self._shutdown_event.is_set()
        ):
            await self._invoke_callback_safely("on_reconnect", self._on_reconnect)
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
    ) -> _CloseDetails:
        code, reason = self._deps._extract_close_details(
            connection=connection, routing_error=routing_error
        )
        close = _CloseDetails(code, reason, forced=self._forced_close_pending)

        await self._cleanup_connection()

        if self._on_disconnect is not None:
            await self._invoke_callback_safely(
                "on_disconnect", self._on_disconnect, routing_error
            )
        return close

    def _decide_reconnect(self, close: _CloseDetails) -> ReconnectDecision | None:
        if self._shutdown_event.is_set() or not self.auto_reconnect:
            return None

        if close.forced:
            # websockets reports the close code it received, not the one we
            # sent, so classifying a forced close would be unreliable.
            decision = ReconnectDecision(should_reconnect=True)
        else:
            decision = self._deps._classify_disconnect(close.code, close.reason)
        if decision.terminal_error is not None:
            self._terminal_error = decision.terminal_error
            self.logger.error("%s", self._terminal_error)
            return None

        if not decision.should_reconnect:
            self.logger.info(
                "Reconnect disabled for close code %s reason=%s",
                close.code,
                close.reason,
            )
            return None
        return decision

    def _suppressed_after_disconnect(self, uptime_s: float) -> bool:
        self._deps._record_disconnect(connection_uptime_s=uptime_s)
        if not self._deps._should_suppress_reconnect():
            return False
        self._terminal_error = PHXConnectionError(
            "Reconnect suppressed after repeated rapid disconnects. "
            "Likely duplicate connection or unstable endpoint."
        )
        self.logger.error("%s", self._terminal_error)
        return True

    def _settle_initial_connection(self, error: Exception | None) -> None:
        """Resolve ``__aenter__``'s wait, unless it already ended or was cancelled."""
        future = self._initial_connection_future
        if future is None or future.done():
            return
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    def _settle_supervisor_exit(self) -> None:
        self._settle_initial_connection(
            PHXConnectionError("Connection supervisor stopped before connecting")
        )
        self._connected_event.clear()
        if self._state != ClientState.SHUTTING_DOWN:
            self._deps._transition_state(ClientState.CLOSED)

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay_s)
        except TimeoutError:
            return

    async def _cleanup_connection(self) -> None:
        self._deps._fail_pending_joins(
            PHXConnectionError("Connection lost before the join completed")
        )
        connection = self.connection
        running = [
            task
            for task in (self._heartbeat_task, self._message_routing_task)
            if task is not None and not task.done()
        ]
        self.connection = None
        self._heartbeat_task = None
        self._message_routing_task = None
        self._pending_heartbeat_ref = None
        self._connected_event.clear()
        self._forced_close_pending = False

        try:
            await cancel_and_wait(*running)
        finally:
            # Close the socket even if the caller is cancelled meanwhile.
            if connection is not None:
                await self._close_websocket(connection)

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
        supervisor = self._supervisor_task
        if supervisor is None:
            raise PHXConnectionError(
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
            await self._deps.shutdown("Signal received")
        if self._terminal_error is not None:
            raise self._terminal_error
        if supervisor.cancelled() and self._shutdown_event.is_set():
            return  # a requested stop
        supervisor.result()
