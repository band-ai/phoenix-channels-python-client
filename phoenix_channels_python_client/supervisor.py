from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Protocol, cast

from websockets import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectDecision,
    ReconnectPolicy,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import make_message


class _SupervisorRuntimeDeps(Protocol):
    async def _rejoin_topics(self, generation: int) -> None: ...
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
                        "Heartbeat response not received for ref=%s; server may be unresponsive",
                        self._pending_heartbeat_ref,
                    )

                ref = self._generate_ref()  # type: ignore[missing-attribute]  # provided by TopicRuntimeMixin
                self._pending_heartbeat_ref = ref

                heartbeat_message = make_message(
                    topic="phoenix",
                    event=Event("heartbeat"),
                    payload={},
                    ref=ref,
                )

                try:
                    await self._protocol_handler.send_message(
                        connection, heartbeat_message
                    )
                    self.logger.debug("Sent heartbeat (ref=%s)", ref)
                except Exception:
                    self.logger.debug(
                        "Failed to send heartbeat; connection likely closing"
                    )
                    break
        except asyncio.CancelledError:
            self.logger.debug("Heartbeat loop cancelled")
            raise

    async def _supervisor_loop(self) -> None:
        attempt = 0
        runtime_deps = cast(_SupervisorRuntimeDeps, self)

        try:
            while not self._shutdown_event.is_set():
                connected_since: float | None = None
                safe_channel_socket_url = getattr(
                    self,
                    "channel_socket_url_redacted",
                    self.channel_socket_url,
                )
                try:
                    connection = await connect(
                        self.channel_socket_url,
                        additional_headers=getattr(self, "additional_headers", None)
                        or None,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if not self.auto_reconnect:
                        if (
                            self._initial_connection_future
                            and not self._initial_connection_future.done()
                        ):
                            self._initial_connection_future.set_exception(
                                PHXConnectionError(
                                    "Failed to connect to "
                                    f"{safe_channel_socket_url}: {exc}"
                                )
                            )
                        self.logger.error(
                            "Connection failed and auto_reconnect=False: %s", exc
                        )
                        break

                    runtime_deps._record_disconnect(connection_uptime_s=0.0)
                    if runtime_deps._should_suppress_reconnect():
                        self._terminal_error = PHXConnectionError(
                            "Reconnect suppressed after repeated rapid disconnects. "
                            "Likely duplicate connection or unstable endpoint."
                        )
                        self.logger.error("%s", self._terminal_error)
                        break

                    if self._state == ClientState.CONNECTING:
                        runtime_deps._transition_state(ClientState.RECONNECTING)

                    delay = runtime_deps._compute_reconnect_delay(attempt=attempt)
                    attempt += 1
                    await self._wait_for_shutdown_or_timeout(delay)
                    continue

                self.connection = connection
                self._conn_generation += 1
                generation = self._conn_generation
                self._connected_event.set()
                runtime_deps._transition_state(ClientState.CONNECTED)
                connected_since = asyncio.get_running_loop().time()

                self._pending_heartbeat_ref = None
                self._message_routing_task = asyncio.create_task(
                    self._start_processing(connection, generation)
                )
                if self._heartbeat_interval_s is not None:
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(connection)
                    )

                try:
                    await runtime_deps._rejoin_topics(generation)
                except Exception:
                    self.logger.exception("Unexpected error while rejoining topics")

                if (
                    self._initial_connection_future
                    and not self._initial_connection_future.done()
                ):
                    self._initial_connection_future.set_result(None)

                if generation > 1 and self._on_reconnect is not None:
                    try:
                        await self._on_reconnect()
                    except Exception:
                        self.logger.exception("Error in on_reconnect callback")

                try:
                    await self._message_routing_task
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if isinstance(exc, ConnectionClosed):
                        self.logger.info(
                            "Message routing stopped due to websocket close code=%s reason=%s",
                            exc.rcvd.code if exc.rcvd is not None else None,
                            exc.rcvd.reason if exc.rcvd is not None else "",
                        )
                    else:
                        self.logger.exception("Message routing task failed")
                    routing_error = exc
                else:
                    routing_error = None

                close_code, close_reason = runtime_deps._extract_close_details(
                    connection=connection,
                    routing_error=routing_error,
                )

                await self._cleanup_connection()

                if self._on_disconnect is not None:
                    try:
                        await self._on_disconnect(routing_error)
                    except Exception:
                        self.logger.exception("Error in on_disconnect callback")

                if self._shutdown_event.is_set():
                    break

                if not self.auto_reconnect:
                    break

                decision = runtime_deps._classify_disconnect(close_code, close_reason)
                if decision.terminal_error is not None:
                    self._terminal_error = decision.terminal_error
                    self.logger.error("%s", self._terminal_error)
                    break

                if not decision.should_reconnect:
                    self.logger.info(
                        "Reconnect disabled for close code %s reason=%s",
                        close_code,
                        close_reason,
                    )
                    break

                uptime = 0.0
                if connected_since is not None:
                    uptime = asyncio.get_running_loop().time() - connected_since

                runtime_deps._record_disconnect(connection_uptime_s=uptime)
                if runtime_deps._should_suppress_reconnect():
                    self._terminal_error = PHXConnectionError(
                        "Reconnect suppressed after repeated rapid disconnects. "
                        "Likely duplicate connection or unstable endpoint."
                    )
                    self.logger.error("%s", self._terminal_error)
                    break

                if uptime >= self.reconnect_policy.stable_reset_s:
                    attempt = 0
                    self._rapid_disconnects.clear()
                else:
                    attempt += 1

                runtime_deps._transition_state(ClientState.RECONNECTING)
                delay = runtime_deps._compute_reconnect_delay(attempt=attempt)
                delay = runtime_deps._apply_disconnect_delay_override(delay, decision)
                await self._wait_for_shutdown_or_timeout(delay)

            if (
                self._initial_connection_future
                and not self._initial_connection_future.done()
            ):
                self._initial_connection_future.set_exception(
                    PHXConnectionError(
                        "Connection supervisor stopped before connecting"
                    )
                )

        finally:
            self._connected_event.clear()
            if self._state != ClientState.SHUTTING_DOWN:
                runtime_deps._transition_state(ClientState.CLOSED)

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay_s)
        except asyncio.TimeoutError:
            return

    async def _cleanup_connection(self) -> None:
        connection = self.connection
        self.connection = None
        self._connected_event.clear()

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._heartbeat_task
        self._heartbeat_task = None
        self._pending_heartbeat_ref = None

        if self._message_routing_task and not self._message_routing_task.done():
            self._message_routing_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._message_routing_task

        self._message_routing_task = None

        if connection is not None:
            try:
                await connection.close()
            except Exception:
                self.logger.exception("Failed while closing websocket connection")

    async def run_forever(self) -> None:
        runtime_deps = cast(_SupervisorRuntimeDeps, self)
        if self._supervisor_task is None:
            raise PHXConnectionError(
                "Client is not connected. Use 'async with' context manager."
            )

        shutdown_signal = asyncio.Event()

        def signal_handler(*_: object) -> None:
            shutdown_signal.set()

        loop = asyncio.get_running_loop()
        signal_handlers_registered = False
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, signal_handler)
            signal_handlers_registered = True
        except (RuntimeError, NotImplementedError):
            self.logger.debug(
                "Signal handlers not available (not running on main thread)"
            )

        shutdown_task = asyncio.create_task(shutdown_signal.wait())

        try:
            done, pending = await asyncio.wait(
                [shutdown_task, self._supervisor_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

            if shutdown_task in done:
                await runtime_deps.shutdown("Signal received")
                return

            # Supervisor task ended unexpectedly: propagate if it failed.
            if self._terminal_error is not None:
                raise self._terminal_error
            self._supervisor_task.result()

        finally:
            if signal_handlers_registered:
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.remove_signal_handler(sig)
