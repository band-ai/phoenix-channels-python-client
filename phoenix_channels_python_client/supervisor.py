from __future__ import annotations

import asyncio
import logging
import signal
from collections import deque
from contextlib import suppress
from typing import Any

from websockets import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.client_types import ClientState, ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.topic_subscription import TopicSubscription


class SupervisorMixin:
    logger: logging.Logger
    channel_socket_url: str
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

    _rejoin_topics: Any
    _record_disconnect: Any
    _should_suppress_reconnect: Any
    _compute_reconnect_delay: Any
    _extract_close_details: Any
    _classify_disconnect: Any
    _apply_disconnect_delay_override: Any
    _transition_state: Any
    shutdown: Any

    async def _start_processing(
        self, connection: ClientConnection, conn_generation: int
    ) -> None:
        await self._protocol_handler.process_websocket_messages(
            connection,
            self._topic_subscriptions,
            conn_generation,
        )

    async def _supervisor_loop(self) -> None:
        attempt = 0

        try:
            while not self._shutdown_event.is_set():
                connected_since: float | None = None
                try:
                    connection = await connect(self.channel_socket_url)
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
                                    f"Failed to connect to {self.channel_socket_url}: {exc}"
                                )
                            )
                        self.logger.error(
                            "Connection failed and auto_reconnect=False: %s", exc
                        )
                        break

                    self._record_disconnect(connection_uptime_s=0.0)
                    if self._should_suppress_reconnect():
                        self._terminal_error = PHXConnectionError(
                            "Reconnect suppressed after repeated rapid disconnects. "
                            "Likely duplicate connection or unstable endpoint."
                        )
                        self.logger.error("%s", self._terminal_error)
                        break

                    if self._state == ClientState.CONNECTING:
                        self._transition_state(ClientState.RECONNECTING)

                    delay = self._compute_reconnect_delay(attempt=attempt)
                    attempt += 1
                    await self._wait_for_shutdown_or_timeout(delay)
                    continue

                self.connection = connection
                self._conn_generation += 1
                generation = self._conn_generation
                self._connected_event.set()
                self._transition_state(ClientState.CONNECTED)
                connected_since = asyncio.get_running_loop().time()

                self._message_routing_task = asyncio.create_task(
                    self._start_processing(connection, generation)
                )

                try:
                    await self._rejoin_topics(generation)
                except Exception:
                    self.logger.exception("Unexpected error while rejoining topics")

                if (
                    self._initial_connection_future
                    and not self._initial_connection_future.done()
                ):
                    self._initial_connection_future.set_result(None)

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

                close_code, close_reason = self._extract_close_details(
                    connection=connection,
                    routing_error=routing_error,
                )

                await self._cleanup_connection()

                if self._shutdown_event.is_set():
                    break

                if not self.auto_reconnect:
                    break

                decision = self._classify_disconnect(close_code, close_reason)
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

                self._record_disconnect(connection_uptime_s=uptime)
                if self._should_suppress_reconnect():
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

                self._transition_state(ClientState.RECONNECTING)
                delay = self._compute_reconnect_delay(attempt=attempt)
                delay = self._apply_disconnect_delay_override(delay, decision)
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
                self._transition_state(ClientState.CLOSED)

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay_s)
        except asyncio.TimeoutError:
            return

    async def _cleanup_connection(self) -> None:
        connection = self.connection
        self.connection = None
        self._connected_event.clear()

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
        if self._supervisor_task is None:
            raise PHXConnectionError(
                "Client is not connected. Use 'async with' context manager."
            )

        shutdown_signal = asyncio.Event()

        def signal_handler(*_: object) -> None:
            shutdown_signal.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, signal_handler)

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
                await self.shutdown("Signal received")
                return

            # Supervisor task ended unexpectedly: propagate if it failed.
            if self._terminal_error is not None:
                raise self._terminal_error
            self._supervisor_task.result()

        finally:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)
