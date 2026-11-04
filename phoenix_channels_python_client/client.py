from __future__ import annotations

import asyncio
import logging
import math
from types import TracebackType
from typing import Self, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from websockets import ClientConnection

from phoenix_channels_python_client.authentication import (
    API_KEY_PARAMETER,
    TOKEN_PARAMETER,
    AuthenticationHeader,
    normalize_headers,
    select_authentication_header,
)
from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import (
    ClientState,
    DisconnectCallback,
    HeartbeatAckCallback,
    ReconnectCallback,
    ReconnectDecider,
    ReconnectPolicy,
    TopicLostCallback,
)
from phoenix_channels_python_client.diagnostics import (
    ConnectionLogger,
    diagnostic_secrets,
)
from phoenix_channels_python_client.exceptions import SocketError
from phoenix_channels_python_client.protocol import (
    DEFAULT_PROTOCOL_VERSION,
    ProtocolVersion,
)
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.reconnect_controller import ReconnectControllerMixin
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import cancel_and_wait

logger = logging.getLogger(__name__)


def _build_channel_socket_urls(
    websocket_url: str,
    api_key: str | None,
    vsn: str,
    *,
    header_auth: AuthenticationHeader | None = None,
) -> tuple[str, str]:
    split_url = urlsplit(websocket_url)
    query_params = parse_qsl(split_url.query, keep_blank_values=True)
    if header_auth is AuthenticationHeader.API_KEY and any(
        key.casefold() == TOKEN_PARAMETER and value for key, value in query_params
    ):
        raise ValueError("token query authentication cannot be combined with x-api-key")
    replaced_parameters = {"vsn"}
    authentication_params: list[tuple[str, str]] = []
    if header_auth:
        replaced_parameters.update({API_KEY_PARAMETER, TOKEN_PARAMETER})
    elif api_key is not None:
        replaced_parameters.add(API_KEY_PARAMETER)
        authentication_params.append((API_KEY_PARAMETER, api_key))
    filtered = [
        (key, value)
        for key, value in query_params
        if key.casefold() not in replaced_parameters
    ]
    with_auth = [
        *filtered,
        *authentication_params,
        ("vsn", vsn),
    ]

    def build(params: list[tuple[str, str]]) -> str:
        return urlunsplit(
            (
                split_url.scheme,
                split_url.netloc,
                split_url.path,
                urlencode(params),
                split_url.fragment,
            )
        )

    return build(with_auth), build(
        [
            (
                key,
                "***"
                if key.casefold() in {API_KEY_PARAMETER, TOKEN_PARAMETER}
                else value,
            )
            for key, value in with_auth
        ]
    )


def _validate_heartbeat_threshold(
    interval: float | None, threshold: float | None
) -> None:
    if threshold is None:
        return
    if not math.isfinite(threshold) or threshold <= 0:
        raise ValueError("heartbeat_dead_threshold_s must be finite and > 0")
    if interval is not None and (not math.isfinite(interval) or threshold <= interval):
        raise ValueError(
            "heartbeat_dead_threshold_s must exceed the finite heartbeat interval"
        )


class Client(SupervisorMixin, TopicRuntimeMixin, ReconnectControllerMixin):
    def __init__(  # noqa: PLR0913, PLR0915  # the public options
        self,
        websocket_url: str,
        *,
        api_key: str | None = None,
        protocol_version: ProtocolVersion | str = DEFAULT_PROTOCOL_VERSION,
        auto_reconnect: bool = True,
        reconnect_policy: ReconnectPolicy | None = None,
        join_timeout_s: float = 10.0,
        leave_timeout_s: float = 5.0,
        max_topic_queue_size: int = 1000,
        callback_drain_timeout_s: float = 2.0,
        heartbeat_interval_s: float | None = 30.0,
        heartbeat_dead_threshold_s: float | None = None,
        on_reconnect: ReconnectCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
        on_heartbeat_ack: HeartbeatAckCallback | None = None,
        on_topic_lost: TopicLostCallback | None = None,
        reconnect_decider: ReconnectDecider | None = None,
        additional_headers: dict[str, str] | None = None,
    ) -> None:
        if api_key == "":
            raise ValueError("api_key must be nonempty or None")
        self.additional_headers = normalize_headers(additional_headers)
        self.logger = ConnectionLogger(
            logger, diagnostic_secrets(websocket_url, api_key, self.additional_headers)
        )

        if heartbeat_interval_s is not None and heartbeat_interval_s <= 0:
            raise ValueError("heartbeat_interval_s must be > 0 or None to disable")

        _validate_heartbeat_threshold(heartbeat_interval_s, heartbeat_dead_threshold_s)

        if join_timeout_s <= 0:
            raise ValueError("join_timeout_s must be > 0")
        if leave_timeout_s <= 0:
            raise ValueError("leave_timeout_s must be > 0")
        if max_topic_queue_size <= 0:
            raise ValueError("max_topic_queue_size must be > 0")
        if callback_drain_timeout_s <= 0:
            raise ValueError("callback_drain_timeout_s must be > 0")

        self.protocol_handler = PHXProtocolHandler(
            protocol_version, diagnostic_logger=self.logger
        )
        connect_url, redacted_url = _build_channel_socket_urls(
            websocket_url=websocket_url,
            api_key=api_key,
            vsn=self.protocol_handler.protocol_version.vsn,
            header_auth=select_authentication_header(self.additional_headers),
        )
        self.channel_socket_url = connect_url
        self.channel_socket_url_redacted = redacted_url

        self.auto_reconnect = auto_reconnect
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.join_timeout_s = join_timeout_s
        self.leave_timeout_s = leave_timeout_s
        self.max_topic_queue_size = max_topic_queue_size
        self.callback_drain_timeout_s = callback_drain_timeout_s

        self.connection: ClientConnection | None = None
        self.topic_subscriptions: dict[str, TopicSubscription] = {}
        self.ref_counter = 0
        self.state = ClientState.CLOSED
        self.topics_lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        self.connected_event = asyncio.Event()
        self.conn_generation = 0
        self.supervisor_task: asyncio.Task[None] | None = None
        self.shutdown_task: asyncio.Task[None] | None = None
        self.message_routing_task: asyncio.Task[None] | None = None
        self.initial_connection_future: asyncio.Future[None] | None = None
        self.terminal_error: Exception | None = None
        self.heartbeat_dead_threshold_s = heartbeat_dead_threshold_s
        self.heartbeat_deadline: float | None = None
        self.heartbeat_timeout: asyncio.Timeout | None = None
        self.heartbeat_interval_s = heartbeat_interval_s
        self.heartbeat_task: asyncio.Task[None] | None = None
        self.pending_heartbeat_ref: str | None = None
        self.on_reconnect = on_reconnect
        self.on_disconnect = on_disconnect
        self.on_heartbeat_ack = on_heartbeat_ack
        self.on_topic_lost = on_topic_lost
        self.reconnect_decider = reconnect_decider
        self.decider = self._new_decider()
        self.lifecycle_tasks: set[asyncio.Task[None]] = set()
        self.cleanup_tasks: set[asyncio.Task[None]] = set()
        self.forced_close_pending = False

    def redact_diagnostic(self, text: str) -> str:
        """Sanitize untrusted transport error text using this connection's policy."""
        return cast(ConnectionLogger, self.logger).redact(text)

    @property
    def active_shutdown(self) -> asyncio.Task[None] | None:
        task = self.shutdown_task
        return task if task is not None and not task.done() else None

    @property
    def is_fully_closed(self) -> bool:
        return (
            self.state == ClientState.CLOSED
            and not self.topic_subscriptions
            and self.connection is None
            and not self.cleanup_tasks
        )

    async def __aenter__(self) -> Self:
        self.logger.debug("Entering Client context")
        if self.state != ClientState.CLOSED or self.active_shutdown is not None:
            raise SocketError("Client is already running")

        self.shutdown_event.clear()
        self.connected_event.clear()
        self.decider = self._new_decider()
        self.terminal_error = None
        self.conn_generation = 0
        self.initial_connection_future = asyncio.get_running_loop().create_future()
        self._transition_state(ClientState.CONNECTING)

        self.supervisor_task = asyncio.create_task(self._supervisor_loop())

        try:
            await self.initial_connection_future
        except (Exception, asyncio.CancelledError):
            # Not BaseException: awaiting cleanup on GeneratorExit raises RuntimeError.
            await self.shutdown("Initial connection failed")
            raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.logger.debug("Leaving Client context")
        await self.shutdown("Leaving Client context")

    async def shutdown(self, reason: str = "Client shutdown") -> None:
        task = self.active_shutdown
        if task is None:
            if self.is_fully_closed:
                return
            self._request_stop()
            task = self.shutdown_task = asyncio.create_task(self._shutdown(reason))
        # Every caller waits for the same shutdown, and one caller's
        # cancellation never aborts it.
        await asyncio.shield(task)

    def _request_stop(self) -> None:
        # Runs inside the shutdown() call, not in its task: a refusal or close landing
        # before the task's first step must already see the stop.
        if self.state not in (ClientState.SHUTTING_DOWN, ClientState.CLOSED):
            self._transition_state(ClientState.SHUTTING_DOWN)
        self.shutdown_event.set()

    async def _shutdown(self, reason: str) -> None:
        self.logger.info("Event loop shutting down! reason=%s", reason)

        await self._unsubscribe_all()
        # Includes on_topic_lost callbacks still running for topics already gone.
        await cancel_and_wait(*self.lifecycle_tasks)
        if self.cleanup_tasks:
            await asyncio.gather(*self.cleanup_tasks)
        await self._stop_supervisor()
        await self._cleanup_connection()
        self.connected_event.clear()
        self._transition_state(ClientState.CLOSED)

    async def _unsubscribe_all(self) -> None:
        topics = list(self.topic_subscriptions)
        results = await asyncio.gather(
            *(
                self.unsubscribe_from_topic(topic, allow_disconnected=True)
                for topic in topics
            ),
            return_exceptions=True,
        )
        for topic, result in zip(topics, results, strict=True):
            if isinstance(result, Exception):
                self.logger.warning(
                    "Failed to unsubscribe from topic %s during shutdown: %s",
                    topic,
                    result,
                )

    async def _stop_supervisor(self) -> None:
        # Cleared, so run_forever() after the stop reports the client isn't running.
        supervisor, self.supervisor_task = self.supervisor_task, None
        if supervisor is not None and not supervisor.done():
            await cancel_and_wait(supervisor)

    def _transition_state(self, new_state: ClientState) -> None:
        if self.state == new_state:
            return

        transitioned_state = transition_client_state(self.state, new_state)
        self.logger.debug("Client state transition: %s -> %s", self.state, new_state)
        self.state = transitioned_state
