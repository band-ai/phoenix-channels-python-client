from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Self
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from websockets import ClientConnection

from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import ClientState, ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.protocol_handler import (
    DEFAULT_PROTOCOL_VERSION,
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.reconnect_controller import ReconnectControllerMixin
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import cancel_and_wait

logger = logging.getLogger(__name__)

ReconnectCallback = Callable[[], Awaitable[None]]
DisconnectCallback = Callable[[Exception | None], Awaitable[None]]
# Must be synchronous and fast: unlike the callbacks above, this runs inline
# on the message-routing hot path, so blocking work here stalls heartbeat
# sends and message routing for every connection.
HeartbeatAckCallback = Callable[[], None]


def _build_channel_socket_urls(
    websocket_url: str, api_key: str, vsn: str
) -> tuple[str, str]:
    split_url = urlsplit(websocket_url)
    query_params = parse_qsl(split_url.query, keep_blank_values=True)
    filtered = [
        (key, value) for key, value in query_params if key not in {"api_key", "vsn"}
    ]
    with_auth = [*filtered, ("api_key", api_key), ("vsn", vsn)]

    connect_url = urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(with_auth),
            split_url.fragment,
        )
    )
    redacted_url = urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(
                [
                    (key, "***" if key == "api_key" else value)
                    for key, value in with_auth
                ]
            ),
            split_url.fragment,
        )
    )
    return connect_url, redacted_url


class PHXChannelsClient(SupervisorMixin, TopicRuntimeMixin, ReconnectControllerMixin):
    def __init__(  # noqa: PLR0913  # the public options
        self,
        websocket_url: str,
        api_key: str,
        *,
        protocol_version: PhoenixChannelsProtocolVersion = DEFAULT_PROTOCOL_VERSION,
        auto_reconnect: bool = True,
        reconnect_policy: ReconnectPolicy | None = None,
        join_timeout_s: float = 10.0,
        leave_timeout_s: float = 5.0,
        max_topic_queue_size: int = 1000,
        callback_drain_timeout_s: float = 2.0,
        heartbeat_interval_s: float | None = 30.0,
        on_reconnect: ReconnectCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
        on_heartbeat_ack: HeartbeatAckCallback | None = None,
        additional_headers: dict[str, str] | None = None,
    ) -> None:
        self.logger = logger

        if heartbeat_interval_s is not None and heartbeat_interval_s <= 0:
            raise ValueError("heartbeat_interval_s must be > 0 or None to disable")

        if join_timeout_s <= 0:
            raise ValueError("join_timeout_s must be > 0")
        if leave_timeout_s <= 0:
            raise ValueError("leave_timeout_s must be > 0")
        if max_topic_queue_size <= 0:
            raise ValueError("max_topic_queue_size must be > 0")
        if callback_drain_timeout_s <= 0:
            raise ValueError("callback_drain_timeout_s must be > 0")

        vsn = (
            "2.0.0"
            if protocol_version == PhoenixChannelsProtocolVersion.V2
            else "1.0.0"
        )
        connect_url, redacted_url = _build_channel_socket_urls(
            websocket_url=websocket_url,
            api_key=api_key,
            vsn=vsn,
        )
        self.channel_socket_url = connect_url
        self.channel_socket_url_redacted = redacted_url

        # Extra WebSocket handshake headers, re-sent on every (re)connect. Lets a
        # caller send the API key as an ``x-api-key`` header instead of (or
        # alongside) the URL query, so a trusted proxy can inject/replace it
        # in-header and the credential stays out of URL/proxy logs.
        self.additional_headers = dict(additional_headers or {})

        self.auto_reconnect = auto_reconnect
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self.join_timeout_s = join_timeout_s
        self.leave_timeout_s = leave_timeout_s
        self.max_topic_queue_size = max_topic_queue_size
        self.callback_drain_timeout_s = callback_drain_timeout_s

        self.connection: ClientConnection | None = None
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self._protocol_handler = PHXProtocolHandler(protocol_version)
        self._ref_counter = 0
        self._state = ClientState.CLOSED
        self._topics_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._conn_generation = 0
        self._supervisor_task: asyncio.Task[None] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self._message_routing_task: asyncio.Task[None] | None = None
        self._initial_connection_future: asyncio.Future[None] | None = None
        self._rapid_disconnects: deque[float] = deque()
        self._terminal_error: Exception | None = None
        self._heartbeat_interval_s = heartbeat_interval_s
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending_heartbeat_ref: str | None = None
        self._on_reconnect = on_reconnect
        self._on_disconnect = on_disconnect
        self._on_heartbeat_ack = on_heartbeat_ack
        self._forced_close_pending = False

    @property
    def _active_shutdown(self) -> asyncio.Task[None] | None:
        task = self._shutdown_task
        return task if task is not None and not task.done() else None

    @property
    def _is_fully_closed(self) -> bool:
        return (
            self._state == ClientState.CLOSED
            and not self._topic_subscriptions
            and self.connection is None
        )

    async def __aenter__(self) -> Self:
        self.logger.debug("Entering PHXChannelsClient context")
        if self._state != ClientState.CLOSED or self._active_shutdown is not None:
            raise PHXConnectionError("Client is already running")

        self._shutdown_event.clear()
        self._connected_event.clear()
        self._rapid_disconnects.clear()
        self._terminal_error = None
        self._conn_generation = 0
        self._initial_connection_future = asyncio.get_running_loop().create_future()
        self._transition_state(ClientState.CONNECTING)

        self._supervisor_task = asyncio.create_task(self._supervisor_loop())

        try:
            await self._initial_connection_future
        except Exception:
            await self.shutdown("Initial connection failed")
            raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        self.logger.debug("Leaving PHXChannelsClient context")
        await self.shutdown("Leaving PHXChannelsClient context")

    async def shutdown(self, reason: str) -> None:
        task = self._active_shutdown
        if task is None:
            if self._is_fully_closed:
                return
            task = self._shutdown_task = asyncio.create_task(self._shutdown(reason))
        # Every caller waits for the same shutdown, and one caller's
        # cancellation never aborts it.
        await asyncio.shield(task)

    async def _shutdown(self, reason: str) -> None:
        self.logger.info("Event loop shutting down! reason=%s", reason)

        if self._state not in (ClientState.SHUTTING_DOWN, ClientState.CLOSED):
            self._transition_state(ClientState.SHUTTING_DOWN)
        self._shutdown_event.set()

        await self._unsubscribe_all()
        await self._stop_supervisor()
        await self._cleanup_connection()
        self._connected_event.clear()
        self._transition_state(ClientState.CLOSED)

    async def _unsubscribe_all(self) -> None:
        topics = list(self._topic_subscriptions)
        results = await asyncio.gather(
            *(
                self.unsubscribe_from_topic(topic, _allow_disconnected=True)
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
        supervisor, self._supervisor_task = self._supervisor_task, None
        if supervisor is not None and not supervisor.done():
            await cancel_and_wait(supervisor)

    def _transition_state(self, new_state: ClientState) -> None:
        if self._state == new_state:
            return

        transitioned_state = transition_client_state(self._state, new_state)
        self.logger.debug("Client state transition: %s -> %s", self._state, new_state)
        self._state = transitioned_state
