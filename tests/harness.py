"""Stand-ins for driving the client's internals directly.

They reach races and defensive branches the fake server can't. Prefer the fake server.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from websockets import ClientConnection

from phoenix_channels_python_client.client_types import (
    PROCEED,
    ClientState,
    Connected,
    ConnectFailed,
    Connecting,
    Disconnected,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from tests.support import TOPIC

# Short, so harness joins and leaves that get no reply time out promptly.
HARNESS_TIMEOUT_S = 0.01

# The delay of every retry the harness decides.
HARNESS_RECONNECT_DELAY_S = 0.001
HARNESS_RETRY = ReconnectDecision(
    should_reconnect=True, delay_s=HARNESS_RECONNECT_DELAY_S
)

DEFAULT_JOIN_REF = "1"


def make_subscription(
    name: str = TOPIC, join_ref: str = DEFAULT_JOIN_REF, **options: Any
) -> TopicSubscription:
    options.setdefault("queue", asyncio.Queue())
    options.setdefault("established", True)
    options.setdefault("conn_generation", 1)
    return TopicSubscription(
        name=name,
        async_callback=None,
        join_ref=join_ref,
        process_topic_messages_task=None,
        **options,
    )


@dataclass
class FakeSocket:
    close_raises: bool = False

    async def close(self, code: int | None = None, reason: str = "") -> None:
        del code, reason
        if self.close_raises:
            raise RuntimeError("close boom")


def fake_connection(socket: FakeSocket | None = None) -> ClientConnection:
    return cast(ClientConnection, socket or FakeSocket())


class FakeRoutingProtocolHandler:
    """Ends each connection as soon as its routing starts."""

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
        **kwargs: Any,
    ) -> None:
        del connection, subscriptions, conn_generation, kwargs


class FakeTopicProtocolHandler(PHXProtocolHandler):
    """Records what would be sent, so joins and leaves get no reply."""

    def __init__(self) -> None:
        super().__init__(ProtocolVersion.V2)
        self.raise_on_send: Exception | None = None
        self.sent: list[Message] = []

    async def send_message(self, websocket: ClientConnection, message: Message) -> None:
        del websocket
        if self.raise_on_send is not None:
            raise self.raise_on_send
        self.sent.append(message)


class TopicRuntimeHarness(TopicRuntimeMixin):
    """Runs the real topic runtime; joins and leaves get no reply.

    A channel recovery waits no backoff, and the client shuts down on the
    ``waits_before_stop``-th backoff it would have waited.
    """

    def __init__(
        self,
        *,
        on_topic_lost: Callable[[str, Exception], Awaitable[None]] | None = None,
        waits_before_stop: int = 1,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.connection: ClientConnection | None = fake_connection()
        self.state = ClientState.CONNECTED
        self.ref_counter = 0
        self.conn_generation = 1
        self.topic_subscriptions: dict[str, TopicSubscription] = {}
        self.fake_handler = FakeTopicProtocolHandler()
        self.protocol_handler = self.fake_handler
        self.topics_lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        self.join_timeout_s = HARNESS_TIMEOUT_S
        self.leave_timeout_s = HARNESS_TIMEOUT_S
        self.max_topic_queue_size = 10
        self.callback_drain_timeout_s = HARNESS_TIMEOUT_S
        self.on_topic_lost = on_topic_lost
        self.lifecycle_tasks: set[asyncio.Task[None]] = set()
        self.cleanup_tasks: set[asyncio.Task[None]] = set()
        # The attempt each requested channel-rejoin backoff was for, in order.
        self.rejoin_backoff_attempts: list[int] = []
        self.waits_before_stop = waits_before_stop

    def _channel_rejoin_delay(self, attempt: int) -> float:
        self.rejoin_backoff_attempts.append(attempt)
        return 0.0

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        del delay_s
        if len(self.rejoin_backoff_attempts) >= self.waits_before_stop:
            self.shutdown_event.set()

    async def _invoke_callback_safely(
        self, label: str, callback: Callable[..., Awaitable[None]], *args: object
    ) -> None:
        del label
        await callback(*args)

    def register(self, subscription: TopicSubscription) -> TopicSubscription:
        self.topic_subscriptions[subscription.name] = subscription
        return subscription


class SupervisorHarness(SupervisorMixin):
    """Runs the real supervisor loop with every collaborator stubbed out."""

    # Starting state is passed in rather than assigned by the test, which would
    # narrow its type for the rest of the test.
    def __init__(
        self,
        *,
        pending_heartbeat_ref: str | None = None,
        forced_close_pending: bool = False,
        close_code: int | None = None,
        connect_failures: int = 0,
        waits_before_stop: int = 1,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.channel_socket_url = "ws://unit-test/socket"
        self.channel_socket_url_redacted = "ws://unit-test/socket?api_key=***"
        self.auto_reconnect = True
        self.connection: ClientConnection | None = None
        self.topic_subscriptions: dict[str, TopicSubscription] = {}
        self.protocol_handler: Any = FakeRoutingProtocolHandler()
        self.shutdown_event = asyncio.Event()
        self.connected_event = asyncio.Event()
        self.conn_generation = 0
        self.state = ClientState.CONNECTING
        self.supervisor_task: asyncio.Task[None] | None = None
        self.message_routing_task: asyncio.Task[None] | None = None
        self.initial_connection_future: asyncio.Future[None] | None = (
            asyncio.get_running_loop().create_future()
        )
        self.terminal_error: Exception | None = None
        self.heartbeat_dead_threshold_s: float | None = None
        self.heartbeat_deadline: float | None = None
        self.heartbeat_timeout: asyncio.Timeout | None = None
        self.heartbeat_interval_s: float | None = None
        self.heartbeat_task: asyncio.Task[None] | None = None
        self.pending_heartbeat_ref = pending_heartbeat_ref
        self.ref_counter = 0
        self.on_reconnect = None
        self.on_disconnect = None
        self.on_heartbeat_ack = None
        self.forced_close_pending = forced_close_pending
        self.close_code = close_code

        self.connect_attempts = 0
        self.wait_delays: list[float] = []
        self.connect_failures = connect_failures
        self.waits_before_stop = waits_before_stop
        self.disconnect_decision = HARNESS_RETRY
        self.connect_failure_decision = HARNESS_RETRY
        self.decided_events: list[ReconnectEvent] = []
        self.rejoin_error: Exception | None = None

    async def _connect(self) -> ClientConnection:
        self.connect_attempts += 1
        if self.connect_attempts <= self.connect_failures:
            # A fresh error each time; a shared one would keep the old traceback.
            raise ConnectionRefusedError
        return fake_connection()

    async def _rejoin_topics(self, generation: int) -> None:
        del generation
        if self.rejoin_error is not None:
            raise self.rejoin_error

    def _fail_pending_joins(self, error: Exception) -> None:
        del error

    def _channel_recoveries(self) -> list[asyncio.Task[None]]:
        return []

    def _extract_close_details(
        self, *, connection: ClientConnection, routing_error: Exception | None
    ) -> tuple[int | None, str]:
        del connection, routing_error
        return self.close_code, ""

    def _decide(self, event: ReconnectEvent) -> ReconnectDecision:
        self.decided_events.append(event)
        decisions: dict[type[ReconnectEvent], ReconnectDecision] = {
            Connecting: PROCEED,
            Connected: PROCEED,
            Disconnected: self.disconnect_decision,
            ConnectFailed: self.connect_failure_decision,
        }
        return decisions[type(event)]

    def _transition_state(self, new_state: ClientState) -> None:
        self.state = new_state

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        self.wait_delays.append(delay_s)
        if len(self.wait_delays) >= self.waits_before_stop:
            self.shutdown_event.set()

    async def shutdown(self, reason: str) -> None:
        del reason
        self.shutdown_event.set()
