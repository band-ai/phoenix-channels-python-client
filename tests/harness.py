"""Stand-ins for driving the client's internals directly, for races and
defensive branches the fake server can't trigger. Prefer the fake server.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, cast

from websockets import ClientConnection

from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectDecision,
    ReconnectPolicy,
)
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from tests.support import TOPIC

# Short, so harness joins and leaves that get no reply time out promptly.
HARNESS_TIMEOUT_S = 0.01

# What the harness's stubbed reconnect delay returns.
HARNESS_RECONNECT_DELAY_S = 0.001

# A connection up at least this long counts as stable; short, to keep tests fast.
HARNESS_STABLE_RESET_S = 0.01

DEFAULT_JOIN_REF = "1"


def make_subscription(
    name: str = TOPIC, join_ref: str = DEFAULT_JOIN_REF, **options: Any
) -> TopicSubscription:
    options.setdefault("queue", asyncio.Queue())
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
        if self.close_raises:
            raise RuntimeError("close boom")


def fake_connection(socket: FakeSocket | None = None) -> ClientConnection:
    return cast(ClientConnection, socket or FakeSocket())


async def connect_to(url: str) -> ClientConnection:
    """A stand-in for ``websockets.connect`` that always succeeds."""
    del url
    return fake_connection()


class FakeRoutingProtocolHandler:
    """Keeps each connection up for ``uptime_s``, then ends it with ``error``."""

    def __init__(self, error: Exception | None = None, uptime_s: float = 0.0) -> None:
        self.error = error
        self.uptime_s = uptime_s

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
        **kwargs: Any,
    ) -> None:
        del connection, subscriptions, conn_generation, kwargs
        await asyncio.sleep(self.uptime_s)
        if self.error is not None:
            raise self.error


class FakeTopicProtocolHandler(PHXProtocolHandler):
    """Records what would be sent, so joins and leaves get no reply."""

    def __init__(self) -> None:
        super().__init__(PhoenixChannelsProtocolVersion.V2)
        self.raise_on_send: Exception | None = None
        self.sent: list[ChannelMessage] = []

    async def send_message(
        self, websocket: ClientConnection, message: ChannelMessage
    ) -> None:
        del websocket
        if self.raise_on_send is not None:
            raise self.raise_on_send
        self.sent.append(message)


class TopicRuntimeHarness(TopicRuntimeMixin):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.connection: ClientConnection | None = fake_connection()
        self._state = ClientState.CONNECTED
        self._ref_counter = 0
        self._conn_generation = 1
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self.fake_handler = FakeTopicProtocolHandler()
        self._protocol_handler = self.fake_handler
        self._topics_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self.join_timeout_s = HARNESS_TIMEOUT_S
        self.leave_timeout_s = HARNESS_TIMEOUT_S
        self.max_topic_queue_size = 10
        self.callback_drain_timeout_s = HARNESS_TIMEOUT_S

    def register(self, subscription: TopicSubscription) -> TopicSubscription:
        self._topic_subscriptions[subscription.name] = subscription
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
        connection_uptime_s: float = 0.0,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.channel_socket_url = "ws://unit-test/socket"
        self.channel_socket_url_redacted = "ws://unit-test/socket?api_key=***"
        self.auto_reconnect = True
        self.reconnect_policy = ReconnectPolicy(stable_reset_s=HARNESS_STABLE_RESET_S)
        self.connection: ClientConnection | None = None
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self._protocol_handler: Any = FakeRoutingProtocolHandler(
            uptime_s=connection_uptime_s
        )
        self._shutdown_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._conn_generation = 0
        self._state = ClientState.CONNECTING
        self._supervisor_task: asyncio.Task[None] | None = None
        self._message_routing_task: asyncio.Task[None] | None = None
        self._initial_connection_future: asyncio.Future[None] | None = (
            asyncio.get_running_loop().create_future()
        )
        self._rapid_disconnects = deque[float]()
        self._terminal_error: Exception | None = None
        self._heartbeat_interval_s: float | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending_heartbeat_ref = pending_heartbeat_ref
        self._ref_counter = 0
        self._on_reconnect = None
        self._on_disconnect = None
        self._on_heartbeat_ack = None
        self._forced_close_pending = forced_close_pending

        self.wait_delays: list[float] = []
        self.suppress_values: list[bool] = []
        self.disconnect_decision = ReconnectDecision(should_reconnect=True)
        self.rejoin_error: Exception | None = None

    async def _rejoin_topics(self, generation: int) -> None:
        del generation
        if self.rejoin_error is not None:
            raise self.rejoin_error

    def _fail_pending_joins(self, error: Exception) -> None:
        del error

    def _record_disconnect(self, connection_uptime_s: float) -> None:
        del connection_uptime_s

    def _should_suppress_reconnect(self) -> bool:
        if self.suppress_values:
            return self.suppress_values.pop(0)
        return False

    def _compute_reconnect_delay(self, attempt: int) -> float:
        del attempt
        return HARNESS_RECONNECT_DELAY_S

    def _extract_close_details(
        self, *, connection: ClientConnection, routing_error: Exception | None
    ) -> tuple[int | None, str]:
        del connection, routing_error
        return None, ""

    def _classify_disconnect(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision:
        del close_code, close_reason
        return self.disconnect_decision

    def _apply_disconnect_delay_override(
        self, computed_delay_s: float, decision: ReconnectDecision
    ) -> float:
        del decision
        return computed_delay_s

    def _transition_state(self, new_state: ClientState) -> None:
        self._state = new_state

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        self.wait_delays.append(delay_s)
        self._shutdown_event.set()

    async def shutdown(self, reason: str) -> None:
        del reason
        self._shutdown_event.set()
