from __future__ import annotations

import asyncio
import logging
from collections import deque
from types import TracebackType

from websockets import ClientConnection

from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectPolicy,
    reconnect_policy_is_invalid,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.reconnect_controller import ReconnectControllerMixin
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription

logger = logging.getLogger(__name__)


class PHXChannelsClient(SupervisorMixin, TopicRuntimeMixin, ReconnectControllerMixin):
    def __init__(
        self,
        websocket_url: str,
        api_key: str,
        *,
        protocol_version: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
        auto_reconnect: bool = True,
        reconnect_policy: ReconnectPolicy | None = None,
        join_timeout_s: float = 10.0,
        leave_timeout_s: float = 5.0,
        max_topic_queue_size: int = 1000,
        callback_drain_timeout_s: float = 2.0,
    ):
        self.logger = logger

        if join_timeout_s <= 0:
            raise ValueError("join_timeout_s must be > 0")
        if leave_timeout_s <= 0:
            raise ValueError("leave_timeout_s must be > 0")
        if max_topic_queue_size <= 0:
            raise ValueError("max_topic_queue_size must be > 0")
        if callback_drain_timeout_s <= 0:
            raise ValueError("callback_drain_timeout_s must be > 0")
        if reconnect_policy_is_invalid(reconnect_policy or ReconnectPolicy()):
            raise ValueError("Invalid reconnect policy configuration")

        vsn = (
            "2.0.0"
            if protocol_version == PhoenixChannelsProtocolVersion.V2
            else "1.0.0"
        )
        self.channel_socket_url = f"{websocket_url}?api_key={api_key}&vsn={vsn}"

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
        self._message_routing_task: asyncio.Task[None] | None = None
        self._initial_connection_future: asyncio.Future[None] | None = None
        self._rapid_disconnects: deque[float] = deque()
        self._terminal_error: Exception | None = None

    @staticmethod
    def reconnect_policy_is_invalid(policy: ReconnectPolicy) -> bool:
        return reconnect_policy_is_invalid(policy)

    async def __aenter__(self) -> PHXChannelsClient:
        self.logger.debug("Entering PHXChannelsClient context")
        if self._state != ClientState.CLOSED:
            raise PHXConnectionError("Client is already running")

        self._shutdown_event.clear()
        self._connected_event.clear()
        self._rapid_disconnects.clear()
        self._terminal_error = None
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

    async def shutdown(
        self,
        reason: str,
    ) -> None:
        if (
            self._state == ClientState.CLOSED
            and not self._topic_subscriptions
            and self.connection is None
        ):
            return

        self.logger.info("Event loop shutting down! reason=%s", reason)

        if self._state not in (ClientState.SHUTTING_DOWN, ClientState.CLOSED):
            self._transition_state(ClientState.SHUTTING_DOWN)

        self._shutdown_event.set()

        topics_to_unsubscribe = list(self._topic_subscriptions.keys())
        if topics_to_unsubscribe:
            unsubscribe_tasks = [
                self.unsubscribe_from_topic(topic, _allow_disconnected=True)
                for topic in topics_to_unsubscribe
            ]
            results = await asyncio.gather(*unsubscribe_tasks, return_exceptions=True)

            for topic, result in zip(topics_to_unsubscribe, results, strict=False):
                if isinstance(result, Exception):
                    self.logger.warning(
                        "Failed to unsubscribe from topic %s during shutdown: %s",
                        topic,
                        result,
                    )

        if self._supervisor_task and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                self.logger.debug("Supervisor task cancelled during shutdown")

        await self._cleanup_connection()
        self._connected_event.clear()
        self._transition_state(ClientState.CLOSED)

    def _transition_state(self, new_state: ClientState) -> None:
        if self._state == new_state:
            return

        transitioned_state = transition_client_state(self._state, new_state)
        self.logger.debug(
            "Client state transition: %s -> %s", self._state.value, new_state.value
        )
        self._state = transitioned_state
