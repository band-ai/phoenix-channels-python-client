from __future__ import annotations

import asyncio
from asyncio import Future, Queue, Task
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from websockets import ClientConnection

from phoenix_channels_python_client.client_types import JoinResponseCallback
from phoenix_channels_python_client.messages import (
    CLIENT_LIFECYCLE_EVENTS,
    ChannelEvent,
    Message,
)
from phoenix_channels_python_client.replies import Reply
from phoenix_channels_python_client.send_session import SessionSends


class ChannelState(StrEnum):
    CREATED = "created"
    JOINING = "joining"
    JOINED = "joined"
    REJOINING = "rejoining"
    LEFT = "left"


class ChannelCause(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    CONFIGURATION = "configuration"
    CRASH = "crash"
    CLOSE = "close"
    TRANSPORT = "transport"
    TIMEOUT = "timeout"
    LEAVE = "leave"
    STOP = "stop"


JoinParams = dict[str, Any] | Callable[[], dict[str, Any]] | None


@dataclass()
class TopicSubscription:
    """A topic subscription and everything needed to handle its messages."""

    name: str
    async_callback: Callable[[Message], Awaitable[None]] | None
    queue: Queue[Message]
    join_ref: str
    process_topic_messages_task: Task[None] | None
    current_join_ready_future: Future[None] | None = field(default=None, init=False)
    unsubscribe_completed_future: Future[None] | None = field(default=None, init=False)
    state: ChannelState = ChannelState.CREATED
    cause: ChannelCause | None = None
    params: JoinParams = None
    connection: ClientConnection | None = None
    join_submitted: bool = False
    terminal_received: asyncio.Event = field(default_factory=asyncio.Event)
    leave_task: Task[None] | None = None
    pending_pushes: dict[str, Future[Reply]] = field(default_factory=dict)
    send_outcomes: set[Future[Any]] = field(default_factory=set)
    send_session: SessionSends | None = None
    cleanup_done: bool = False
    loss_reported: bool = False
    leave_requested: asyncio.Event = field(default_factory=asyncio.Event)
    event_handlers: dict[ChannelEvent, Callable[[dict[str, Any]], Awaitable[None]]] = (
        field(default_factory=dict)
    )
    on_join_response: JoinResponseCallback | None = None
    blocks_reconnect: bool = True
    established: bool = False
    leave_ref: str | None = None
    conn_generation: int = 0
    dropped_message_count: int = 0
    current_callback_task: Task[None] | None = None
    # A rejoin sets this before draining; the topic task then takes no new message.
    restart_requested: bool = False
    # Rejoins the channel after a crash, while the socket stays up.
    recovery_task: Task[None] | None = None

    @property
    def current_join_ready(self) -> Future[None]:
        if self.current_join_ready_future is None:
            self.current_join_ready_future = asyncio.get_running_loop().create_future()
        return self.current_join_ready_future

    @current_join_ready.setter
    def current_join_ready(self, value: Future[None]) -> None:
        self.current_join_ready_future = value

    @property
    def unsubscribe_completed(self) -> Future[None]:
        if self.unsubscribe_completed_future is None:
            self.unsubscribe_completed_future = (
                asyncio.get_running_loop().create_future()
            )
        return self.unsubscribe_completed_future

    def add_event_handler(
        self, event: ChannelEvent, handler: Callable[[dict[str, Any]], Awaitable[None]]
    ) -> None:
        if event in CLIENT_LIFECYCLE_EVENTS:
            raise ValueError(
                f"{event} is handled by the client; use on_topic_lost and "
                "channel.joined instead"
            )
        self.event_handlers[event] = handler

    def remove_event_handler(self, event: ChannelEvent) -> None:
        self.event_handlers.pop(event, None)

    def get_event_handler(
        self, event: ChannelEvent
    ) -> Callable[[dict[str, Any]], Awaitable[None]] | None:
        return self.event_handlers.get(event)

    def has_event_handler(self, event: ChannelEvent) -> bool:
        return event in self.event_handlers
