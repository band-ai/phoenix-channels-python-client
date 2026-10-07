from __future__ import annotations

import asyncio
from asyncio import Future, Queue, Task
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from phoenix_channels_python_client.phx_messages import (
    CLIENT_LIFECYCLE_EVENTS,
    ChannelEvent,
    ChannelMessage,
)


class TopicProcessingState(StrEnum):
    WAITING_FOR_JOIN = "waiting_for_join"
    PROCESSING_LEAVE = "processing_leave"
    NORMAL_PROCESSING = "normal_processing"


@dataclass()
class TopicSubscription:
    """A topic subscription and everything needed to handle its messages."""

    name: str
    async_callback: Callable[[ChannelMessage], Awaitable[None]] | None
    queue: Queue[ChannelMessage]
    join_ref: str
    process_topic_messages_task: Task[None] | None
    current_join_ready: Future[None] = field(default_factory=asyncio.Future)
    unsubscribe_completed: Future[None] = field(default_factory=asyncio.Future)
    leave_requested: asyncio.Event = field(default_factory=asyncio.Event)
    event_handlers: dict[ChannelEvent, Callable[[dict[str, Any]], Awaitable[None]]] = (
        field(default_factory=dict)
    )
    conn_generation: int = 0
    dropped_message_count: int = 0
    current_callback_task: Future[None] | None = None
    # Rejoins the channel after a crash, while the socket stays up.
    recovery_task: Task[None] | None = None

    def add_event_handler(
        self, event: ChannelEvent, handler: Callable[[dict[str, Any]], Awaitable[None]]
    ) -> None:
        if event in CLIENT_LIFECYCLE_EVENTS:
            raise ValueError(
                f"{event} is handled by the client; use on_topic_lost and "
                "is_topic_joined instead"
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
