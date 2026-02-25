from __future__ import annotations

import asyncio
from asyncio import Future, Queue, Task
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from phoenix_channels_python_client.phx_messages import ChannelEvent, ChannelMessage


class TopicProcessingState(Enum):
    WAITING_FOR_JOIN = "waiting_for_join"
    PROCESSING_LEAVE = "processing_leave"
    NORMAL_PROCESSING = "normal_processing"


@dataclass()
class TopicSubscription:
    """Represents a topic subscription with all necessary components for message handling."""

    name: str
    async_callback: Callable[[ChannelMessage], Awaitable[None]] | None
    queue: Queue[ChannelMessage]
    join_ref: str
    process_topic_messages_task: Task[None] | None
    subscription_ready: Future[None] = field(default_factory=asyncio.Future)
    current_join_ready: Future[None] = field(default_factory=asyncio.Future)
    unsubscribe_completed: Future[None] = field(default_factory=asyncio.Future)
    leave_requested: asyncio.Event = field(default_factory=asyncio.Event)
    event_handlers: dict[ChannelEvent, Callable[[dict[str, Any]], Awaitable[None]]] = (
        field(default_factory=dict)
    )
    conn_generation: int = 0
    dropped_message_count: int = 0
    current_callback_task: Task[None] | None = None

    def add_event_handler(
        self, event: ChannelEvent, handler: Callable[[dict[str, Any]], Awaitable[None]]
    ) -> None:
        self.event_handlers[event] = handler

    def remove_event_handler(self, event: ChannelEvent) -> None:
        self.event_handlers.pop(event, None)

    def get_event_handler(
        self, event: ChannelEvent
    ) -> Callable[[dict[str, Any]], Awaitable[None]] | None:
        return self.event_handlers.get(event)

    def has_event_handler(self, event: ChannelEvent) -> bool:
        return event in self.event_handlers
