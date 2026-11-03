from __future__ import annotations

import asyncio
from asyncio import Future, Queue, Task
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum

from phoenix_channels_python_client.client_types import (
    EventHandler,
    JoinResponseCallback,
)
from phoenix_channels_python_client.phx_messages import (
    CLIENT_LIFECYCLE_EVENTS,
    ChannelEvent,
    ChannelMessage,
    Payload,
    PHXEvent,
    PushReply,
)
from phoenix_channels_python_client.utils import parse_event


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
    event_handlers: dict[ChannelEvent, EventHandler] = field(default_factory=dict)
    on_join_response: JoinResponseCallback | None = None
    blocks_reconnect: bool = True
    # Sent with every join of this topic, the first and each rejoin.
    join_payload: Payload = field(default_factory=dict)
    # In-flight pushes by ref, each settled by its phx_reply.
    pending_pushes: dict[str, Future[PushReply]] = field(default_factory=dict)
    established: bool = False
    join_drop_baseline: int = 0
    leave_ref: str | None = None
    conn_generation: int = 0
    dropped_message_count: int = 0
    current_callback_task: Task[None] | None = None
    # A rejoin sets this before draining; the topic task then takes no new message.
    restart_requested: bool = False
    # Rejoins the channel after a crash, while the socket stays up.
    recovery_task: Task[None] | None = None

    def resolve_push(self, message: ChannelMessage) -> bool:
        """Settle the push ``message`` replies to; False if it replies to none."""
        if message.event != PHXEvent.reply or message.ref is None:
            return False
        pending = self.pending_pushes.get(message.ref)
        if pending is None:
            return False
        status = message.reply_status
        if not pending.done():
            pending.set_result(
                PushReply(
                    status=status if isinstance(status, str) else None,
                    response=message.reply_response
                    if message.reply_response is not None
                    else {},
                )
            )
        return True

    def add_event_handler(self, event: ChannelEvent, handler: EventHandler) -> None:
        # PHXEvent is not a StrEnum, so the wire strings "phx_error" and
        # "phx_close" are not members of CLIENT_LIFECYCLE_EVENTS.
        if parse_event(event) in CLIENT_LIFECYCLE_EVENTS:
            raise ValueError(
                f"{event} is handled by the client; use on_topic_lost and "
                "is_topic_joined instead"
            )
        self.event_handlers[event] = handler

    def remove_event_handler(self, event: ChannelEvent) -> None:
        self.event_handlers.pop(event, None)

    def get_event_handler(self, event: ChannelEvent) -> EventHandler | None:
        return self.event_handlers.get(event)

    def has_event_handler(self, event: ChannelEvent) -> bool:
        return event in self.event_handlers
