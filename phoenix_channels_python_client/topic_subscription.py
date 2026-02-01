from asyncio import Queue, Task, Future, Event
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable, Optional, Dict, Any

from phoenix_channels_python_client.phx_messages import ChannelMessage, ChannelEvent


class TopicProcessingState(Enum):
    WAITING_FOR_JOIN = "waiting_for_join"
    PROCESSING_LEAVE = "processing_leave"
    NORMAL_PROCESSING = "normal_processing"


@dataclass()
class TopicSubscription:
    """Represents a topic subscription with all necessary components for message handling"""

    name: str
    async_callback: Optional[Callable[[ChannelMessage], Awaitable[None]]]
    queue: Queue[ChannelMessage]
    subscription_ready: Future[None]
    join_ref: str
    process_topic_messages_task: Optional[Task[None]] = None
    leave_requested: Event = field(default_factory=Event)
    unsubscribe_completed: Optional[Future[None]] = None
    current_callback_task: Optional[Task[None]] = None
    event_handlers: Dict[ChannelEvent, Callable[[Dict[str, Any]], Awaitable[None]]] = (
        field(default_factory=dict)
    )

    def add_event_handler(
        self, event: ChannelEvent, handler: Callable[[Dict[str, Any]], Awaitable[None]]
    ) -> None:
        self.event_handlers[event] = handler

    def remove_event_handler(self, event: ChannelEvent) -> None:
        self.event_handlers.pop(event, None)

    def get_event_handler(
        self, event: ChannelEvent
    ) -> Optional[Callable[[Dict[str, Any]], Awaitable[None]]]:
        return self.event_handlers.get(event)

    def has_event_handler(self, event: ChannelEvent) -> bool:
        return event in self.event_handlers
