from dataclasses import dataclass
from enum import Enum, unique
from functools import cached_property
from typing import Any, NewType, Optional, Union


Event = NewType("Event", str)
ChannelEvent = Union["PHXEvent", Event]
ChannelMessage = Union["PHXMessage", "PHXEventMessage"]


@unique
class PHXEvent(Enum):
    """Phoenix Channels admin events"""

    close = "phx_close"
    error = "phx_error"
    join = "phx_join"
    reply = "phx_reply"
    leave = "phx_leave"

    value: str

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class BasePHXMessage:
    topic: str
    ref: Optional[str]
    payload: dict[str, Any]

    @cached_property
    def subtopic(self) -> Optional[str]:
        if ":" not in self.topic:
            return None

        _, subtopic = self.topic.split(":", 1)
        return subtopic


@dataclass(frozen=True)
class PHXMessage(BasePHXMessage):
    event: Event
    join_ref: Optional[str] = None


@dataclass(frozen=True)
class PHXEventMessage(BasePHXMessage):
    event: PHXEvent
    join_ref: Optional[str] = None
