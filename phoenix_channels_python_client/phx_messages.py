from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from functools import cached_property
from typing import Any, NewType


@unique
class PHXEvent(Enum):
    close = "phx_close"
    error = "phx_error"
    join = "phx_join"
    reply = "phx_reply"
    leave = "phx_leave"

    def __str__(self) -> str:
        return self.value


UserEvent = NewType("UserEvent", str)
Event = UserEvent
ChannelEvent = PHXEvent | Event


@dataclass(frozen=True)
class BasePHXMessage:
    topic: str
    ref: str | None
    payload: dict[str, Any]

    @cached_property
    def subtopic(self) -> str | None:
        if ":" not in self.topic:
            return None
        _, subtopic = self.topic.split(":", 1)
        return subtopic


@dataclass(frozen=True)
class PHXMessage(BasePHXMessage):
    event: Event
    join_ref: str | None = None


@dataclass(frozen=True)
class PHXEventMessage(BasePHXMessage):
    event: PHXEvent
    join_ref: str | None = None


ChannelMessage = PHXMessage | PHXEventMessage
# Compatibility alias for existing tests and call sites.
Message = PHXMessage | PHXEventMessage
