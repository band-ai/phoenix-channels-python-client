from __future__ import annotations

from enum import Enum, unique
from functools import cached_property
from typing import Any, NewType

from pydantic import BaseModel, ConfigDict


PHOENIX_TOPIC = "phoenix"


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
# Compatibility alias for existing imports and call sites.
Event = UserEvent
ChannelEvent = PHXEvent | UserEvent


class BasePHXMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    topic: str
    ref: str | None
    payload: dict[str, Any]

    @cached_property
    def subtopic(self) -> str | None:
        if ":" not in self.topic:
            return None
        _, subtopic = self.topic.split(":", 1)
        return subtopic


class PHXMessage(BasePHXMessage):
    event: Event
    join_ref: str | None = None


class PHXEventMessage(BasePHXMessage):
    event: PHXEvent
    join_ref: str | None = None


ChannelMessage = PHXMessage | PHXEventMessage
# Compatibility alias for existing tests and call sites.
Message = ChannelMessage
