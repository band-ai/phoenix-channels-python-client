from __future__ import annotations

from enum import StrEnum, unique
from functools import cached_property
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

PHOENIX_TOPIC = "phoenix"


@unique
class SystemEvent(StrEnum):
    close = "phx_close"
    error = "phx_error"
    # The protocol event intentionally shadows str.join.
    join = "phx_join"  # type: ignore[bad-override]
    reply = "phx_reply"
    leave = "phx_leave"


# The client handles these itself: a crash rejoins, a close loses the topic.
CLIENT_LIFECYCLE_EVENTS = frozenset({SystemEvent.error, SystemEvent.close})

HEARTBEAT_EVENT = "heartbeat"
ChannelEvent = str | SystemEvent

# Servers may send a ref as a JSON number; the client matches refs as strings.
Ref = Annotated[str | None, Field(coerce_numbers_to_str=True)]


class _BaseMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    topic: str
    ref: Ref
    payload: dict[str, Any]

    @cached_property
    def subtopic(self) -> str | None:
        if ":" not in self.topic:
            return None
        _, subtopic = self.topic.split(":", 1)
        return subtopic


class _UserMessage(_BaseMessage):
    event: str
    join_ref: Ref = None


class _SystemMessage(_BaseMessage):
    event: SystemEvent
    join_ref: Ref = None


Message = _UserMessage | _SystemMessage
