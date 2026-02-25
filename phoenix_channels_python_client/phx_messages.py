from dataclasses import dataclass
from enum import Enum, unique
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
Event = UserEvent | PHXEvent
ChannelEvent = Event


@dataclass(frozen=True)
class Message:
    topic: str
    event: Event
    payload: dict[str, Any]
    ref: str | None = None
    join_ref: str | None = None

    @classmethod
    def from_raw(cls, raw_data: list[Any]) -> "Message":
        if not isinstance(raw_data, list):
            raise TypeError(f"Protocol expects array format, got {type(raw_data).__name__}")

        try:
            join_ref, ref, topic, event, payload = raw_data
        except ValueError as e:
            raise ValueError(
                f"Protocol expects 5-element array [join_ref, ref, topic, event, payload], got {len(raw_data)} elements"
            ) from e

        try:
            event = PHXEvent(event)
        except ValueError:
            event = UserEvent(event)

        return cls(topic=topic, event=event, payload=payload or {}, ref=ref, join_ref=join_ref)

    def to_raw(self) -> list[Any]:
        return [self.join_ref, self.ref, self.topic, str(self.event), self.payload]

    def __post_init__(self) -> None:
        if not isinstance(self.topic, str) or not self.topic:
            raise TypeError(f"topic must be a non-empty string, got: {self.topic!r}")

        if not isinstance(self.event, (str, PHXEvent)) or not self.event:
            raise TypeError(f"event must be a non-empty string, got: {self.event!r}")

        if not isinstance(self.payload, dict):
            raise TypeError(f"payload must be a dict, got: {type(self.payload).__name__}")

        if self.ref is not None and not isinstance(self.ref, str):
            raise TypeError(f"ref must be a string or None, got: {type(self.ref).__name__}")

        if self.join_ref is not None and not isinstance(self.join_ref, str):
            raise TypeError(
                f"join_ref must be a string or None, got: {type(self.join_ref).__name__}"
            )


# Backward-compatible aliases retained for the alpha codebase.
ChannelMessage = Message
PHXMessage = Message
PHXEventMessage = Message
