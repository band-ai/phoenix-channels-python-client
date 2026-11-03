from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, unique
from functools import cached_property
from typing import Annotated, Any, NewType

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, Strict, TypeAdapter

PHOENIX_TOPIC = "phoenix"

# Buffers are copied to bytes; a str is rejected, not encoded.
BufferPayload = bytearray | memoryview


def _buffer_to_bytes(value: object) -> object:
    return bytes(value) if isinstance(value, BufferPayload) else value


# A dict is sent as a JSON text frame; bytes as a binary frame (V2 only).
Payload = dict[str, Any] | Annotated[bytes, BeforeValidator(_buffer_to_bytes), Strict()]
# What a caller may pass; messages hold the coerced ``Payload``.
OutgoingPayload = Payload | BufferPayload

_PAYLOAD_ADAPTER: TypeAdapter[Payload] = TypeAdapter(Payload)


def validate_payload(value: OutgoingPayload) -> Payload:
    """``value`` as a ``Payload``; a ``pydantic.ValidationError`` if it is none."""
    return _PAYLOAD_ADAPTER.validate_python(value)


@dataclass(frozen=True, slots=True)
class PushReply:
    """The ``phx_reply`` to a push: its ``status`` and ``response``.

    ``response`` is ``bytes`` for a binary reply, otherwise the JSON value the
    server sent (``{}`` when it sent none).
    """

    status: str | None
    response: object


@unique
class PHXEvent(Enum):
    close = "phx_close"
    error = "phx_error"
    join = "phx_join"
    reply = "phx_reply"
    leave = "phx_leave"

    def __str__(self) -> str:
        return self.value


# The client handles these itself: a crash rejoins, a close loses the topic.
CLIENT_LIFECYCLE_EVENTS = frozenset({PHXEvent.error, PHXEvent.close})

UserEvent = NewType("UserEvent", str)
# Compatibility alias for existing imports and call sites.
Event = UserEvent
# Not a PHXEvent member: that would reroute make_message and change a public enum.
HEARTBEAT_EVENT = Event("heartbeat")
ChannelEvent = PHXEvent | UserEvent

# Servers may send a ref as a JSON number; the client matches refs as strings.
Ref = Annotated[str | None, Field(coerce_numbers_to_str=True)]


class BasePHXMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    topic: str
    ref: Ref
    payload: Payload

    @property
    def reply_status(self) -> object:
        """The ``status`` of a reply, or None when the payload isn't an object."""
        return self.payload.get("status") if isinstance(self.payload, dict) else None

    @property
    def reply_response(self) -> object:
        """The ``response`` of a reply, or None when the payload isn't an object."""
        return self.payload.get("response") if isinstance(self.payload, dict) else None

    @cached_property
    def subtopic(self) -> str | None:
        if ":" not in self.topic:
            return None
        _, subtopic = self.topic.split(":", 1)
        return subtopic


class PHXMessage(BasePHXMessage):
    event: Event
    join_ref: Ref = None


class PHXEventMessage(BasePHXMessage):
    event: PHXEvent
    join_ref: Ref = None


ChannelMessage = PHXMessage | PHXEventMessage
# Compatibility alias for existing tests and call sites.
Message = ChannelMessage
