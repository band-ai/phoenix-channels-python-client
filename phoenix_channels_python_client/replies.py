from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict

from phoenix_channels_python_client.messages import Message, Payload, _require_payload

DEFAULT_PUSH_TIMEOUT_S = 10.0


class Reply(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str
    response: Annotated[Payload, BeforeValidator(_require_payload)]


def reply_from_message(message: Message, *, binary_status: str | None = None) -> Reply:
    if binary_status is not None:
        if type(message.payload) is not bytes:
            raise TypeError("Binary reply requires a bytes payload")
        return Reply(status=binary_status, response=message.payload)
    payload = message.payload
    if not isinstance(payload, dict):
        raise TypeError("Text reply requires a response envelope")
    status, response = payload.get("status"), payload.get("response")
    if not isinstance(status, str) or not isinstance(response, dict):
        raise TypeError("Reply must contain a string status and a response map")
    return Reply(status=status, response=response)
