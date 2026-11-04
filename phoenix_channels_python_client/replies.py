from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from phoenix_channels_python_client.messages import Message

DEFAULT_PUSH_TIMEOUT_S = 10.0


class Reply(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str
    response: dict[str, Any]


def reply_from_message(message: Message, *, binary_status: str | None = None) -> Reply:
    if binary_status is not None:
        raise TypeError("Binary replies are not supported")
    payload = message.payload
    status = payload.get("status")
    response = payload.get("response")
    if not isinstance(status, str) or not isinstance(response, dict):
        raise TypeError("Reply must contain a string status and a response map")
    return Reply(status=status, response=response)
