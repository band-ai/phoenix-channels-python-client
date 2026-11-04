import asyncio
import logging
from typing import Any

from phoenix_channels_python_client.messages import (
    ChannelEvent,
    Message,
    Payload,
    SystemEvent,
    SystemMessage,
    UserMessage,
)

logger = logging.getLogger(__name__)


def parse_event(event: ChannelEvent) -> ChannelEvent:
    try:
        return SystemEvent(event)
    except ValueError:
        return event


def make_message(
    event: ChannelEvent,
    topic: str,
    ref: str | None = None,
    payload: Payload | None = None,
    join_ref: str | None = None,
) -> Message:
    if payload is None:
        payload = {}

    processed_event = parse_event(event)
    if isinstance(processed_event, SystemEvent):
        return SystemMessage(
            event=processed_event,
            topic=topic,
            ref=ref,
            payload=payload,
            join_ref=join_ref,
        )
    return UserMessage(
        event=processed_event,
        topic=topic,
        ref=ref,
        payload=payload,
        join_ref=join_ref,
    )


async def cancel_and_wait(*futures: asyncio.Future[Any]) -> None:
    """Cancel ``futures`` and wait until they have all finished.

    Unlike ``suppress(CancelledError)``, the caller's own cancellation still
    propagates. A failure raised while a future unwinds is logged, not raised.
    """
    if not futures:
        return
    for future in futures:
        future.cancel()
    await asyncio.wait(futures)
    for future in futures:
        if not future.cancelled() and (error := future.exception()):
            logger.error("Failed while being cancelled", exc_info=error)
