import asyncio
import logging
from typing import Any

from phoenix_channels_python_client.phx_messages import (
    ChannelEvent,
    ChannelMessage,
    PHXEvent,
    PHXEventMessage,
    PHXMessage,
)

logger = logging.getLogger(__name__)


def parse_event(event: ChannelEvent) -> ChannelEvent:
    try:
        return PHXEvent(event)
    except ValueError:
        return event


def make_message(
    event: ChannelEvent,
    topic: str,
    ref: str | None = None,
    payload: dict[str, Any] | None = None,
    join_ref: str | None = None,
) -> ChannelMessage:
    if payload is None:
        payload = {}

    processed_event = parse_event(event)
    if isinstance(processed_event, PHXEvent):
        return PHXEventMessage(
            event=processed_event,
            topic=topic,
            ref=ref,
            payload=payload,
            join_ref=join_ref,
        )
    return PHXMessage(
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


def setup_logging(level: int = logging.INFO) -> None:
    """Configure clean logging with timestamps for Phoenix Channels Python Client.

    Args:
        level: Logging level (default: logging.INFO for production, use
            logging.DEBUG for development)

    Example:
        >>> from phoenix_channels_python_client.utils import setup_logging
        >>> import logging
        >>> setup_logging(logging.INFO)
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
