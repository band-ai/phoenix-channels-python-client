import logging
from typing import Any, Optional

from phoenix_channels_python_client.phx_messages import (
    ChannelEvent,
    ChannelMessage,
    PHXEvent,
    PHXEventMessage,
    PHXMessage,
)


def parse_event(event: ChannelEvent) -> ChannelEvent:
    try:
        return PHXEvent(event)
    except ValueError:
        return event


def make_message(
    event: ChannelEvent,
    topic: str,
    ref: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
    join_ref: Optional[str] = None,
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
    else:
        return PHXMessage(
            event=processed_event,
            topic=topic,
            ref=ref,
            payload=payload,
            join_ref=join_ref,
        )


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure clean logging with timestamps for Phoenix Channels Python Client.

    Args:
        level: Logging level (default: logging.INFO for production, use logging.DEBUG for development)

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
