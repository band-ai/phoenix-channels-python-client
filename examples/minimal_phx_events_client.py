#!/usr/bin/env python3
"""
Minimal Phoenix Channels client.

Connects to a Phoenix Channels server, subscribes to one topic and logs every
message until Ctrl+C or SIGTERM.

Usage:
    python examples/minimal_phx_events_client.py

This is a self-contained demo with hardcoded defaults for easy testing.
Modify the constants below to customize the connection settings.
"""

from __future__ import annotations

import asyncio
import logging

from phoenix_channels_python_client import (
    PHXChannelsClient,
    PhoenixChannelsProtocolVersion,
    setup_logging,
)
from phoenix_channels_python_client.phx_messages import ChannelMessage

# Demo configuration - modify these values as needed
API_KEY = "your-api-key"
WS_BASE_URL = "wss://your-server.com/socket/websocket"
TOPIC = "user_rooms:your-room-id"

logger = logging.getLogger(__name__)


async def message_handler(message: ChannelMessage) -> None:
    logger.info("Received: %s", message)


async def main() -> None:
    async with PHXChannelsClient(
        WS_BASE_URL,
        api_key=API_KEY,
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic(TOPIC, message_handler)
        logger.info("Ready - press Ctrl+C or send SIGTERM to stop")
        await client.run_forever()


if __name__ == "__main__":
    # logging.DEBUG shows protocol traffic.
    setup_logging(logging.INFO)
    asyncio.run(main())
