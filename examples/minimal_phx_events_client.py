#!/usr/bin/env python3
"""Minimal Phoenix Channels client.

Connects to a Phoenix Channels server, joins one channel and logs every
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
    Client,
    Message,
    ProtocolVersion,
)

# Demo configuration - modify these values as needed
API_KEY = "your-api-key"
WS_BASE_URL = "wss://your-server.com/socket/websocket"
TOPIC = "user_rooms:your-room-id"

logger = logging.getLogger(__name__)


async def main() -> None:
    async with Client(
        WS_BASE_URL,
        api_key=API_KEY,
        protocol_version=ProtocolVersion.V2,
    ) as client:
        room = client.channel(TOPIC)

        @room.on_any
        async def message_handler(message: Message) -> None:
            logger.info("Received: %s", message)

        async with room:
            logger.info("Ready - press Ctrl+C or send SIGTERM to stop")
            await client.run_forever()


if __name__ == "__main__":
    # Set the package logger to DEBUG for connection diagnostics.
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
