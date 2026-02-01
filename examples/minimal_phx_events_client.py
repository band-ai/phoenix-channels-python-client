#!/usr/bin/env python3
"""
Minimal Phoenix Events Client for Thenvoi Platform

This example demonstrates how to use the phx-events library to connect
to the Thenvoi platform's WebSocket API and listen to real-time events.

Usage:
    python examples/minimal_phx_events_client.py

This is a self-contained demo with hardcoded defaults for easy testing.
Modify the constants below to customize the connection settings.
"""

import asyncio
import logging

from phoenix_channels_python_client import (
    PHXChannelsClient,
    PhoenixChannelsProtocolVersion,
    setup_logging,
)

# Demo configuration - modify these values as needed
API_KEY = "your-api-key"
WS_BASE_URL = "wss://your-server.com/socket/websocket"

# Configure logging with timestamps - use logging.INFO for clean output, logging.DEBUG for development
setup_logging(logging.INFO)

logger = logging.getLogger(__name__)


async def message_handler(message):
    logger.info(f"Received: {message}")


async def main():
    """Main client function."""
    try:
        async with PHXChannelsClient(
            WS_BASE_URL,
            api_key=API_KEY,
            protocol_version=PhoenixChannelsProtocolVersion.V2,
        ) as client:
            await client.subscribe_to_topic("user_rooms:your-room-id", message_handler)
            logger.info("Ready - Press Ctrl+C to stop")

            await client.run_forever()

    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise


if __name__ == "__main__":
    """Run the minimal client."""
    asyncio.run(main())
