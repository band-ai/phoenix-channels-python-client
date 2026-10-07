#!/usr/bin/env python3
"""Run the client inside a host that owns its process signals.

Services, desktop apps and test runners often install their own SIGTERM/SIGINT
handling. Such a host tells run_forever() to leave signals alone and schedules
shutdown() from its own handler; run_forever() then returns normally.

loop.add_signal_handler() is Unix-only.

Usage:
    python examples/embedded_host_signals.py
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from phoenix_channels_python_client import PHXChannelsClient, setup_logging
from phoenix_channels_python_client.phx_messages import ChannelMessage

API_KEY = "your-api-key"
WS_URL = "wss://your-server.com/socket/websocket"
TOPIC = "user_rooms:your-room-id"
HOST_SIGNALS = (signal.SIGTERM, signal.SIGINT)

logger = logging.getLogger(__name__)


@contextmanager
def host_signal_handlers(on_signal: Callable[[], object]) -> Iterator[None]:
    loop = asyncio.get_running_loop()
    previous = {sig: signal.getsignal(sig) for sig in HOST_SIGNALS}
    for sig in HOST_SIGNALS:
        loop.add_signal_handler(sig, on_signal)
    try:
        yield
    finally:
        for sig, handler in previous.items():
            loop.remove_signal_handler(sig)
            # remove_signal_handler resets to SIG_DFL, not asyncio.run's handler.
            signal.signal(sig, handler)


async def handle_message(message: ChannelMessage) -> None:
    logger.info("Received: %s", message)


async def main() -> None:
    async with (
        PHXChannelsClient(WS_URL, api_key=API_KEY) as client,
        asyncio.TaskGroup() as tasks,
    ):
        await client.subscribe_to_topic(TOPIC, handle_message)

        def stop() -> None:
            tasks.create_task(client.shutdown("Host received a stop signal"))

        with host_signal_handlers(stop):
            logger.info("Ready - send SIGTERM or press Ctrl+C to stop")
            await client.run_forever(install_signal_handlers=False)
    logger.info("Stopped cleanly")


if __name__ == "__main__":
    setup_logging(logging.INFO)
    asyncio.run(main())
