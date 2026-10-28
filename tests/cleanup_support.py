"""A removed subscription whose real callback finalizer remains held."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from phoenix_channels_python_client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.fake_server import FakePhoenixServer
from tests.join_support import push
from tests.support import STOP_REASON, TOPIC, make_client


@dataclass
class HeldCleanup:
    client: PHXChannelsClient
    running: asyncio.Event = field(default_factory=asyncio.Event)
    finalizing: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    callback: asyncio.Task[None] | None = None

    async def receive(self, message: ChannelMessage) -> None:
        del message
        self.callback = asyncio.current_task()
        self.running.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.finalizing.set()
            await self.release.wait()
            self.finished.set()


@asynccontextmanager
async def terminal_cleanup(peer: FakePhoenixServer) -> AsyncIterator[HeldCleanup]:
    client = make_client(peer, auto_reconnect=False, heartbeat_interval_s=None)
    held = HeldCleanup(client)
    leave: asyncio.Task[None] | None = None
    await client.__aenter__()
    try:
        await client.subscribe_to_topic(TOPIC, held.receive)
        _, request = await peer.joined.get()
        await push(peer, request)
        await held.running.wait()
        leave = asyncio.create_task(client.unsubscribe_from_topic(TOPIC))
        await peer.left.get()
        leave.cancel()
        await held.finalizing.wait()
        leave.cancel()
        await asyncio.gather(leave, return_exceptions=True)
        await peer.close_all_clients()
        await client.run_forever(install_signal_handlers=False)
        yield held
    finally:
        held.release.set()
        if held.callback is not None:
            await asyncio.gather(held.callback, return_exceptions=True)
        if leave is not None:
            leave.cancel()
            await asyncio.gather(leave, return_exceptions=True)
        await client.shutdown(STOP_REASON)
        # Also completes teardown when testing the original premature-close bug.
        await asyncio.gather(*client._cleanup_tasks)
