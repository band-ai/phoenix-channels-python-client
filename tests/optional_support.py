"""Acquire subscribed clients and probes for optional recovery tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from tests.join_support import JoinProbe, deadline
from tests.support import FAST_RECONNECT, OTHER_TOPIC, TOPIC, make_client

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from phoenix_channels_python_client.client import PHXChannelsClient
    from tests.fake_server import FakePhoenixServer


JOIN_DEADLINE = 10.0


@asynccontextmanager
async def subscribed_optional(
    server: FakePhoenixServer,
    **options: Any,
) -> AsyncIterator[
    tuple[
        PHXChannelsClient,
        JoinProbe,
        asyncio.Queue[None],
        asyncio.Queue[tuple[str, Exception]],
    ]
]:
    reconnects: asyncio.Queue[None] = asyncio.Queue()
    losses: asyncio.Queue[tuple[str, Exception]] = asyncio.Queue()

    async def on_lost(topic: str, error: Exception) -> None:
        await losses.put((topic, error))

    async with (
        deadline(),
        make_client(
            server,
            reconnect_policy=FAST_RECONNECT,
            on_reconnect=lambda: reconnects.put(None),
            on_topic_lost=on_lost,
            **options,
        ) as client,
    ):
        probe = JoinProbe(client)
        await client.subscribe_to_topic(
            TOPIC,
            probe.receive,
            on_join_response=probe.install,
            blocks_reconnect=False,
        )
        await server.joined.get()
        await probe.installed.get()
        yield client, probe, reconnects, losses


@asynccontextmanager
async def subscribed_optional_pair(
    server: FakePhoenixServer,
    **options: Any,
) -> AsyncIterator[tuple[PHXChannelsClient, JoinProbe, JoinProbe, asyncio.Queue[None]]]:
    async with subscribed_optional(server, **options) as (
        client,
        optional,
        reconnects,
        _,
    ):
        required = JoinProbe(client, OTHER_TOPIC)
        await client.subscribe_to_topic(
            OTHER_TOPIC,
            required.receive,
            on_join_response=required.install,
        )
        await server.joined.get()
        await required.installed.get()
        yield client, optional, required, reconnects
