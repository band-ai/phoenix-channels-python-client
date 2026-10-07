from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import FakePhoenixServer
from tests.support import make_client


@pytest.fixture
def protocol() -> PhoenixChannelsProtocolVersion:
    """The protocol under test; ``each_protocol`` runs a test on every version."""
    return PhoenixChannelsProtocolVersion.V2


@pytest_asyncio.fixture
async def phoenix_server(
    protocol: PhoenixChannelsProtocolVersion,
) -> AsyncIterator[FakePhoenixServer]:
    async with FakePhoenixServer(protocol) as server:
        yield server


@pytest_asyncio.fixture
async def client(phoenix_server: FakePhoenixServer) -> AsyncIterator[PHXChannelsClient]:
    """A client connected with default options."""
    async with make_client(phoenix_server) as client:
        yield client


@pytest.fixture
def received() -> asyncio.Queue[ChannelMessage]:
    """Messages delivered to a topic subscribed with ``received.put``."""
    return asyncio.Queue()
