from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from tests.fake_server import FakePhoenixServer
from tests.support import TopicRecorder, make_client, subscribe_with_handlers

pytest_plugins = ("tests.clock",)


@pytest.fixture
def protocol() -> ProtocolVersion:
    """The protocol under test; ``each_protocol`` runs a test on every version."""
    return ProtocolVersion.V2


@pytest_asyncio.fixture
async def phoenix_server(
    protocol: ProtocolVersion,
) -> AsyncIterator[FakePhoenixServer]:
    async with FakePhoenixServer(protocol) as server:
        yield server


@pytest_asyncio.fixture
async def client(phoenix_server: FakePhoenixServer) -> AsyncIterator[Client]:
    """A client connected with default options."""
    async with make_client(phoenix_server) as client:
        yield client


@pytest.fixture
def received() -> asyncio.Queue[Message]:
    """Messages delivered to a topic subscribed with ``received.put``."""
    return asyncio.Queue()


@pytest_asyncio.fixture
async def subscribed_handlers(client: Client) -> TopicRecorder:
    return await subscribe_with_handlers(client)
