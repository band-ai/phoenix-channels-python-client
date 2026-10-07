from __future__ import annotations

import asyncio

from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient

from tests.fake_server import FakePhoenixServer
from tests.support import API_KEY, ASYNC_TIMEOUT_S, TOPIC, make_client


async def test_run_forever_returns_when_the_server_closes_normally(
    phoenix_server: FakePhoenixServer,
):
    async with make_client(phoenix_server) as client:
        await client.subscribe_to_topic(TOPIC)
        run = asyncio.create_task(client.run_forever())

        await phoenix_server.close_all_clients(code=CloseCode.NORMAL_CLOSURE)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

    assert client.get_current_subscriptions() == {}
    assert client.connection is None


async def test_leaving_the_client_shuts_it_down(phoenix_server: FakePhoenixServer):
    async with make_client(phoenix_server) as client:
        pass

    assert client._shutdown_event.is_set()


async def test_reconnection_is_enabled_by_default(client: PHXChannelsClient):
    assert client.auto_reconnect is True


async def test_reconnection_can_be_disabled(phoenix_server: FakePhoenixServer):
    async with make_client(phoenix_server, auto_reconnect=False) as client:
        assert client.auto_reconnect is False


async def test_additional_headers_are_sent_on_the_ws_handshake(
    phoenix_server: FakePhoenixServer,
):
    """`additional_headers` ride the WebSocket handshake, so a caller can send the
    API key as an `x-api-key` header (for proxy in-header injection) rather than
    only in the URL query."""
    headers = {"x-api-key": "header-only-value"}

    async with make_client(phoenix_server, additional_headers=headers) as client:
        # subscribing forces a live connection, so the server sees the handshake
        await client.subscribe_to_topic(TOPIC)

    server_ws = phoenix_server.client_websocket
    assert server_ws is not None
    request = server_ws.request
    assert request is not None
    assert request.headers["x-api-key"] == headers["x-api-key"]
    # the header is additive; the existing api_key query param is untouched
    assert f"api_key={API_KEY}" in client.channel_socket_url
