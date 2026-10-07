from __future__ import annotations

import asyncio

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.exceptions import PHXConnectionError

from tests.fake_server import FakePhoenixServer
from tests.support import (
    API_KEY,
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    TOPIC,
    make_client,
    reconnect,
)

# Not a Phoenix frame in either protocol, so the client can't parse it.
UNPARSEABLE_FRAME = "not a phoenix frame"


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


async def test_entering_fails_when_the_server_is_unreachable(
    phoenix_server: FakePhoenixServer,
):
    await phoenix_server.stop()

    with pytest.raises(PHXConnectionError):
        async with make_client(phoenix_server, auto_reconnect=False):
            pass


async def test_run_forever_before_entering_raises(phoenix_server: FakePhoenixServer):
    with pytest.raises(PHXConnectionError, match="not connected"):
        await make_client(phoenix_server).run_forever()


async def test_on_disconnect_receives_the_error_that_ended_the_connection(
    phoenix_server: FakePhoenixServer,
):
    errors: asyncio.Queue[Exception | None] = asyncio.Queue()
    client = make_client(phoenix_server, auto_reconnect=False, on_disconnect=errors.put)

    async with client:
        await phoenix_server.send_raw(UNPARSEABLE_FRAME)

        error = await asyncio.wait_for(errors.get(), ASYNC_TIMEOUT_S)

    assert isinstance(error, ValueError)


async def test_on_reconnect_fires_after_a_reconnect_but_not_the_first_connect(
    phoenix_server: FakePhoenixServer,
):
    reconnects: asyncio.Queue[None] = asyncio.Queue()

    async def on_reconnect() -> None:
        await reconnects.put(None)

    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_reconnect=on_reconnect
    )
    async with client:
        assert reconnects.empty()

        await reconnect(phoenix_server, client)

        await asyncio.wait_for(reconnects.get(), ASYNC_TIMEOUT_S)


async def test_raising_lifecycle_callbacks_do_not_stop_reconnecting(
    phoenix_server: FakePhoenixServer,
):
    async def fail_on_disconnect(error: Exception | None) -> None:
        raise ValueError("callback boom")

    async def fail_on_reconnect() -> None:
        raise ValueError("callback boom")

    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        on_disconnect=fail_on_disconnect,
        on_reconnect=fail_on_reconnect,
    )
    async with client:
        await reconnect(phoenix_server, client)
        await reconnect(phoenix_server, client)

        await client.subscribe_to_topic(TOPIC)
