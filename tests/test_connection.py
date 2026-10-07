from __future__ import annotations

import asyncio
from typing import Any

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError

from tests.fake_server import FakePhoenixServer
from tests.support import (
    API_KEY,
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    TOPIC,
    UNPARSEABLE_FRAME,
    expect_message,
    make_client,
    reconnect,
    reconnect_after,
    wait_for_condition,
)

# The close code the client sends when it drops a connection on purpose. Spelled
# out, not imported, so the test pins the value peers see.
FORCED_CLOSE_CODE = 4000

# A close reason's limit in bytes, per RFC 6455.
MAX_CLOSE_REASON_BYTES = 123


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


async def test_leaving_the_client_closes_its_connection(
    phoenix_server: FakePhoenixServer,
):
    async with make_client(phoenix_server) as client:
        assert phoenix_server.list_client_connections()

    assert client.connection is None
    assert await wait_for_condition(
        lambda: not phoenix_server.list_client_connections()
    )


async def test_close_connection_drops_the_connection_and_the_client_reconnects(
    phoenix_server: FakePhoenixServer,
):
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await reconnect_after(client, client.close_connection("dead threshold"))

    assert phoenix_server.closes[0] == (FORCED_CLOSE_CODE, "dead threshold")


async def test_close_connection_truncates_an_oversized_reason(
    phoenix_server: FakePhoenixServer,
):
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await reconnect_after(client, client.close_connection("x" * 200))

    assert phoenix_server.closes[0] == (
        FORCED_CLOSE_CODE,
        "x" * MAX_CLOSE_REASON_BYTES,
    )


async def test_close_connection_before_entering_does_nothing(
    phoenix_server: FakePhoenixServer,
):
    await make_client(phoenix_server).close_connection("not connected")

    assert phoenix_server.closes == []


async def test_additional_headers_are_sent_on_the_ws_handshake(
    phoenix_server: FakePhoenixServer,
):
    """`additional_headers` ride the WebSocket handshake, so a caller can send the
    API key as an `x-api-key` header (for proxy in-header injection) rather than
    only in the URL query."""
    headers = {"x-api-key": "header-only-value"}

    async with make_client(phoenix_server, additional_headers=headers) as client:
        (server_ws,) = phoenix_server.list_client_connections()
        assert server_ws.request is not None
        assert server_ws.request.headers["x-api-key"] == headers["x-api-key"]

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

        error = await expect_message(errors)

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

        await expect_message(reconnects)


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


@pytest.mark.parametrize(
    "option,value,message",
    [
        ("heartbeat_interval_s", 0, "heartbeat_interval_s must be > 0"),
        ("heartbeat_interval_s", -1.0, "heartbeat_interval_s must be > 0"),
        ("join_timeout_s", 0, "join_timeout_s must be > 0"),
        ("leave_timeout_s", 0, "leave_timeout_s must be > 0"),
        ("max_topic_queue_size", 0, "max_topic_queue_size must be > 0"),
        ("callback_drain_timeout_s", 0, "callback_drain_timeout_s must be > 0"),
        (
            "reconnect_policy",
            ReconnectPolicy(base_delay_s=-1),
            "Invalid reconnect policy configuration",
        ),
    ],
)
def test_an_out_of_range_client_option_is_rejected(
    option: str, value: Any, message: str
):
    with pytest.raises(ValueError, match=message):
        make_client(FakePhoenixServer(), **{option: value})
