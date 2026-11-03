from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import FakePhoenixServer
from tests.support import (
    API_KEY,
    ASYNC_TIMEOUT_S,
    EVENT,
    FAST_RECONNECT,
    STOP_REASON,
    STOPS_ON_NORMAL_CLOSE,
    TOPIC,
    UNPARSEABLE_FRAME,
    deliver,
    each_protocol,
    expect_message,
    make_client,
    reconnect,
    reconnect_after,
    start_server_close,
    wait_for_condition,
)

# The close code the client sends when it drops a connection on purpose. Spelled
# out, not imported, so the test pins the value peers see.
FORCED_CLOSE_CODE = 4000

# A close reason's limit in bytes, per RFC 6455.
MAX_CLOSE_REASON_BYTES = 123


@each_protocol
@pytest.mark.parametrize("options", [{"api_key": None}, {}], ids=["none", "omitted"])
@pytest.mark.parametrize(
    ("query", "credentials"),
    [
        ("api_key=url-secret", [("api_key", "url-secret")]),
        (
            "API%5FKEY=private%2Bkey%2F%3F&api_key=second&aPi_KeY=",
            [("API_KEY", "private+key/?"), ("api_key", "second"), ("aPi_KeY", "")],
        ),
    ],
)
async def test_without_an_api_key_argument_the_url_credential_is_preserved(
    phoenix_server: FakePhoenixServer,
    options: dict[str, Any],
    query: str,
    credentials: list[tuple[str, str]],
) -> None:
    url = phoenix_server.url_for(FakePhoenixServer.SOCKET_PATH)
    async with PHXChannelsClient(
        f"{url}?{query}&keep=one&keep=&vsn=old",
        protocol_version=phoenix_server.protocol,
        **options,
    ) as client:
        (connection,) = phoenix_server.list_client_connections()
        assert connection.request is not None
        expected = [
            *credentials,
            ("keep", "one"),
            ("keep", ""),
            ("vsn", phoenix_server.protocol.vsn),
        ]
        assert (
            parse_qsl(urlsplit(client.channel_socket_url).query, keep_blank_values=True)
            == expected
        )
        assert (
            parse_qsl(urlsplit(connection.request.path).query, keep_blank_values=True)
            == expected
        )


@pytest.mark.parametrize(
    "headers",
    [None, {"x-api-key": "header-key"}, {"x-auth-token": "header-token"}],
)
def test_an_empty_api_key_argument_is_rejected(
    headers: dict[str, str] | None,
) -> None:
    with pytest.raises(ValueError, match=r"^api_key must be nonempty or None$"):
        PHXChannelsClient("ws://127.0.0.1:1/socket", "", additional_headers=headers)


@each_protocol
@pytest.mark.parametrize("options", [{}, {"api_key": None}], ids=["omitted", "none"])
async def test_a_client_connects_without_an_api_key(
    phoenix_server: FakePhoenixServer, options: dict[str, Any]
) -> None:
    async with PHXChannelsClient(
        phoenix_server.url_for(FakePhoenixServer.SOCKET_PATH),
        protocol_version=phoenix_server.protocol,
        **options,
    ) as client:
        (connection,) = phoenix_server.list_client_connections()
        assert connection.request is not None
        expected = [("vsn", phoenix_server.protocol.vsn)]
        assert parse_qsl(urlsplit(connection.request.path).query) == expected
        assert parse_qsl(urlsplit(client.channel_socket_url).query) == expected
        assert client.channel_socket_url_redacted == client.channel_socket_url


@each_protocol
@pytest.mark.parametrize("positional", [True, False], ids=["positional", "keyword"])
@pytest.mark.parametrize("api_key", [API_KEY, " "], ids=["key", "whitespace"])
async def test_an_api_key_argument_replaces_url_credentials(
    phoenix_server: FakePhoenixServer, api_key: str, *, positional: bool
) -> None:
    url = phoenix_server.url_for(FakePhoenixServer.SOCKET_PATH)
    url += "?API%5FKEY=old&api_key=other&token=legacy-token&keep=&vsn=old"
    if positional:
        client = PHXChannelsClient(
            url, api_key, protocol_version=phoenix_server.protocol
        )
    else:
        client = PHXChannelsClient(
            url, api_key=api_key, protocol_version=phoenix_server.protocol
        )
    async with client:
        (connection,) = phoenix_server.list_client_connections()
        assert connection.request is not None
        expected = [
            ("token", "legacy-token"),
            ("keep", ""),
            ("api_key", api_key),
            ("vsn", phoenix_server.protocol.vsn),
        ]
        assert (
            parse_qsl(urlsplit(connection.request.path).query, keep_blank_values=True)
            == expected
        )
        assert (
            parse_qsl(urlsplit(client.channel_socket_url).query, keep_blank_values=True)
            == expected
        )


@each_protocol
@pytest.mark.parametrize(
    "options",
    [{}, {"api_key": None}, {"api_key": API_KEY}],
    ids=["omitted", "none", "given"],
)
@pytest.mark.parametrize(
    "headers",
    [
        {"X-API-Key": "header-key"},
        {"X-Auth-Token": "header-token"},
        {"X-API-Key": "header-key", "X-Auth-Token": "header-token"},
    ],
    ids=["api-key", "token", "both"],
)
async def test_header_authentication_suppresses_query_credentials(
    phoenix_server: FakePhoenixServer,
    options: dict[str, Any],
    headers: dict[str, str],
) -> None:
    url = phoenix_server.url_for(FakePhoenixServer.SOCKET_PATH)
    token = "legacy-token" if "X-Auth-Token" in headers else ""
    async with PHXChannelsClient(
        f"{url}?API%5FKEY=old&api_key=other&token={token}&keep=one&keep=",
        protocol_version=phoenix_server.protocol,
        additional_headers=headers,
        reconnect_policy=FAST_RECONNECT,
        **options,
    ) as client:
        for reconnecting in (False, True):
            if reconnecting:
                await reconnect(phoenix_server, client)
            (connection,) = phoenix_server.list_client_connections()
            assert connection.request is not None
            expected = [
                ("keep", "one"),
                ("keep", ""),
                ("vsn", phoenix_server.protocol.vsn),
            ]
            assert (
                parse_qsl(
                    urlsplit(connection.request.path).query, keep_blank_values=True
                )
                == expected
            )
            assert (
                parse_qsl(
                    urlsplit(client.channel_socket_url).query, keep_blank_values=True
                )
                == expected
            )
            assert client.channel_socket_url_redacted == client.channel_socket_url
            assert client.additional_headers == {
                name.lower(): value for name, value in headers.items()
            }
            for name, value in headers.items():
                assert connection.request.headers[name] == value


@pytest.mark.parametrize(
    ("protocol", "version"),
    [
        (PhoenixChannelsProtocolVersion.V1, "1.0"),
        (PhoenixChannelsProtocolVersion.V2, "2.0"),
        (PhoenixChannelsProtocolVersion.V1, "unsupported"),
    ],
)
async def test_a_string_version_connects_and_receives_a_message(
    phoenix_server: FakePhoenixServer,
    version: str,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    async with PHXChannelsClient(
        phoenix_server.url_for(FakePhoenixServer.SOCKET_PATH),
        API_KEY,
        protocol_version=version,
    ) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        await deliver(phoenix_server, client, payload={"ok": True})

        message = await expect_message(received)

    assert (message.topic, message.event, message.payload) == (
        TOPIC,
        EVENT,
        {"ok": True},
    )


async def test_run_forever_returns_on_a_normal_close_when_reconnecting_on_it_is_off(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(
        phoenix_server, reconnect_policy=STOPS_ON_NORMAL_CLOSE
    ) as client:
        await client.subscribe_to_topic(TOPIC)
        run = asyncio.create_task(client.run_forever())

        await phoenix_server.close_all_clients(code=CloseCode.NORMAL_CLOSURE)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

    assert client.get_current_subscriptions() == {}
    assert client.connection is None


async def test_leaving_the_client_closes_its_connection(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server) as client:
        assert phoenix_server.list_client_connections()

    assert client.connection is None
    assert await wait_for_condition(
        lambda: not phoenix_server.list_client_connections()
    )


async def test_close_connection_drops_the_connection_and_the_client_reconnects(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await reconnect_after(client, client.close_connection("dead threshold"))

    assert phoenix_server.closes[0] == (FORCED_CLOSE_CODE, "dead threshold")


async def test_close_connection_truncates_an_oversized_reason(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await reconnect_after(client, client.close_connection("x" * 200))

    assert phoenix_server.closes[0] == (
        FORCED_CLOSE_CODE,
        "x" * MAX_CLOSE_REASON_BYTES,
    )


async def test_forced_close_during_a_terminal_server_close_does_not_reconnect(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        run = asyncio.create_task(client.run_forever(install_signal_handlers=False))
        server_close = await start_server_close(
            phoenix_server, client, code=CloseCode.POLICY_VIOLATION
        )

        await client.close_connection(STOP_REASON)

        with pytest.raises(PHXConnectionError):
            await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
        await asyncio.wait_for(server_close, ASYNC_TIMEOUT_S)


async def test_close_connection_before_entering_does_nothing(
    phoenix_server: FakePhoenixServer,
) -> None:
    await make_client(phoenix_server).close_connection("not connected")

    assert phoenix_server.closes == []


async def test_additional_headers_are_sent_on_the_ws_handshake(
    phoenix_server: FakePhoenixServer,
) -> None:
    """`additional_headers` ride the WebSocket handshake.

    So a caller can send the API key as an `x-api-key` header (for proxy in-header
    injection) rather than only in the URL query.
    """
    headers = {"x-api-key": "header-only-value"}

    async with make_client(phoenix_server, additional_headers=headers) as client:
        (server_ws,) = phoenix_server.list_client_connections()
        assert server_ws.request is not None
        assert server_ws.request.headers["x-api-key"] == headers["x-api-key"]

    assert "api_key=" not in client.channel_socket_url


async def test_entering_fails_when_the_server_is_unreachable(
    phoenix_server: FakePhoenixServer,
) -> None:
    await phoenix_server.stop()

    with pytest.raises(PHXConnectionError):
        async with make_client(phoenix_server, auto_reconnect=False):
            pass


async def test_run_forever_before_entering_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    with pytest.raises(PHXConnectionError, match="not connected"):
        await make_client(phoenix_server).run_forever()


async def test_on_disconnect_receives_the_error_that_ended_the_connection(
    phoenix_server: FakePhoenixServer,
) -> None:
    errors: asyncio.Queue[Exception | None] = asyncio.Queue()
    client = make_client(phoenix_server, auto_reconnect=False, on_disconnect=errors.put)

    async with client:
        await phoenix_server.send_raw(UNPARSEABLE_FRAME)

        error = await expect_message(errors)

    assert isinstance(error, ValueError)


async def test_on_reconnect_fires_after_a_reconnect_but_not_the_first_connect(
    phoenix_server: FakePhoenixServer,
) -> None:
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
) -> None:
    async def fail_on_disconnect(error: Exception | None) -> None:
        del error
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
    ("options", "message"),
    [
        ({"heartbeat_interval_s": 0}, "heartbeat_interval_s must be > 0"),
        ({"heartbeat_interval_s": -1.0}, "heartbeat_interval_s must be > 0"),
        ({"join_timeout_s": 0}, "join_timeout_s must be > 0"),
        ({"leave_timeout_s": 0}, "leave_timeout_s must be > 0"),
        ({"max_topic_queue_size": 0}, "max_topic_queue_size must be > 0"),
        ({"callback_drain_timeout_s": 0}, "callback_drain_timeout_s must be > 0"),
        ({"max_message_size": 0}, "max_message_size must be > 0"),
    ],
)
def test_an_out_of_range_client_option_is_rejected(
    options: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        make_client(FakePhoenixServer(), **options)
