from __future__ import annotations

from typing import Any

import pytest

import phoenix_channels_python_client as phoenix
from phoenix_channels_python_client import Client


def test_wildcard_imports_expose_only_the_public_api() -> None:
    assert set(phoenix.__all__) == {
        "ChannelError",
        "Client",
        "ClientError",
        "ConnectFailed",
        "Connected",
        "Connecting",
        "DisconnectCallback",
        "Disconnected",
        "HeartbeatAckCallback",
        "JoinResponseCallback",
        "Message",
        "ProtocolVersion",
        "PushTimeoutError",
        "ReconnectCallback",
        "ReconnectDecider",
        "ReconnectDecision",
        "ReconnectEvent",
        "ReconnectPolicy",
        "SocketError",
        "SystemEvent",
        "TopicLostCallback",
    }
    assert all(hasattr(phoenix, name) for name in phoenix.__all__)


@pytest.mark.parametrize(
    "name",
    [
        "PHXChannelsClient",
        "PHXClientError",
        "PHXTopicError",
        "PHXConnectionError",
        "PhoenixChannelsProtocolVersion",
        "PHXEvent",
        "ChannelMessage",
        "PHXProtocolHandler",
        "Event",
        "__author__",
        "setup_logging",
    ],
)
def test_removed_names_have_no_root_compatibility_alias(name: str) -> None:
    assert not hasattr(phoenix, name)


@pytest.mark.parametrize("version", ["unsupported", "1.0.0", "2.0.0", "", " 2.0"])
def test_client_rejects_an_invalid_protocol_version(version: str) -> None:
    with pytest.raises(ValueError, match="not a valid ProtocolVersion"):
        Client("ws://127.0.0.1:1/socket", protocol_version=version)


def test_api_key_cannot_be_passed_positionally() -> None:
    # The dynamic call intentionally exercises a statically invalid signature.
    constructor: Any = Client
    with pytest.raises(TypeError, match="positional"):
        constructor("ws://127.0.0.1:1/socket", "key")


async def test_shutdown_accepts_no_reason(client: Client) -> None:
    await client.shutdown()

    assert client.connection is None
    assert client.get_current_subscriptions() == {}


def test_protocol_handler_is_not_a_client_public_method() -> None:
    assert not hasattr(Client, "get_protocol_handler")
