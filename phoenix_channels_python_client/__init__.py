"""Phoenix Channels Python Client.

A Python client library for connecting to Phoenix Channels.
"""

from __future__ import annotations

from phoenix_channels_python_client.client import (
    ConnectedCallback,
    DisconnectCallback,
    HeartbeatAckCallback,
    PHXChannelsClient,
    ReconnectCallback,
    ReconnectDecider,
    ReconnectPolicy,
    TopicLostCallback,
)
from phoenix_channels_python_client.client_types import (
    ConnectFailed,
    Connecting,
    Disconnected,
    JoinResponseCallback,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.utils import setup_logging

__version__ = "0.4.2"
__author__ = "Phoenix Channels Python Client"

__all__ = [
    "ConnectFailed",
    "ConnectedCallback",
    "Connecting",
    "DisconnectCallback",
    "Disconnected",
    "HeartbeatAckCallback",
    "JoinResponseCallback",
    "PHXChannelsClient",
    "PHXProtocolHandler",
    "PhoenixChannelsProtocolVersion",
    "ReconnectCallback",
    "ReconnectDecider",
    "ReconnectDecision",
    "ReconnectEvent",
    "ReconnectPolicy",
    "TopicLostCallback",
    "setup_logging",
]
