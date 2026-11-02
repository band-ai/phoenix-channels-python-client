"""Phoenix Channels Python Client.

A Python client library for connecting to Phoenix Channels.
"""

from __future__ import annotations

from phoenix_channels_python_client.client import (
    DisconnectCallback,
    HeartbeatAckCallback,
    PHXChannelsClient,
    ReconnectCallback,
    ReconnectPolicy,
    TopicLostCallback,
)
from phoenix_channels_python_client.client_types import (
    Connected,
    ConnectFailed,
    Connecting,
    Disconnected,
    JoinResponseCallback,
    ReconnectDecider,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
    PHXProtocolHandler,
)
from phoenix_channels_python_client.utils import setup_logging

__version__ = "0.4.4"
__author__ = "Phoenix Channels Python Client"

__all__ = [
    "ConnectFailed",
    "Connected",
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
