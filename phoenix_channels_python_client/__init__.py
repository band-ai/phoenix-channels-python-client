"""
Phoenix Channels Python Client

A Python client library for connecting to Phoenix Channels.
"""

from __future__ import annotations

from phoenix_channels_python_client.client import (
    DisconnectCallback,
    HeartbeatAckCallback,
    PHXChannelsClient,
    ReconnectCallback,
    ReconnectPolicy,
)
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.utils import setup_logging

__version__ = "0.2.4"
__author__ = "Phoenix Channels Python Client"

__all__ = [
    "DisconnectCallback",
    "HeartbeatAckCallback",
    "PHXChannelsClient",
    "PHXProtocolHandler",
    "PhoenixChannelsProtocolVersion",
    "ReconnectCallback",
    "ReconnectPolicy",
    "setup_logging",
]
