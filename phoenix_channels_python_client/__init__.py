"""
Phoenix Channels Python Client

A Python client library for connecting to Phoenix Channels.
"""

from __future__ import annotations

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.utils import setup_logging

__version__ = "0.1.5"
__author__ = "Phoenix Channels Python Client"

__all__ = [
    "PHXChannelsClient",
    "PHXProtocolHandler",
    "PhoenixChannelsProtocolVersion",
    "ReconnectPolicy",
    "setup_logging",
]
