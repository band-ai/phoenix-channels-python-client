# Phoenix Channels Python Client

[![PyPI version](https://img.shields.io/pypi/v/phoenix-channels-python-client.svg)](https://pypi.org/project/phoenix-channels-python-client/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modern, async Python client library for connecting to [Phoenix Channels](https://hexdocs.pm/phoenix/channels.html) - the real-time WebSocket layer of the Phoenix Framework.

## Installation

### For Users

Install from source:

```bash
git clone https://github.com/your-org/phoenix_channels_python_client.git
cd phoenix_channels_python_client
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install .
```

### For Development

Set up development environment:

```bash
git clone https://github.com/your-org/phoenix_channels_python_client.git
cd phoenix_channels_python_client
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
python3 -m pip install -U pip
pip install -e ".[dev]"
```

Run tests:

```bash
pip install -e ".[test]"
pytest
```

### Dependencies

- Python 3.11+
- `websockets>=14.2`

## Quick Start

Here's a minimal example to get you started:

```python
import asyncio
from phoenix_channels_python_client import PHXChannelsClient

async def main():
    # Connect to Phoenix WebSocket server
    async with PHXChannelsClient(
        websocket_url="ws://localhost:4000/socket/websocket",
        api_key="your-api-key"
    ) as client:
        
        # Define message handler
        async def handle_message(message):
            print(f"Received: {message}")
        
        # Subscribe to a topic
        await client.subscribe_to_topic("room:lobby", handle_message)
        
        # Use built-in convenience method to keep connection alive
        await client.run_forever()  # Handles Ctrl+C automatically

if __name__ == "__main__":
    asyncio.run(main())
```

## Phoenix System Events

Phoenix Channels uses several reserved event names for internal protocol communication. The client library automatically handles these system events to manage connections, subscriptions, and message routing:

- `phx_join` - Channel join requests
- `phx_reply` - Server replies to client messages  
- `phx_leave` - Channel leave notifications
- `phx_close` - Channel close events
- `phx_error` - Channel error events

**Important:** You should avoid using these event names when handling custom events in your application, as they are reserved for Phoenix's internal protocol. The library uses these events to determine message routing and connection state management.

If you have a specific use case that requires handling these system events directly, you can do so, but be aware that this may interfere with the library's automatic connection management.

## Protocol Versions

Phoenix Channels supports two protocol versions. Choose based on your Phoenix server version:

### Protocol v2.0 (Default)
```python
from phoenix_channels_python_client import PHXChannelsClient

async with PHXChannelsClient(
    websocket_url="ws://localhost:4000/socket/websocket",
    api_key="your-api-key"
    # protocol_version defaults to v2.0
) as client:
    # Your code here
```

### Protocol v1.0
```python
from phoenix_channels_python_client import PHXChannelsClient, PhoenixChannelsProtocolVersion

async with PHXChannelsClient(
    websocket_url="ws://localhost:4000/socket/websocket", 
    api_key="your-api-key",
    protocol_version=PhoenixChannelsProtocolVersion.V1
) as client:
    # Your code here
```

## Usage Examples

### Basic Topic Subscription

```python
import asyncio
from phoenix_channels_python_client import PHXChannelsClient

async def message_handler(message):
    print(f"Topic: {message.topic}")
    print(f"Event: {message.event}")
    print(f"Payload: {message.payload}")

async def main():
    async with PHXChannelsClient("ws://localhost:4000/socket/websocket", "api-key") as client:
        await client.subscribe_to_topic("chat:general", message_handler)
        
        # Use built-in method to keep connection alive
        await client.run_forever()

asyncio.run(main())
```

## Message Handlers vs Event-Specific Handlers

The library provides two complementary ways to handle incoming messages:

**Message Handler** - Receives ALL messages for a topic:
- Set when subscribing to a topic or separately with `set_message_handler()`
- Gets the complete message object with topic, event, payload, etc.
- Good for logging, debugging, or handling any message type

**Event-Specific Handlers** - Receive only messages with specific event names:
- Added with `add_event_handler()` for particular events like "user_join", "chat_message", etc.
- Only get the payload (data) part of the message
- Perfect for handling specific application events

**Execution order** when you have both types of handlers:
1. **Message handler runs first** (if set) - receives the full message
2. **Event-specific handler runs second** (if matching event) - receives just the payload

You can add, remove, or change either type of handler at any time during your connection.

### Event-Specific Handlers

You can register handlers for specific events on a topic:

```python
import asyncio
from phoenix_channels_python_client import PHXChannelsClient

async def handle_user_join(payload):
    print(f"User joined: {payload}")

async def handle_user_leave(payload):
    print(f"User left: {payload}")

async def main():
    async with PHXChannelsClient("ws://localhost:4000/socket/websocket", "api-key") as client:
        # Subscribe to topic first
        await client.subscribe_to_topic("room:lobby")
        
        # Add event-specific handlers for custom events
        client.add_event_handler("room:lobby", "user_join", handle_user_join)
        client.add_event_handler("room:lobby", "user_leave", handle_user_leave)
        
        # Use built-in method to keep connection alive
        await client.run_forever()

asyncio.run(main())
```

## Unsubscribing from Topics

You can unsubscribe from topics to stop receiving messages and clean up resources:

```python
import asyncio
from phoenix_channels_python_client import PHXChannelsClient

async def main():
    async with PHXChannelsClient("ws://localhost:4000/socket/websocket", "api-key") as client:
        # Subscribe to a topic
        await client.subscribe_to_topic("room:lobby", lambda msg: print(f"Message: {msg.payload}"))
        
        # Do some work...
        await asyncio.sleep(5)
        
        # Unsubscribe when no longer needed
        await client.unsubscribe_from_topic("room:lobby")
        print("Unsubscribed from room:lobby")
        
        # Continue with other work or subscribe to different topics
        await client.run_forever()

asyncio.run(main())
```

**Notes on unsubscription:**
- Removes all handlers (both message and event-specific) for that topic
- Sends a leave message to the Phoenix server

---

## Security Considerations

**API Key in URL**: This client passes the API key as a URL query parameter when connecting to the WebSocket (e.g., `wss://server.com/socket?api_key=xxx`). While the connection uses WSS (encrypted in transit), the API key may be logged by:

- Server access logs
- Reverse proxies and load balancers
- Network monitoring tools

**Recommendations:**
- Ensure your infrastructure does not log full URLs in production
- Use short-lived, rotatable tokens rather than long-lived API keys
- Consider this limitation when evaluating this client for sensitive environments

**Note:** The official Phoenix JS client (v1.8+) supports header-based authentication via the `authToken` option, which avoids URL logging. This Python client currently uses the older `params` style for broad compatibility.

---

**Need help?** Open an issue on GitHub or check the [Phoenix Channels documentation](https://hexdocs.pm/phoenix/channels.html) for more information about the Phoenix Channels protocol.
