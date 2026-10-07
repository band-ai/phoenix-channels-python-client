# Phoenix Channels Python Client

[![PyPI version](https://img.shields.io/pypi/v/phoenix-channels-python-client.svg)](https://pypi.org/project/phoenix-channels-python-client/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modern, async Python client library for connecting to [Phoenix Channels](https://hexdocs.pm/phoenix/channels.html) - the real-time WebSocket layer of the Phoenix Framework.

## Installation

### For Users

```bash
pip install phoenix-channels-python-client
```

Or from source:

```bash
git clone https://github.com/band-ai/phoenix-channels-python-client.git
cd phoenix-channels-python-client
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install .
```

### For Development

Set up development environment:

```bash
git clone https://github.com/band-ai/phoenix-channels-python-client.git
cd phoenix-channels-python-client
uv sync --extra dev
```

Run tests:

```bash
uv run pytest
```

### Dependencies

- Python 3.11+
- `pydantic>=2.8`
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
        
        # Ctrl+C or SIGTERM shuts the client down and run_forever() returns.
        # The previous handlers are restored first, so a second signal reaches them.
        await client.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

### Owning process signals yourself

If your application owns its process signals (a service, desktop app or test
runner), call `client.run_forever(install_signal_handlers=False)`. It then
leaves signal handlers alone; stop the client by scheduling
`client.shutdown(reason)` on the client's event loop, and `run_forever()`
returns `None`:

```python
loop = asyncio.get_running_loop()
async with asyncio.TaskGroup() as tasks:
    loop.add_signal_handler(
        signal.SIGTERM, lambda: tasks.create_task(client.shutdown("SIGTERM"))
    )
    try:
        await client.run_forever(install_signal_handlers=False)
    finally:
        # Once the group exits, a later SIGTERM must not reach the closed group.
        loop.remove_signal_handler(signal.SIGTERM)
```

From a plain `signal.signal` handler or another thread, use
`loop.call_soon_threadsafe(...)` instead: a bare `asyncio.create_task` there
doesn't wake a loop blocked on I/O, so the shutdown can stall until the next
network activity. See [`examples/embedded_host_signals.py`](examples/embedded_host_signals.py).

With the default, handlers are installed only when `run_forever()` runs on the
main thread; elsewhere it just waits. A handler you registered with
`loop.add_signal_handler` before calling `run_forever()` still fires for the
same signal; one registered while it runs replaces the client's.

## Phoenix System Events

Phoenix reserves `phx_join`, `phx_reply`, `phx_leave`, `phx_close` and
`phx_error`. Don't use these names for your own events.

- The client sends `phx_join` and `phx_leave` and consumes their `phx_reply`.
- The client handles `phx_error` and `phx_close` itself, and neither reaches
  your handlers: a crashed channel is rejoined, and a channel the server
  closes loses its topic (see [Channel recovery](#channel-recovery)).
- `add_event_handler` raises `ValueError` for `PHXEvent.error` and
  `PHXEvent.close`. Use `on_topic_lost` and `is_topic_joined()` instead.

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
    ...  # your code here
```

### Protocol v1.0
```python
from phoenix_channels_python_client import PHXChannelsClient, PhoenixChannelsProtocolVersion

async with PHXChannelsClient(
    websocket_url="ws://localhost:4000/socket/websocket", 
    api_key="your-api-key",
    protocol_version=PhoenixChannelsProtocolVersion.V1
) as client:
    ...  # your code here
```

The client adds `vsn=2.0.0` (V2) or `vsn=1.0.0` (V1) to the socket URL.

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

All handlers must be `async def`. You can add, remove, or change either type of handler at any time during your connection.

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

async def on_message(msg):
    print(f"Message: {msg.payload}")

async def main():
    async with PHXChannelsClient("ws://localhost:4000/socket/websocket", "api-key") as client:
        # Subscribe to a topic
        await client.subscribe_to_topic("room:lobby", on_message)
        
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
- Sends `phx_leave` and waits for the reply (`leave_timeout_s`, default 5 s)
- Raises `PHXTopicError` if the topic isn't subscribed or the leave times out, and `PHXConnectionError` while disconnected

## Configuration

Everything after `api_key` is keyword-only:

| Option | Default | Meaning |
|---|---|---|
| `protocol_version` | `V2` | Phoenix Channels protocol version |
| `auto_reconnect` | `True` | Reconnect and rejoin topics after a disconnect |
| `reconnect_policy` | `ReconnectPolicy()` | Backoff and close-code handling (see below) |
| `heartbeat_interval_s` | `30.0` | Heartbeat period; `None` disables heartbeats |
| `join_timeout_s` | `10.0` | Wait for a join reply |
| `leave_timeout_s` | `5.0` | Wait for a leave reply |
| `max_topic_queue_size` | `1000` | Per-topic buffer; the oldest message is dropped when full |
| `callback_drain_timeout_s` | `2.0` | How long a running callback may finish before a rejoin cancels it |
| `on_reconnect` | `None` | `async () -> None`, called after topics are rejoined |
| `on_disconnect` | `None` | `async (error: Exception \| None) -> None`; `None` on a clean close |
| `on_heartbeat_ack` | `None` | Synchronous `() -> None`; runs on the message path, so keep it fast |
| `on_topic_lost` | `None` | `async (topic: str, error: Exception) -> None`, called when the client drops a topic you didn't unsubscribe |
| `additional_headers` | `None` | Extra handshake headers sent on every (re)connect |

Exceptions raised in callbacks are logged, not raised.

## Connection Lifecycle and Errors

- `async with` waits for the first connection. With `auto_reconnect=True` it keeps retrying; with `auto_reconnect=False` a failed first connection raises `PHXConnectionError`.
- `run_forever()` returns `None` after `shutdown()`, a signal, or a close that doesn't reconnect. It raises `PHXConnectionError` on a terminal close, after repeated rapid disconnects, or when the client has never entered `async with`.
- `subscribe_to_topic()` raises `PHXTopicError` on a rejected join, a join timeout or a duplicate subscription, and `PHXConnectionError` while disconnected or if the connection drops before the join completes.
- `await client.close_connection(reason)` force-closes the current connection; the client then reconnects if `auto_reconnect` is on. It does nothing if the client isn't connected or the connection is already closing.
- Exceptions live in `phoenix_channels_python_client.exceptions`.

### Reconnects

| Close code | Behaviour (defaults) |
|---|---|
| 1000, 1001 | Stop, unless `reconnect_on_normal_close=True` |
| 1008 | Stop with `PHXConnectionError` (`policy_violation_is_terminal=True`) |
| 1012 | Reconnect after at least a random 1–5 s |
| 1013 | Reconnect after at least a random 30–60 s |
| Anything else, or no close frame | Reconnect with jittered backoff: 0.5 s × 2ⁿ, capped at 30 s, reset after 60 s of uptime |

Disconnects within 5 s of connecting count as rapid and get longer minimum delays; ten within 60 s stop the client with `PHXConnectionError`. Tune all of this with `ReconnectPolicy`.

A `ReconnectPolicy` is validated when it is built: an out-of-range or unknown field raises `pydantic.ValidationError`, which is a `ValueError`. To derive one policy from another, validate the merged fields:

```python
base = ReconnectPolicy()
policy = ReconnectPolicy.model_validate(base.model_dump() | {"max_delay_s": 10.0})
```

### Channel recovery

A channel can fail while the socket stays up. When its process crashes, the server sends `phx_error`, and the client rejoins that topic on the same socket, waiting the `ReconnectPolicy` backoff (`base_delay_s`, `factor`, `max_delay_s`, with equal jitter) before each attempt. A rejoin that times out is retried the same way, including one after a socket reconnect. A drop of the socket hands the topic to the socket's own rejoin.

The client drops the topic and calls `on_topic_lost(topic, error)` once when the server rejects a rejoin, after a crash or a reconnect, or closes the channel with `phx_close`. It never calls it for `unsubscribe_from_topic()`, `shutdown()` or a failed first `subscribe_to_topic()`, which raises instead. Subscribing to the topic again from the callback works.

`get_current_subscriptions()` lists the subscribed topics. `is_topic_joined(topic)` tells whether a topic's channel is joined right now: it's false while the socket is down or a crashed channel waits to rejoin. Unsubscribing a topic whose channel is being recovered completes without waiting for a leave reply.

### Heartbeats

The client sends a `heartbeat` on the `phoenix` topic every `heartbeat_interval_s`. A missed reply only logs a warning; to act on it, combine `on_heartbeat_ack` with `close_connection()`.

## Logging

```python
import logging
from phoenix_channels_python_client import setup_logging

setup_logging(logging.DEBUG)  # loggers live under "phoenix_channels_python_client"
```

## Examples

- [`examples/minimal_phx_events_client.py`](examples/minimal_phx_events_client.py): subscribe to a topic and log messages until Ctrl+C or SIGTERM.
- [`examples/embedded_host_signals.py`](examples/embedded_host_signals.py): run the client in a host that owns its process signals.

---

## Security Considerations

**API Key in URL**: This client passes the API key as a URL query parameter when connecting to the WebSocket (e.g., `wss://server.com/socket?api_key=xxx`). Even over WSS (encrypted in transit), the API key may be logged by:

- Server access logs
- Reverse proxies and load balancers
- Network monitoring tools

**Recommendations:**
- Ensure your infrastructure does not log full URLs in production
- Use short-lived, rotatable tokens rather than long-lived API keys
- Consider this limitation when evaluating this client for sensitive environments

**Note:** The official Phoenix JS client (v1.8+) supports header-based authentication via the `authToken` option, which avoids URL logging. This client always sends `api_key` in the URL (logged URLs show `api_key=***`). Pass `additional_headers={"x-api-key": key}` to also send it as a handshake header on every (re)connect, e.g. for a proxy that reads it from there.

---

**Need help?** Open an issue on GitHub or check the [Phoenix Channels documentation](https://hexdocs.pm/phoenix/channels.html) for more information about the Phoenix Channels protocol.
