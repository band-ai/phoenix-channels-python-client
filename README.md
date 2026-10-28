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
| `heartbeat_interval_s` | `30.0` | Heartbeat period; `None` disables heartbeats and timeout enforcement |
| `heartbeat_dead_threshold_s` | `None` | Optional finite positive acknowledgement-silence limit, greater than the enabled finite interval |
| `join_timeout_s` | `10.0` | Wait for a join reply |
| `leave_timeout_s` | `5.0` | Wait for a leave reply |
| `max_topic_queue_size` | `1000` | Per-topic buffer; the oldest message is dropped when full |
| `callback_drain_timeout_s` | `2.0` | How long the message being handled (its message handler, then its event handler) may finish before a rejoin cancels it; queued messages are dropped |
| `on_reconnect` | `None` | `async () -> None`, called after required first-attempt reconciliation |
| `on_disconnect` | `None` | `async (error: Exception \| None) -> None`; `None` on a clean close |
| `on_heartbeat_ack` | `None` | Synchronous `() -> None`; runs on the message path, so keep it fast |
| `on_topic_lost` | `None` | `async (topic: str, error: Exception) -> None`, called when the client drops a topic you didn't unsubscribe |
| `additional_headers` | `None` | Extra handshake headers sent on every (re)connect |

Exceptions raised in ordinary message and lifecycle callbacks are logged, not raised.
Join-response installation failures instead fail the join (see below).

## Connection Lifecycle and Errors

- `async with` waits for the first connection. With `auto_reconnect=True` it keeps retrying, except after a refused handshake with HTTP 403, which raises `PHXConnectionError`; with `auto_reconnect=False` a failed first connection raises `PHXConnectionError`.
- `run_forever()` returns `None` after `shutdown()`, a signal, or a close that doesn't reconnect. It raises `PHXConnectionError` on a terminal close, a refused handshake with HTTP 403, after repeated rapid disconnects, or when the client has never entered `async with`.
- `subscribe_to_topic()` raises `PHXTopicError` on a rejected join, a join timeout or a duplicate subscription, and `PHXConnectionError` while disconnected or if the connection drops before the join completes.
- `await client.close_connection(reason)` force-closes the current connection; the client then reconnects if `auto_reconnect` is on. It does nothing if the client isn't connected or the connection is already closing.
- Exceptions live in `phoenix_channels_python_client.exceptions`.

### Reconnects

| Close code or handshake | Behaviour (defaults) |
|---|---|
| 1000, 1001 | Reconnect (Phoenix ≥ 1.8.8 sends 1001 for "reconnect expected"); stop with `reconnect_on_normal_close=False` |
| 1008 | Stop with `PHXConnectionError` (`policy_violation_is_terminal=True`) |
| Handshake refused with HTTP 403 | Stop with `PHXConnectionError`; other statuses (401, 5xx, …) retry with the backoff below |
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

The client drops the topic and calls `on_topic_lost(topic, error)` once when the server rejects a rejoin, after a crash or a reconnect, closes the channel with `phx_close`, or its message processor fails. It never calls it for `unsubscribe_from_topic()`, `shutdown()` or a failed first `subscribe_to_topic()`, which raises instead. Subscribing to the topic again from the callback works. `shutdown()` cancels the callback, including when the callback itself called `shutdown()`, so code after that call does not run.

`get_current_subscriptions()` lists the subscribed topics. `is_topic_joined(topic)` tells whether a topic's channel is joined right now: it's false while the socket is down or a crashed channel waits to rejoin. Unsubscribing a topic whose channel is being recovered completes without waiting for a leave reply.

### Heartbeats

The client sends a `heartbeat` on the `phoenix` topic every `heartbeat_interval_s`. By default, missed replies log warnings and sending continues with a new reference each interval.

Set `heartbeat_dead_threshold_s` to enable closure after acknowledgement silence:

```python
client = PHXChannelsClient(
    websocket_url, api_key,
    heartbeat_interval_s=30.0,
    heartbeat_dead_threshold_s=60.0,
)
```

The deadline starts when each socket is adopted, including during rejoin and reconnect callbacks. Only a matching pending reply received strictly before expiry renews it, before `on_heartbeat_ack` runs. Enabled enforcement keeps one outstanding heartbeat, skipping sends until it is acknowledged or expires. WebSocket Pong and unrelated messages do not renew it. Rejoin completion grants no additional grace.

Expiry or heartbeat send failure force-closes that socket; the supervisor handles cleanup and reconnect/rejoin according to `auto_reconnect` and the existing policy. The deadline bounds close initiation; the transport has its own close-handshake timeout. `heartbeat_interval_s=None` disables heartbeat activity even with a threshold, but a supplied threshold must still be finite and positive. Long thresholds do not prevent the server from applying its own inactivity timeout.

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

**API Key in URL**: Without an authentication header, this client passes the API key as a URL query parameter when connecting to the WebSocket (e.g., `wss://server.com/socket?api_key=xxx`). Even over WSS (encrypted in transit), the API key may be logged by:

- Server access logs
- Reverse proxies and load balancers
- Network monitoring tools

**Recommendations:**
- Ensure your infrastructure does not log full URLs in production
- Use short-lived, rotatable tokens rather than long-lived API keys
- Consider this limitation when evaluating this client for sensitive environments

Pass `additional_headers={"x-api-key": key}` or `{"x-auth-token": jwt}` to use header-only authentication on every connection and reconnect.

---

**Need help?** Open an issue on GitHub or check the [Phoenix Channels documentation](https://hexdocs.pm/phoenix/channels.html) for more information about the Phoenix Channels protocol.

### Authentication and diagnostic safety

A nonempty `x-api-key` or `x-auth-token` handshake header selects header-only authentication on
every connection and reconnect. Embedded API-key parameters are removed; empty
or conflicting authentication headers and simultaneous token-query authentication
are rejected before connecting. A JWT header overrides an embedded token and takes
precedence over an API-key header, matching the platform. Without an authentication header, legacy query authentication
is preserved and remains visible to servers and proxies. No fallback is attempted.

`client.logger` forwards sanitized connection records without raw arguments,
exception objects, or socket metadata. `client.redact_diagnostic(text)` applies
the credential policy to transport errors a caller needs to retain. Caller-visible
errors may still contain personal data and should not be logged outside this policy.

Connection diagnostics omit dynamic strings and transport payloads, including URLs,
headers, frame bodies, user/topic identities, and untrusted server error details.
Logs retain static lifecycle descriptions, native connection states,
and exception types. Application data and caller-visible errors are not changed;
applications remain responsible for logs outside this connection-owned boundary.

### Ordered join responses

Use the optional keyword-only `on_join_response` hook to install a snapshot from
initial joins, socket rejoins and same-socket channel recovery:

```python
state = None

def install_snapshot(response):
    global state
    state = validate_snapshot(response)

await client.subscribe_to_topic(
    "room:123", on_message, on_join_response=install_snapshot
)
```

`JoinResponseCallback` is exported from the package. The hook receives the decoded
successful `payload.response`; a missing response is `None`. The transport adds no
application schema or empty snapshot. Installation runs synchronously before
`is_topic_joined()` becomes true, subscribe completes, or later deltas deliver.
Subscribe returns `None`. Success admits the subscription; a later disconnect or
channel failure can already have changed its current readiness when the caller
resumes.

Hooks must be short, nonblocking and return `None`. Do not await, perform I/O,
wait on locks, defer installation, or change task/client lifecycle inside them.
Async callables, awaitable results and other non-`None` results fail admission.
The join timeout bounds reply waiting and cannot preempt synchronous code. Hook
exceptions (including directly raised `CancelledError`) fail with `PHXTopicError`
and preserve their cause; arbitrary prior hook effects are not rolled back.
Initial failure belongs to the caller. An established subscription whose installer
fails is removed and reported once through `on_topic_lost`.

Initial cancellation, timeout and leave remove the exact subscription. Physical
cleanup remains owned if its waiter is canceled again, and shutdown awaits it.
Never-established subscriptions are not adopted by automatic recovery. Recorded
queue loss before a hook-enabled installation fails the attempt transiently;
steady-state queues still drop their oldest message at capacity. There is no
replay guarantee. A fresh client must install a fresh snapshot after process loss.

Join replies match request refs on both protocols. V2 also filters stale channel
generations by `join_ref`. V1 cannot reliably fence unsolicited events/lifecycle
frames from older channels; it does not provide V2 event-generation isolation.

### Optional reconnect participation

`subscribe_to_topic(topic, async_callback=None, *, on_join_response=None,
blocks_reconnect=True) -> None` preserves required recovery by default. Required
topics reconcile their first socket-rejoin attempt sequentially before
`on_reconnect`; transient failures continue through existing recovery/backoff.
The barrier does not wait for eventual success of every required topic.

Set `blocks_reconnect=False` for optional subscriptions. After required first
attempts, their existing per-topic recovery tasks perform an immediate join and
retry transient failures with the existing backoff. Required event delivery and
`on_reconnect` do not wait for optional replies, old callback drains or retries.
This option cannot isolate synchronous code that blocks the event loop. Explicit
initial subscribe always waits for its own join response and installation.

`on_reconnect` conveys no optional readiness or registration-absence guarantee.
Check `is_topic_joined()` for current readiness and use `on_topic_lost` to replace
permanently rejected optional subscriptions; do not blindly resubscribe them
from `on_reconnect`. Leave, shutdown and another socket disconnect cancel/await
the owned recovery; obsolete generations cannot install or revive a replacement.
