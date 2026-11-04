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

## Public API migration

The public names have changed without compatibility aliases:

| Previous name | Current name |
| --- | --- |
| `PHXChannelsClient` | `Client` |
| `PHXClientError` | `ClientError` |
| `PHXTopicError` | `ChannelError` |
| `PHXConnectionError` | `SocketError` |
| `PhoenixChannelsProtocolVersion` | `ProtocolVersion` |
| `PHXEvent` | `SystemEvent` |
| `ChannelMessage` | `Message` |
| `phx_messages` module | `messages` module |
| `Client(url, key)` | `Client(url, api_key=key)` |
| `setup_logging(level)` | Standard-library logging configuration (see [Logging](#logging)) |
| `PHXProtocolHandler` | Internal protocol implementation; no public replacement |
| `get_protocol_handler()` | Removed; protocol parsing and serialization are internal |
| `Event` | Use `str` or `SystemEvent` for event names |
| `__author__` | Removed; no public replacement |

Import public types from `phoenix_channels_python_client`. `Message` is an
annotation/result union, not a model constructor. The installed package ships
`py.typed` for type checkers.
`SystemEvent` is a `StrEnum`, so members compare directly with their wire strings.

Create a `Channel`, register handlers, then join it. Retain the handle for
readiness, handler changes and cleanup; the topic-string client methods below
have been removed without compatibility aliases.

| Previous method | Channel replacement |
| --- | --- |
| `subscribe_to_topic(topic, callback)` | `room = client.channel(topic)`; `room.on_any(callback)`; `await room.join()` or `async with room:` |
| `unsubscribe_from_topic(topic)` | `await room.leave()` |
| `is_topic_joined(topic)` | `room.joined` |
| `add_event_handler(topic, event, handler)` | `room.on(event, handler)` or `@room.on(event)` |
| `remove_event_handler(topic, event)` | `room.off(event)` |
| `set_message_handler(topic, handler)` | `room.on_any(handler)` or `@room.on_any` |
| `remove_message_handler(topic)` | `room.off_any()` |
| `get_current_subscriptions()` | Retain your channel handles; no replacement collection getter |
| `get_event_handler`, `has_event_handler`, `list_event_handlers` | Retain your handlers; no replacement introspection methods |
| `get_message_handler`, `has_message_handler` | Retain your handlers; no replacement introspection methods |

## Quick Start

Create the channel, register its handlers, then join with `async with`:

```python
import asyncio

from phoenix_channels_python_client import Client, Payload

async def main() -> None:
    async with Client(
        "ws://localhost:4000/socket/websocket", api_key="your-api-key"
    ) as client:
        room = client.channel("room:lobby")

        @room.on("new_msg")
        async def handle_message(payload: Payload) -> None:
            if isinstance(payload, bytes):
                print(f"Received binary: {payload.hex()}")
            else:
                print(f"Received: {payload}")

        async with room:
            await client.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

`client.channel(...)` is synchronous, returns an unjoined `Channel`, and sends
nothing. Handlers registered before entry are ready for the first event after
the join reply. Application queues still drop their oldest event at capacity,
and recovery can discard undelivered events; this is not a lossless stream.

Entering `async with room:` joins; exiting leaves, including on an exception or
cancellation. With the default `run_forever()`, Ctrl+C or SIGTERM stops the client.
The previous signal handlers are restored before a second signal is handled.

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

See [`examples/embedded_host_signals.py`](https://github.com/band-ai/phoenix-channels-python-client/blob/main/examples/embedded_host_signals.py) for a complete program.

From a plain `signal.signal` handler or another thread, use
`loop.call_soon_threadsafe(...)` instead: a bare `asyncio.create_task` there
doesn't wake a loop blocked on I/O, so the shutdown can stall until the next
network activity.

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
- `channel.on(...)` raises `ValueError` for `SystemEvent.error` and
  `SystemEvent.close`. Use `on_topic_lost` and `channel.joined` instead.

## Protocol Versions

Phoenix Channels supports two protocol versions. Choose based on your Phoenix server version:

### Protocol v2.0 (Default)
```python
from phoenix_channels_python_client import Client

async with Client(
    websocket_url="ws://localhost:4000/socket/websocket",
    api_key="your-api-key"
    # protocol_version defaults to v2.0
) as client:
    ...  # your code here
```

### Protocol v1.0
```python
from phoenix_channels_python_client import Client, ProtocolVersion

async with Client(
    websocket_url="ws://localhost:4000/socket/websocket", 
    api_key="your-api-key",
    protocol_version=ProtocolVersion.V1
) as client:
    ...  # your code here
```

The client accepts `ProtocolVersion.V1`, `ProtocolVersion.V2`, or their exact
string values `"1.0"` and `"2.0"`. Other values raise `ValueError`. It adds
`vsn=2.0.0` (V2) or `vsn=1.0.0` (V1) to the socket URL.

## Usage Examples

### Joining a channel

Use `on_any` when you need the complete `Message`:

```python
import asyncio

from phoenix_channels_python_client import Client, Message

async def main() -> None:
    async with Client("ws://localhost:4000/socket/websocket", api_key="api-key") as client:
        room = client.channel("chat:general")

        @room.on_any
        async def message_handler(message: Message) -> None:
            print(f"Topic: {message.topic}")
            print(f"Event: {message.event}")
            print(f"Payload: {message.payload!r}")

        async with room:
            await client.run_forever()

asyncio.run(main())
```

## Catch-all vs event handlers

A channel supports one catch-all handler and one handler per event:

| Registration | Handler receives | Removal |
| --- | --- | --- |
| `@room.on_any` or `room.on_any(handler)` | Full `Message` | `room.off_any()` |
| `@room.on("new_msg")` or `room.on("new_msg", handler)` | `Payload` (dict or bytes) | `room.off("new_msg")` |

Both registration forms return the handler unchanged. Registering again replaces
the previous handler for that slot. Handlers must be async functions. `Payload`
is the public alias `dict[str, Any] | bytes`; narrow it before accessing dict
fields or decoding application bytes.

For each dispatched event, the catch-all runs first, then its event-specific
handler. Handler selection is captured for that dispatch: changes apply to later
dispatches, while teardown or cancellation can interrupt the current one. You may
add or remove handlers before joining or while the channel is active. Removal is
idempotent; it does not leave the channel.

### Event-Specific Handlers

Register event decorators before joining:

```python
import asyncio

from phoenix_channels_python_client import Client, Payload

async def main() -> None:
    async with Client("ws://localhost:4000/socket/websocket", api_key="api-key") as client:
        room = client.channel("room:lobby")

        @room.on("user_join")
        async def handle_user_join(payload: Payload) -> None:
            if isinstance(payload, bytes):
                print(f"Binary join notice: {payload.hex()}")
            else:
                print(f"User joined: {payload}")

        @room.on("user_leave")
        async def handle_user_leave(payload: Payload) -> None:
            if isinstance(payload, bytes):
                print(f"Binary leave notice: {payload.hex()}")
            else:
                print(f"User left: {payload}")

        async with room:
            await client.run_forever()

asyncio.run(main())
```

## Leaving a channel

Leaving a channel stops delivery and cleans up its membership. Prefer a context
manager so exceptions and cancellation also trigger cleanup:

```python
import asyncio

from phoenix_channels_python_client import Client, Message

async def main() -> None:
    async with Client("ws://localhost:4000/socket/websocket", api_key="api-key") as client:
        room = client.channel("room:lobby")

        @room.on_any
        async def on_message(message: Message) -> None:
            print(f"Message: {message.payload!r}")

        async with room:
            await asyncio.sleep(5)

        await room.leave()  # Repeated leave is harmless.
        print(f"Left {room.topic}; joined={room.joined}")

asyncio.run(main())
```

For manual ownership, call `await room.join()` and ensure `await room.leave()`
runs in your cleanup path. A left handle cannot join again; create a new channel.
`room.joined` is false during leave or recovery, and `room.topic` remains available.
A successful join confirms its reply was installed; a later server failure can
already have changed readiness by the time your code resumes.

Join rejection and timeout raise `ChannelError`; joining without a connected
client raises `SocketError`. A second live channel for the same topic raises
`ChannelError`. Duplicate/disconnected attempts rejected before admission leave
the created handle usable. A failed admitted initial join retires that handle.

Leave is idempotent, including while joining, rejoining or disconnected. A live
membership sends a JSON leave and uses `leave_timeout_s`; failure can raise
`ChannelError` or `SocketError`. Cleanup remains owned if its waiter is cancelled.
If cleanup cannot confirm that the old membership ended, its topic can stay
reserved until a terminal control or connection end makes reuse safe. A new handle
for that topic then raises `ChannelError`; unrelated channels remain usable.

### Join parameters

`params` follows Phoenix's name for the join payload. Pass a dict, `None` for `{}`,
or a synchronous callable returning a dict:

```python
room = client.channel("room:lobby", params={"locale": "en"})
room = client.channel("room:private", params=lambda: {"token": fresh_token()})
```

The callable is evaluated once for each actual initial join or rejoin, allowing
fresh credentials after reconnect. Creation alone does not call it or send a
frame. Providers must be nonblocking and return a JSON-serializable dict;
provider/encoding failure raises `ChannelError` with its cause. A provider failure
during recovery permanently loses only that channel instead of retrying forever.

### Ordered join responses

Pass `on_join_response` when creating a channel to install a snapshot from every
initial join, socket rejoin and same-socket recovery:

```python
state = None

def install_snapshot(response):
    global state
    state = validate_snapshot(response)

room = client.channel("room:123", on_join_response=install_snapshot)
room.on_any(on_message)
async with room:
    await client.run_forever()
```

`JoinResponseCallback` is exported from the package. The hook receives decoded
successful `payload.response`; a missing response is `None`. It runs synchronously
before the channel becomes joined, the join completes, or later events are
delivered. The client adds no application schema or empty snapshot. Join and leave
replies are handled separately from the bounded application-event queue.

Hooks must be short, nonblocking and return `None`. Async callables, awaitables
and other non-`None` results fail installation with `ChannelError`, as do hook
exceptions (including a directly raised `CancelledError`); the original cause is
preserved. Arbitrary prior hook effects are not rolled back. Do not wait on locks,
perform I/O, defer installation or change client lifecycle inside the hook. An
asyncio deadline cannot preempt synchronous code that blocks the loop.

Initial failure retires the admitted handle. Installation failure on an established
channel is reported once through `on_topic_lost`. Queue overflow and recovery can
still discard application events, so applications needing durable state must
resynchronize rather than assume replay.

Join replies match request refs on both protocols. V2 additionally checks a
present `join_ref`; a missing join_ref is accepted, including for broadcasts.
Membership controls use their available refs for correlation. V1 cannot provide
V2 isolation for arbitrary unsolicited events from old memberships.

## Pushing Events

Use `await channel.push(event, payload, timeout_s=10.0)` to send an application
event and wait for its matching reply. Pass a JSON-serializable dict or, on V2,
`bytes` (see [Binary payloads](#binary-payloads)). `timeout_s` must be finite and
greater than zero. The default is 10 seconds.

```python
import asyncio

from phoenix_channels_python_client import Client, Reply

async def main() -> None:
    async with Client("ws://localhost:4000/socket/websocket", api_key="api-key") as client:
        room = client.channel("room:lobby")
        async with room:
            reply: Reply = await room.push("new_msg", {"body": "Hello"}, timeout_s=5.0)
            if reply.status == "ok":
                print(reply.response)
            else:
                print(f"Server replied {reply.status}: {reply.response!r}")

asyncio.run(main())
```

`Reply` is a frozen Pydantic model with `status: str` and
`response: Payload`. A text reply has a dict response; a binary reply has a bytes
response. Status values are server-defined: an `"error"` reply
is returned as a `Reply`, without raising `ChannelError`.

| Situation | Result |
| --- | --- |
| Channel is not joined, including while rejoining | `ChannelError` immediately, even if the socket is also unavailable |
| Connection is lost after joined admission, before or during send/wait | `SocketError` |
| Deadline expires without a reply | `PushTimeoutError` (a `ChannelError` subclass) |
| Channel crashes, closes, is explicitly left, or has a local processing failure | `ChannelError` |
| Client is explicitly shut down while a push is pending | `ChannelError` |
| Server returns an error status | `Reply` with that status and response |
| Caller is cancelled | `asyncio.CancelledError` |

A malformed matching reply raises `ChannelError` with the validation cause.
Invalid payload encoding raises its validation or encoding exception before
submission. A timeout after a completed send affects only that push. If a write
remains stalled at its deadline, the client closes that socket; other pending
pushes fail with `SocketError` and normal reconnect policy applies. An already
settled reply or failure keeps its outcome.

Pushes are never buffered or replayed after recovery. A timeout or cancellation
does not establish whether the server executed the request; choose retries at
the application level.

Replies are resolved independently of topic handlers. A handler may await a
push on its own channel. `push()` does not wait for earlier messages to finish
handling, so its completion does not imply that earlier callbacks completed.
Only a reply matching a pending push is consumed by that caller; an unmatched
or late reply can reach the normal handlers, subject to current membership
checks.

### Binary payloads

V2 supports `bytes` for pushes, replies, server events and broadcasts. Dicts use
JSON text frames in both protocols. Passing bytes on V1 raises `TypeError` before
submission; the client does not encode, decode or select an application format
for you. Join parameters and lifecycle control frames remain JSON dicts.

For example, the [protobuf wire format](https://protobuf.dev/programming-guides/encoding/)
for `message Update { string body = 1; }` serializes `body="hello"` to the bytes below. With generated protobuf classes,
pass `update.SerializeToString()` and parse a bytes response with your generated
class. This example uses the wire bytes directly, so it needs no protobuf
dependency:

```python
import asyncio

from phoenix_channels_python_client import Client, ProtocolVersion

async def main() -> None:
    async with Client(
        "ws://localhost:4000/socket/websocket",
        api_key="api-key",
        protocol_version=ProtocolVersion.V2,
    ) as client:
        room = client.channel("room:lobby")
        async with room:
            encoded_update = b"\x0a\x05hello"
            reply = await room.push("update", encoded_update)
            if isinstance(reply.response, bytes):
                print(f"Protobuf response: {reply.response.hex()}")
            else:
                print(f"JSON response: {reply.response}")

asyncio.run(main())
```

Event handlers and `Message.payload` also receive raw bytes for binary server
events and broadcasts. A broadcast can have no `join_ref` and is delivered to
its current topic. The application chooses the payload decoder; binary data
is never interpreted as JSON merely because it contains JSON-looking bytes.

## Configuration

`api_key` is keyword-only and defaults to `None`. Without an
authentication header, a supplied key replaces any `api_key` in the URL; omitting
it (or passing `None`) keeps the URL credential, if present. An empty argument
(`api_key=""`) raises `ValueError`.

Connect without a credential, or keep one already in the URL:

```python
async with Client("ws://localhost:4000/socket/websocket") as client:
    ...

async with Client(
    "wss://server.com/socket/websocket?api_key=your-api-key"
) as client:
    ...
```

Only `websocket_url` may be positional; all other constructor options are keyword-only:

| Option | Default | Meaning |
|---|---|---|
| `protocol_version` | `V2` | Phoenix Channels protocol version |
| `auto_reconnect` | `True` | Reconnect and rejoin topics after a disconnect |
| `reconnect_policy` | `ReconnectPolicy()` | Backoff and close-code handling (see below) |
| `heartbeat_interval_s` | `30.0` | Heartbeat period; `None` disables heartbeats and timeout enforcement |
| `heartbeat_dead_threshold_s` | `None` | Optional finite positive acknowledgement-silence limit, greater than the enabled finite interval |
| `join_timeout_s` | `10.0` | Finite positive join-operation deadline |
| `leave_timeout_s` | `5.0` | Finite positive leave-operation deadline |
| `max_topic_queue_size` | `1000` | Per-topic buffer; the oldest message is dropped when full |
| `callback_drain_timeout_s` | `2.0` | How long the message being handled (its catch-all handler, then its event handler) may finish before a rejoin cancels it; queued messages are dropped |
| `on_reconnect` | `None` | `async () -> None`, called after required first-attempt reconciliation |
| `on_disconnect` | `None` | `async (error: Exception \| None) -> None`; `None` on a clean close |
| `on_heartbeat_ack` | `None` | Synchronous `() -> None`; runs on the message path, so keep it fast |
| `on_topic_lost` | `None` | `async (topic: str, error: Exception) -> None`, called when the client permanently loses a channel you did not leave |
| `reconnect_decider` | `None` | Synchronous `(event: ReconnectEvent) -> ReconnectDecision`; replaces the built-in reconnect policy (see [Owning the reconnect policy](#owning-the-reconnect-policy)) |
| `additional_headers` | `None` | Extra handshake headers sent on every (re)connect |

Exceptions raised in ordinary message and lifecycle callbacks are logged, not raised.
Join-response installation failures instead fail the join (see below).

## Connection Lifecycle and Errors

- `async with` waits for the first connection. With `auto_reconnect=True` it keeps retrying, except after a refused handshake with HTTP 403, which raises `SocketError`; with `auto_reconnect=False` a failed first connection raises `SocketError`.
- `run_forever()` returns `None` after `shutdown()`, a signal, or a close that doesn't reconnect. It raises `SocketError` on a terminal close, a refused handshake with HTTP 403, after repeated rapid disconnects, or when the client has never entered `async with`.
- `channel.join()` raises `ChannelError` on rejection, timeout or duplicate live membership, and `SocketError` when the client is disconnected or the connection fails before admission completes.
- `await client.close_connection(reason)` force-closes the current connection; the client then reconnects if `auto_reconnect` is on. It does nothing if the client isn't connected or the connection is already closing.
- `await client.shutdown()` uses the default reason `"Client shutdown"`; pass a
  reason to describe an explicit stop.
- `ClientError`, `ChannelError`, `SocketError` and `PushTimeoutError` are exported
  from the package and defined in `phoenix_channels_python_client.exceptions`.
  `ChannelError` and `SocketError` derive from `ClientError`; `PushTimeoutError`
  derives from `ChannelError`.

### Reconnects

| Close code or handshake | Behaviour (defaults) |
|---|---|
| 1000, 1001 | Reconnect (Phoenix ≥ 1.8.8 sends 1001 for "reconnect expected"); stop with `reconnect_on_normal_close=False` |
| 1008 | Stop with `SocketError` (`policy_violation_is_terminal=True`) |
| Handshake refused with HTTP 403 | Stop with `SocketError`; other statuses (401, 5xx, …) retry with the backoff below |
| 1012 | Reconnect after at least a random 1–5 s |
| 1013 | Reconnect after at least a random 30–60 s |
| Anything else, or no close frame | Reconnect with jittered backoff: 0.5 s × 2ⁿ, capped at 30 s, reset after 60 s of uptime |

Disconnects within 5 s of connecting count as rapid and get longer minimum delays; ten within 60 s stop the client with `SocketError`. Tune all of this with `ReconnectPolicy`.

A `ReconnectPolicy` is validated when it is built: an out-of-range or unknown field raises `pydantic.ValidationError`, which is a `ValueError`. To derive one policy from another, validate the merged fields:

```python
base = ReconnectPolicy()
policy = ReconnectPolicy.model_validate(base.model_dump() | {"max_delay_s": 10.0})
```

### Owning the reconnect policy

Pass `reconnect_decider` to make every reconnect decision yourself. While `auto_reconnect` is on, the client calls it with:

- `Connecting()` before every connection attempt, the first included;
- `Connected(generation)` when a socket is up, before any topic rejoins on it;
- `Disconnected(close_code, close_reason)` after every close. A forced close (heartbeat expiry, `close_connection()`) has `close_code=None`, since the received code is unreliable then;
- `ConnectFailed(error)` after every failed attempt, with the raw exception (a refused handshake is a `websockets` `InvalidStatus` carrying the response).

It returns a `ReconnectDecision`:

- `ReconnectDecision(should_reconnect=True, delay_s=2.0)` after `Disconnected` or `ConnectFailed` waits exactly 2 s before the next attempt; `delay_s` defaults to `0`. `Connecting` and `Connected` only proceed or stop, and a delay there stops the client with `SocketError`.
- `ReconnectDecision(should_reconnect=False)` stops; `run_forever()` returns `None`, and before the first connection `async with` raises `SocketError`. A stop at `Connected` closes that socket before any rejoin.
- `ReconnectDecision(should_reconnect=False, terminal_error=SocketError(...))` stops; `run_forever()` (or `async with`, before the first connection) raises that error.

The decider replaces the whole built-in policy above, rapid-disconnect suppression included; the built-in policy is itself a decider over the same events. It runs inline on the client's supervisor task, so keep it fast and never block. The client calls the same decider on every `async with` entry, so reset any per-run state yourself; the built-in one is rebuilt each time. If it raises, the client stops with a `SocketError` whose `__cause__` is that exception; if it returns anything but a `ReconnectDecision` (an `async def` decider returns a coroutine), the client stops with a `SocketError` too. It is not asked once `shutdown()` has begun. To end a live connection, call `close_connection()` and stop at the next decision.

```python
def decide(event: ReconnectEvent) -> ReconnectDecision:
    match event:
        case ConnectFailed(error=InvalidStatus(response=response)) if response.status_code == 403:
            return ReconnectDecision(should_reconnect=False, terminal_error=SocketError("refused"))
        case Disconnected() | ConnectFailed():
            return ReconnectDecision(should_reconnect=True, delay_s=1.0)
        case _:
            return ReconnectDecision(should_reconnect=True)
```

### Channel recovery

A channel can crash while the socket stays up. On `phx_error`, the client marks it
not joined and retries its join on that socket using the `ReconnectPolicy`
backoff. A missed rejoin reply is transient and retried. A socket disconnect hands
recovery to the next connection. Only established channels recover automatically;
a failed initial join belongs to its caller.

Rejected rejoins, invalid join params, installation failures and server
`phx_close` permanently lose that channel. The client calls
`on_topic_lost(topic, error)` once for permanent loss. Explicit leave, shutdown
and failed initial join do not call it. Create a fresh handle to replace a lost
channel after its old cleanup releases the topic.

Retain your channel handles and check `room.joined`: it is false while disconnected
or rejoining. `await room.leave()` also works during recovery and stops future
rejoin attempts. Client lifecycle callbacks stay constructor options; `shutdown()`
cancels them, including a callback that itself called `shutdown()`.

### Optional reconnect participation

`client.channel(topic, *, params=None, on_join_response=None,
blocks_reconnect=True)` creates a channel that blocks the reconnect barrier by
default. Required channels reconcile their first socket-rejoin attempt sequentially
before `on_reconnect`; transient failures continue through recovery/backoff. The
barrier does not wait for eventual success of every required channel.

Set `blocks_reconnect=False` when creating an optional channel. Its owned recovery
task attempts an immediate join after required first attempts and retries transient
failures with the same backoff. Required delivery and `on_reconnect` do not wait for
optional replies, callback drains or retries. Explicit `await room.join()` still
waits for that channel's reply and installation.

`on_reconnect` does not promise optional readiness. Check each handle's `joined`
property and use `on_topic_lost` to replace permanent losses. Do not blindly create
replacement channels on every reconnect. Leave, shutdown and another socket
disconnect stop or hand over recovery; obsolete attempts cannot revive a left
handle.

### Heartbeats

The client sends a `heartbeat` on the `phoenix` topic every `heartbeat_interval_s`. By default, missed replies log warnings and sending continues with a new reference each interval.

Set `heartbeat_dead_threshold_s` to enable closure after acknowledgement silence:

```python
client = Client(
    websocket_url, api_key=api_key,
    heartbeat_interval_s=30.0,
    heartbeat_dead_threshold_s=60.0,
)
```

The deadline starts when each socket is adopted, including during rejoin and reconnect callbacks. Only a matching pending reply received strictly before expiry renews it, before `on_heartbeat_ack` runs. Enabled enforcement keeps one outstanding heartbeat, skipping sends until it is acknowledged or expires. WebSocket Pong and unrelated messages do not renew it. Rejoin completion grants no additional grace.

Expiry or heartbeat send failure force-closes that socket; the supervisor handles cleanup and reconnect/rejoin according to `auto_reconnect` and the existing policy. The deadline bounds close initiation; the transport has its own close-handshake timeout. `heartbeat_interval_s=None` disables heartbeat activity even with a threshold, but a supplied threshold must still be finite and positive. Long thresholds do not prevent the server from applying its own inactivity timeout.

## Logging

The library uses loggers under `phoenix_channels_python_client`. Configure
logging in your application; importing the package does not configure it.
Connection log lines show `<omitted>` in place of values that may carry personal
data (see [Authentication and diagnostic safety](#authentication-and-diagnostic-safety)):

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("phoenix_channels_python_client").setLevel(logging.DEBUG)
```

## Examples

- [`examples/minimal_phx_events_client.py`](https://github.com/band-ai/phoenix-channels-python-client/blob/main/examples/minimal_phx_events_client.py): join a channel and log messages until Ctrl+C or SIGTERM.
- [`examples/embedded_host_signals.py`](https://github.com/band-ai/phoenix-channels-python-client/blob/main/examples/embedded_host_signals.py): run the client in a host that owns its process signals.

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

No `api_key` argument is needed for header authentication:

```python
async with Client(
    "wss://server.com/socket/websocket",
    additional_headers={"x-api-key": "your-api-key"},
) as client:
    ...
```

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

Connection diagnostics omit dynamic values and transport payloads, including URLs,
headers, frame bodies, user/topic identities, numbers, and untrusted server error
details. Each omitted value appears as `<omitted>`, for example
`No handler found for event <omitted> on topic <omitted>`. Logs retain static
lifecycle descriptions, enum values such as connection states, booleans, and
exception types. Application data and caller-visible errors are not changed;
applications remain responsible for logs outside this connection-owned boundary.

---

**Need help?** Open an issue on GitHub or check the [Phoenix Channels documentation](https://hexdocs.pm/phoenix/channels.html) for more information about the Phoenix Channels protocol.
