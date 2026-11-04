from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, NoReturn, cast

import pytest
from websockets import ClientConnection

from phoenix_channels_python_client.codec import encode_message
from phoenix_channels_python_client.messages import Message
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.protocol_handler import PHXProtocolHandler
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import make_message
from tests.harness import make_subscription
from tests.support import EVENT, TOPIC

V1 = ProtocolVersion.V1
V2 = ProtocolVersion.V2


@dataclass
class ScriptedConnection:
    """A websocket that yields ``incoming`` frames, then ends, and records sends."""

    incoming: list[str | bytes] = field(default_factory=list)
    sent: list[str | bytes] = field(default_factory=list)

    def __aiter__(self) -> ScriptedConnection:
        return self

    async def __anext__(self) -> str | bytes:
        if not self.incoming:
            raise StopAsyncIteration
        return self.incoming.pop(0)

    async def send(self, text: str | bytes) -> None:
        self.sent.append(text)


class QueueEmptiedWhileFull(asyncio.Queue[Message]):
    """Reports full, but is empty by the time the oldest message is dropped."""

    def full(self) -> bool:
        return True

    def get_nowait(self) -> NoReturn:
        raise asyncio.QueueEmpty

    async def put(self, item: Message) -> None:
        self._put(item)


def v2_frame(join_ref: str, payload: dict[str, object] | None = None) -> str:
    return json.dumps([join_ref, "1", TOPIC, EVENT, payload or {}])


async def route(
    frames: list[str | bytes],
    queue: asyncio.Queue[Any],
    *,
    join_ref: str,
    generation: int,
) -> None:
    """Route ``frames`` to a ``TOPIC`` subscription on connection ``generation``."""
    subscription = make_subscription(join_ref=join_ref, queue=queue, conn_generation=2)
    await PHXProtocolHandler(V2).process_websocket_messages(
        cast(ClientConnection, ScriptedConnection(frames)),
        {TOPIC: subscription},
        conn_generation=generation,
    )


@pytest.mark.parametrize("version", ["unsupported", "1.0.0", "2.0.0", "", " 2.0"])
def test_an_unknown_version_is_rejected(version: str) -> None:
    with pytest.raises(ValueError, match="not a valid ProtocolVersion"):
        PHXProtocolHandler(version)


async def test_sending_a_message_writes_one_frame() -> None:
    handler = PHXProtocolHandler(V2)
    connection = ScriptedConnection()
    message = make_message(event=EVENT, topic=TOPIC, join_ref="jr", ref="1")

    await handler.send_message(cast(ClientConnection, connection), message)

    assert connection.sent == [encode_message(message, V2)]


async def test_routing_drops_messages_from_an_older_connection() -> None:
    queue: asyncio.Queue[Any] = asyncio.Queue()

    await route([v2_frame("jr")], queue, join_ref="jr", generation=1)

    assert queue.empty()


async def test_routing_into_a_full_queue_that_empties_meanwhile_drops_nothing() -> None:
    queue = QueueEmptiedWhileFull()
    subscription = make_subscription(join_ref="jr", queue=queue)

    await PHXProtocolHandler(V2).process_websocket_messages(
        cast(ClientConnection, ScriptedConnection([v2_frame("jr")])),
        {TOPIC: subscription},
        conn_generation=subscription.conn_generation,
    )

    assert subscription.dropped_message_count == 0


async def test_routing_a_binary_broadcast_uses_the_absent_join_ref_rule() -> None:
    queue: asyncio.Queue[Message] = asyncio.Queue()
    await route(
        [b"\x02\x0a\x0ctest-topiccustom_event\xff"],
        queue,
        join_ref="current",
        generation=2,
    )
    assert (await queue.get()).payload == b"\xff"


async def test_routing_a_binary_push_drops_a_stale_join_ref() -> None:
    queue: asyncio.Queue[Message] = asyncio.Queue()
    await route(
        [b"\x00\x03\x0a\x0coldtest-topiccustom_event\xff"],
        queue,
        join_ref="current",
        generation=2,
    )
    assert queue.empty()


async def test_binary_reply_status_reaches_control_matching_before_the_queue() -> None:
    queue: asyncio.Queue[Message] = asyncio.Queue()
    subscription = make_subscription(join_ref="jr", queue=queue)
    matched: list[tuple[Message, str | None]] = []

    def control(
        topic: TopicSubscription, message: Message, binary_status: str | None = None
    ) -> bool:
        assert topic is subscription
        matched.append((message, binary_status))
        return True

    await PHXProtocolHandler(V2).process_websocket_messages(
        cast(
            ClientConnection,
            ScriptedConnection([b"\x01\x02\x02\x0a\x06jrr1test-topiccustom\xff"]),
        ),
        {TOPIC: subscription},
        conn_generation=subscription.conn_generation,
        on_channel_control=control,
    )
    assert [(message.ref, message.payload, status) for message, status in matched] == [
        ("r1", b"\xff", "custom")
    ]
    assert queue.empty()
