from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from pydantic import ValidationError

from phoenix_channels_python_client import (
    ChannelError,
    Client,
    Message,
    Payload,
    ProtocolVersion,
    Reply,
    SocketError,
)
from tests.binary_support import BinaryFrame, BinaryPeer
from tests.fake_server import WireEvent
from tests.join_support import deadline
from tests.push_support import PushPeer, pending_push
from tests.support import EVENT, OTHER_TOPIC, TOPIC, each_protocol, make_client

# Opaque protobuf wire bytes: field 1 is "hello", field 2 is 150; the client
# transports them without depending on a schema or reserializing their bytes.
PROTOBUF_REQUEST = b"\x0a\x05hello\x10\x96\x01"
PROTOBUF_RESPONSE = b"\x0a\x05world\x10\x97\x01"
BARRIER_EVENT = "after_binary"


@pytest_asyncio.fixture
async def binary_peer() -> AsyncIterator[BinaryPeer]:
    async with BinaryPeer() as peer:
        yield peer


@pytest.mark.parametrize(
    ("payload", "response"), [(PROTOBUF_REQUEST, PROTOBUF_RESPONSE), (b"", b"")]
)
async def test_binary_push_and_custom_status_reply_preserve_opaque_bytes(
    binary_peer: BinaryPeer, payload: bytes, response: bytes
) -> None:
    async with (
        deadline(),
        make_client(binary_peer) as client,
        client.channel(TOPIC) as channel,
    ):
        _, join = await binary_peer.joined.get()
        async with pending_push(channel, payload=payload) as task:
            request = await binary_peer.binary_pushes.get()
            assert request[1] == BinaryFrame(
                join.ref, request[1].ref, TOPIC, EVENT, payload
            )
            assert request[1].ref != join.ref
            await binary_peer.binary_reply(request, response, status="protobuf-result")
            reply = await task
            assert reply.status == "protobuf-result"
            assert type(reply.response) is bytes
            assert reply.response == response
            with pytest.raises(ValidationError, match="frozen"):
                reply.response = b"changed"  # type: ignore[read-only]


async def test_binary_server_push_and_broadcast_reach_both_handler_kinds(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        messages: asyncio.Queue[Message] = asyncio.Queue()
        payloads: asyncio.Queue[Payload] = asyncio.Queue()
        channel = client.channel(TOPIC)
        channel.on_any(messages.put)
        channel.on(EVENT, payloads.put)
        async with channel:
            connection, join = await binary_peer.joined.get()
            await binary_peer.binary_push(
                connection, TOPIC, EVENT, PROTOBUF_REQUEST, join_ref=join.ref
            )
            pushed = await messages.get()
            assert (pushed.join_ref, pushed.ref, pushed.payload) == (
                join.ref,
                None,
                PROTOBUF_REQUEST,
            )
            assert await payloads.get() == PROTOBUF_REQUEST
            await binary_peer.binary_broadcast(connection, TOPIC, EVENT, b"")
            broadcast = await messages.get()
            assert (broadcast.join_ref, broadcast.ref, broadcast.payload) == (
                None,
                None,
                b"",
            )
            assert await payloads.get() == b""


async def test_stale_binary_membership_is_dropped_before_a_current_push(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        received: asyncio.Queue[Message] = asyncio.Queue()
        channel = client.channel(TOPIC)
        channel.on_any(received.put)
        async with channel:
            connection, join = await binary_peer.joined.get()
            await binary_peer.binary_push(
                connection, TOPIC, EVENT, b"stale", join_ref="old-membership"
            )
            await binary_peer.binary_push(
                connection, TOPIC, BARRIER_EVENT, b"current", join_ref=join.ref
            )
            message = await received.get()
            assert (message.event, message.payload) == (BARRIER_EVENT, b"current")
            assert received.empty()


async def test_an_unmatched_binary_reply_routes_its_bytes_without_status_metadata(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        received: asyncio.Queue[Message] = asyncio.Queue()
        channel = client.channel(TOPIC)
        channel.on_any(received.put)
        async with channel:
            connection, join = await binary_peer.joined.get()
            request = BinaryFrame(join.ref, "unmatched", TOPIC, EVENT, b"")
            await binary_peer.binary_reply(
                (connection, request), PROTOBUF_RESPONSE, status="custom-status"
            )
            message = await received.get()
            assert (message.event, message.join_ref, message.ref, message.payload) == (
                WireEvent.REPLY,
                join.ref,
                "unmatched",
                PROTOBUF_RESPONSE,
            )
            assert not hasattr(message, "status")


async def test_a_binary_join_ack_fails_only_that_channel(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        neighbor = client.channel(OTHER_TOPIC)
        await neighbor.join()
        binary_peer.defer_join_targets.add((1, TOPIC))
        channel = client.channel(TOPIC)
        joining = asyncio.create_task(channel.join())
        try:
            request = await binary_peer.deferred_joins.get()
            await binary_peer.binary_reply(request, b"not a JSON acknowledgement")
            with pytest.raises(ChannelError, match="JSON object"):
                await joining
            assert not channel.joined
            async with pending_push(neighbor) as task:
                await binary_peer.answer_push(await binary_peer.pushes.get())
                assert (await task).status == "ok"
            assert neighbor.joined
        finally:
            joining.cancel()
            await asyncio.gather(joining, return_exceptions=True)


async def test_a_binary_leave_ack_does_not_end_the_neighboring_channel(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        channel, neighbor = client.channel(TOPIC), client.channel(OTHER_TOPIC)
        await channel.join()
        await neighbor.join()
        binary_peer.defer_leaves = True
        leaving = asyncio.create_task(channel.leave())
        try:
            request = await binary_peer.deferred_leaves.get()
            await binary_peer.binary_reply(request, b"not a JSON acknowledgement")
            with pytest.raises(ChannelError, match="acknowledgement rejected"):
                await leaving
            assert not channel.joined
            async with pending_push(neighbor) as task:
                await binary_peer.answer_push(await binary_peer.pushes.get())
                assert (await task).status == "ok"
            assert neighbor.joined
        finally:
            binary_peer.defer_leaves = False
            leaving.cancel()
            await asyncio.gather(leaving, return_exceptions=True)


async def test_undecodable_binary_wire_fails_pending_push_with_decode_cause(
    binary_peer: BinaryPeer,
) -> None:
    async with (
        deadline(),
        make_client(binary_peer, auto_reconnect=False) as client,
    ):
        channel = client.channel(TOPIC)
        await channel.join()
        async with pending_push(channel, payload=PROTOBUF_REQUEST) as task:
            connection, _ = await binary_peer.binary_pushes.get()
            await connection.send(b"\xff")
            with pytest.raises(SocketError) as failure:
                await task
            assert isinstance(failure.value.__cause__, ValueError)
            assert not channel.joined
        await client.run_forever(install_signal_handlers=False)


@pytest.mark.parametrize("payload", ["text", bytearray(b"bytes"), memoryview(b"bytes")])
def test_reply_response_rejects_implicit_bytes_coercion(payload: object) -> None:
    constructor: Any = Reply
    with pytest.raises(TypeError, match="Payload"):
        constructor(status="ok", response=payload)


@each_protocol
@pytest.mark.parametrize("payload", ["text", bytearray(b"bytes"), memoryview(b"bytes")])
async def test_push_rejects_payloads_that_would_need_implicit_bytes_coercion(
    client: Client, payload: object
) -> None:
    async with client.channel(TOPIC) as channel:
        invalid_payload: Any = payload
        with pytest.raises(TypeError, match="Payload"):
            await channel.push(EVENT, invalid_payload)
        assert channel.joined
        assert channel.subscription.pending_pushes == {}


async def test_v1_push_rejects_bytes_without_losing_its_json_channel() -> None:
    async with (
        deadline(),
        PushPeer(ProtocolVersion.V1) as peer,
        make_client(peer) as client,
        client.channel(TOPIC) as channel,
    ):
        with pytest.raises(TypeError, match="V2"):
            await channel.push(EVENT, PROTOBUF_REQUEST)
        assert channel.joined
        assert channel.subscription.pending_pushes == {}
        async with pending_push(channel) as task:
            await peer.answer_push(await peer.pushes.get())
            assert (await task).response == {}


async def test_binary_reply_membership_fences_precede_correlation(
    binary_peer: BinaryPeer,
) -> None:
    async with (
        deadline(),
        make_client(binary_peer) as client,
        client.channel(TOPIC) as channel,
        pending_push(channel, payload=PROTOBUF_REQUEST) as task,
    ):
        connection, request = await binary_peer.binary_pushes.get()
        await binary_peer.binary_reply(
            (connection, request._replace(join_ref="old-membership")),
            b"stale",
            status="stale",
        )
        await binary_peer.binary_reply(
            (connection, request._replace(join_ref=None)),
            PROTOBUF_RESPONSE,
            status="current",
        )
        reply = await task
        assert (reply.status, reply.response) == ("current", PROTOBUF_RESPONSE)


async def test_a_binary_handler_can_push_and_receive_a_binary_reply_on_its_own_topic(
    binary_peer: BinaryPeer,
) -> None:
    async with deadline(), make_client(binary_peer) as client:
        completed: asyncio.Queue[Reply] = asyncio.Queue()
        channel = client.channel(TOPIC)

        @channel.on(EVENT)
        async def receive(payload: Payload) -> None:
            assert isinstance(payload, bytes)
            completed.put_nowait(await channel.push("protobuf-request", payload))

        async with channel:
            connection, join = await binary_peer.joined.get()
            await binary_peer.binary_push(
                connection, TOPIC, EVENT, PROTOBUF_REQUEST, join_ref=join.ref
            )
            request = await binary_peer.binary_pushes.get()
            assert (request[1].event, request[1].payload) == (
                "protobuf-request",
                PROTOBUF_REQUEST,
            )
            await binary_peer.binary_reply(request, PROTOBUF_RESPONSE)
            assert (await completed.get()).response == PROTOBUF_RESPONSE
