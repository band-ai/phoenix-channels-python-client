from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import (
    BufferPayload,
    ChannelMessage,
    Payload,
    PHXEvent,
    PushReply,
)
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import BinaryPush, FakePhoenixServer, ReplyStatus
from tests.support import (
    BINARY_PAYLOAD,
    EVENT,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    MAX_BINARY_FIELD_SIZE,
    OTHER_TOPIC,
    REJOINED_ONCE,
    TOPIC,
    UNICODE_TOPIC,
    deliver,
    each_protocol,
    expect_message,
    joins,
    make_client,
    reconnect,
    wait_for_condition,
)

BINARY_JOIN_PAYLOAD = b"\x00\xffjoin"
JSON_JOIN_PAYLOAD = {"token": "secret", "nested": {"n": 1}}
JOIN_PAYLOADS = [BINARY_JOIN_PAYLOAD, JSON_JOIN_PAYLOAD]
V1_ONLY = pytest.mark.parametrize(
    "protocol", [PhoenixChannelsProtocolVersion.V1], ids=["v1.0"]
)
V2_ONLY = pytest.mark.parametrize(
    "protocol", [PhoenixChannelsProtocolVersion.V2], ids=["v2.0"]
)


PushMethod = Callable[[PHXChannelsClient, str, str, Payload], Awaitable[object]]
each_push_method = pytest.mark.parametrize(
    "push",
    [PHXChannelsClient.push, PHXChannelsClient.push_nowait],
    ids=["push", "push_nowait"],
)


@V2_ONLY
async def test_a_bytes_join_payload_is_sent_as_a_binary_join_and_answered_in_binary(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    join_reply = b"\x01joined"
    phoenix_server.binary_join_responses[TOPIC] = join_reply
    installed: list[object] = []

    await client.subscribe_to_topic(
        TOPIC, join_payload=BINARY_JOIN_PAYLOAD, on_join_response=installed.append
    )

    _, join = phoenix_server.joined.get_nowait()
    join_ref = client.get_current_subscriptions()[TOPIC].join_ref
    assert (join.join_ref, join.ref) == (join_ref, join_ref)
    assert phoenix_server.received_join_payloads == [(TOPIC, BINARY_JOIN_PAYLOAD)]
    assert installed == [join_reply]
    assert client.is_topic_joined(TOPIC)


@each_protocol
async def test_a_json_join_payload_is_sent_as_text(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(TOPIC, join_payload=JSON_JOIN_PAYLOAD)

    assert phoenix_server.received_join_payloads == [(TOPIC, JSON_JOIN_PAYLOAD)]


@each_protocol
async def test_a_join_without_a_payload_sends_an_empty_object(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(TOPIC)

    assert phoenix_server.received_join_payloads == [(TOPIC, {})]


@V2_ONLY
@pytest.mark.parametrize("join_payload", JOIN_PAYLOADS)
async def test_a_rejoin_after_a_reconnect_sends_the_same_join_payload(
    phoenix_server: FakePhoenixServer, join_payload: Payload
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, join_payload=join_payload)

        await reconnect(phoenix_server, client)

        assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_ONCE)
    assert phoenix_server.received_join_payloads == [(TOPIC, join_payload)] * 2


@V2_ONLY
async def test_a_channel_recovery_sends_the_same_join_payload(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(TOPIC, join_payload=BINARY_JOIN_PAYLOAD)

    await phoenix_server.crash_channel(TOPIC)

    assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_ONCE)
    assert phoenix_server.received_join_payloads == [(TOPIC, BINARY_JOIN_PAYLOAD)] * 2


@V1_ONLY
async def test_a_bytes_join_payload_fails_the_join_on_v1(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    with pytest.raises(TypeError, match="cannot send a binary payload"):
        await client.subscribe_to_topic(TOPIC, join_payload=BINARY_JOIN_PAYLOAD)

    assert client.get_current_subscriptions() == {}
    assert phoenix_server.join_topics == []


@V2_ONLY
async def test_pushing_bytes_returns_the_binary_reply(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(TOPIC)
    join_ref = client.get_current_subscriptions()[TOPIC].join_ref

    reply = await client.push(TOPIC, EVENT, BINARY_PAYLOAD)

    assert reply == PushReply(status=ReplyStatus.OK, response=BINARY_PAYLOAD)
    _, push = await expect_message(phoenix_server.binary_pushes)
    assert push.ref is not None
    assert push == BinaryPush(join_ref, push.ref, TOPIC, EVENT, BINARY_PAYLOAD)
    assert client.get_current_subscriptions()[TOPIC].pending_pushes == {}


@V2_ONLY
@pytest.mark.parametrize(
    "payload",
    [b"", bytearray(BINARY_PAYLOAD), memoryview(BINARY_PAYLOAD)],
    ids=["empty", "bytearray", "memoryview"],
)
async def test_a_binary_payload_of_any_bytes_like_type_round_trips_as_bytes(
    client: PHXChannelsClient, payload: bytes | BufferPayload
) -> None:
    await client.subscribe_to_topic(TOPIC)

    reply = await client.push(TOPIC, EVENT, payload)

    assert reply.response == bytes(payload)
    assert isinstance(reply.response, bytes)


@V2_ONLY
async def test_a_topic_with_multi_byte_characters_round_trips_a_binary_push(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(UNICODE_TOPIC)

    reply = await client.push(UNICODE_TOPIC, EVENT, BINARY_PAYLOAD)

    assert reply.response == BINARY_PAYLOAD
    _, push = await expect_message(phoenix_server.binary_pushes)
    assert push.topic == UNICODE_TOPIC


@each_protocol
async def test_a_text_payload_is_rejected_and_leaves_nothing_pending(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(ValueError, match="valid bytes"):
        await client.push(TOPIC, EVENT, cast(Any, "text"))

    assert client.get_current_subscriptions()[TOPIC].pending_pushes == {}


@V2_ONLY
async def test_an_oversized_event_is_rejected_before_anything_is_sent(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    await client.subscribe_to_topic(TOPIC)
    oversized_event = "e" * (MAX_BINARY_FIELD_SIZE + 1)

    with pytest.raises(ValueError, match="event must be at most"):
        await client.push(TOPIC, oversized_event, BINARY_PAYLOAD)

    assert client.get_current_subscriptions()[TOPIC].pending_pushes == {}
    assert phoenix_server.binary_pushes.empty()


@each_protocol
async def test_pushing_a_json_payload_returns_the_json_reply(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)

    reply = await client.push(TOPIC, EVENT, {"question": 42})

    assert reply == PushReply(status=ReplyStatus.OK, response={"question": 42})


@each_protocol
async def test_concurrent_pushes_each_get_their_own_reply(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)

    replies = await asyncio.gather(
        *(client.push(TOPIC, EVENT, {"n": n}) for n in range(3))
    )

    assert [reply.response for reply in replies] == [{"n": 0}, {"n": 1}, {"n": 2}]


@each_protocol
async def test_a_push_reply_does_not_reach_the_topic_handlers(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    await client.subscribe_to_topic(TOPIC, received.put)

    await client.push(TOPIC, EVENT, {})
    await deliver(phoenix_server, client)

    message = await expect_message(received)
    assert message.ref is None
    assert received.empty()


@V2_ONLY
async def test_a_binary_push_nowait_reaches_the_server_without_waiting_for_a_reply(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    phoenix_server.answer_pushes = False
    await client.subscribe_to_topic(TOPIC)
    subscription = client.get_current_subscriptions()[TOPIC]

    await client.push_nowait(TOPIC, EVENT, BINARY_PAYLOAD)

    _, push = await expect_message(phoenix_server.binary_pushes)
    assert push == BinaryPush(
        subscription.join_ref, push.ref, TOPIC, EVENT, BINARY_PAYLOAD
    )
    assert push.ref is not None
    assert subscription.pending_pushes == {}


@V2_ONLY
async def test_the_reply_to_a_binary_push_nowait_reaches_the_topic_callback(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    await client.subscribe_to_topic(TOPIC, received.put)

    await client.push_nowait(TOPIC, EVENT, BINARY_PAYLOAD)

    _, push = await expect_message(phoenix_server.binary_pushes)
    reply = await expect_message(received)
    assert (reply.event, reply.ref, reply.reply_status, reply.reply_response) == (
        PHXEvent.reply,
        push.ref,
        ReplyStatus.OK,
        BINARY_PAYLOAD,
    )


@each_protocol
async def test_the_reply_to_a_json_push_nowait_reaches_the_topic_callback(
    client: PHXChannelsClient, received: asyncio.Queue[ChannelMessage]
) -> None:
    await client.subscribe_to_topic(TOPIC, received.put)

    await client.push_nowait(TOPIC, EVENT, {"question": 42})

    reply = await expect_message(received)
    assert (reply.event, reply.reply_status, reply.reply_response) == (
        PHXEvent.reply,
        ReplyStatus.OK,
        {"question": 42},
    )


@each_protocol
async def test_a_push_times_out_without_a_reply(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    phoenix_server.answer_pushes = False
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(PHXTopicError, match="Timed out waiting for the reply"):
        await client.push(TOPIC, EVENT, {}, timeout_s=JOIN_TIMEOUT_S)

    assert client.get_current_subscriptions()[TOPIC].pending_pushes == {}


@each_protocol
async def test_a_late_reply_after_a_timeout_is_ignored(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    phoenix_server.answer_pushes = False
    await client.subscribe_to_topic(TOPIC, received.put)
    with pytest.raises(PHXTopicError):
        await client.push(TOPIC, EVENT, {}, timeout_s=JOIN_TIMEOUT_S)

    phoenix_server.answer_pushes = True
    reply = await client.push(TOPIC, EVENT, {"again": True})

    assert reply.response == {"again": True}


@each_protocol
@each_push_method
async def test_pushing_to_a_topic_that_is_not_joined_raises(
    client: PHXChannelsClient, push: PushMethod
) -> None:
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(PHXTopicError, match=f"{OTHER_TOPIC} is not joined"):
        await push(client, OTHER_TOPIC, EVENT, {})


@each_protocol
@each_push_method
async def test_pushing_before_connecting_raises(
    phoenix_server: FakePhoenixServer, push: PushMethod
) -> None:
    with pytest.raises(PHXConnectionError):
        await push(make_client(phoenix_server), TOPIC, EVENT, {})


@each_protocol
async def test_pushing_with_a_non_positive_timeout_raises(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(ValueError, match="timeout_s"):
        await client.push(TOPIC, EVENT, {}, timeout_s=0)


@V1_ONLY
@each_push_method
async def test_pushing_bytes_on_v1_raises_and_leaves_nothing_pending(
    client: PHXChannelsClient, push: PushMethod
) -> None:
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(TypeError, match="cannot send a binary payload"):
        await push(client, TOPIC, EVENT, BINARY_PAYLOAD)

    assert client.get_current_subscriptions()[TOPIC].pending_pushes == {}


@each_protocol
async def test_a_push_fails_when_the_connection_drops_before_its_reply(
    phoenix_server: FakePhoenixServer,
) -> None:
    phoenix_server.answer_pushes = False
    async with make_client(phoenix_server, auto_reconnect=False) as client:
        await client.subscribe_to_topic(TOPIC)
        subscription = client.get_current_subscriptions()[TOPIC]
        pushing = asyncio.ensure_future(client.push(TOPIC, EVENT, {}))
        assert await wait_for_condition(lambda: bool(subscription.pending_pushes))

        await phoenix_server.close_all_clients()

        with pytest.raises(PHXConnectionError):
            await pushing
    assert subscription.pending_pushes == {}


@each_protocol
async def test_a_push_fails_when_the_client_shuts_down_before_its_reply(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    phoenix_server.answer_pushes = False
    await client.subscribe_to_topic(TOPIC)
    subscription = client.get_current_subscriptions()[TOPIC]
    pushing = asyncio.ensure_future(client.push(TOPIC, EVENT, {}))
    assert await wait_for_condition(lambda: bool(subscription.pending_pushes))

    await client.shutdown("test stop")

    with pytest.raises(PHXConnectionError):
        await pushing
    assert subscription.pending_pushes == {}


@each_protocol
async def test_a_push_fails_when_its_channel_crashes_before_the_reply(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    phoenix_server.answer_pushes = False
    await client.subscribe_to_topic(TOPIC)
    subscription = client.get_current_subscriptions()[TOPIC]
    pushing = asyncio.ensure_future(client.push(TOPIC, EVENT, {}))
    assert await wait_for_condition(lambda: bool(subscription.pending_pushes))

    await phoenix_server.crash_channel(TOPIC)

    with pytest.raises(PHXConnectionError, match="crashed"):
        await pushing
    assert subscription.pending_pushes == {}
