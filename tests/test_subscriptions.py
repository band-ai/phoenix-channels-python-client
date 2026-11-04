from __future__ import annotations

import asyncio
import asyncio.log
import gc

import pytest

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.exceptions import ChannelError, SocketError
from phoenix_channels_python_client.messages import Message, Payload
from tests.fake_server import FakePhoenixServer
from tests.support import (
    ASYNC_TIMEOUT_S,
    EVENT,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    REJECTED_TOPIC,
    TOPIC,
    TopicRecorder,
    channel_for,
    deliver,
    each_protocol,
    expect_message,
    join_channel,
    make_client,
    only_client_id,
    start_server_close,
    subscribe_with_handlers,
    wait_for_condition,
)


@each_protocol
async def test_subscribing_registers_the_topic_with_its_callback(
    client: Client, received: asyncio.Queue[Message]
) -> None:
    await join_channel(client, TOPIC, received.put)

    subscription = client.topic_subscriptions.copy()[TOPIC]
    assert subscription.name == TOPIC
    assert subscription.async_callback == received.put


async def test_subscribing_before_connecting_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    with pytest.raises(SocketError):
        await join_channel(make_client(phoenix_server), TOPIC)


async def test_subscribing_while_the_server_closes_raises_a_connection_error(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        server_close = await start_server_close(phoenix_server, client)

        with pytest.raises(SocketError):
            await join_channel(client, TOPIC)
        await asyncio.wait_for(server_close, ASYNC_TIMEOUT_S)


@each_protocol
async def test_subscribing_to_a_topic_the_server_rejects_raises(
    client: Client,
) -> None:
    with pytest.raises(ChannelError, match="unmatched topic"):
        await join_channel(client, REJECTED_TOPIC)


async def test_rejected_join_leaves_no_unretrieved_future_error(
    phoenix_server: FakePhoenixServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Run in its own frame: a live client or bound exception keeps the
    # subscription, and its futures, from being collected.
    async def subscribe_to_rejected_topic() -> None:
        async with make_client(phoenix_server) as client:
            with pytest.raises(ChannelError):
                await join_channel(client, REJECTED_TOPIC)

    await subscribe_to_rejected_topic()
    gc.collect()

    assert not [
        record for record in caplog.records if record.name == asyncio.log.logger.name
    ]


@each_protocol
async def test_subscribing_twice_to_a_topic_raises(client: Client) -> None:
    await join_channel(client, TOPIC)

    with pytest.raises(ChannelError, match="already has a live channel"):
        await join_channel(client, TOPIC)


@each_protocol
async def test_unsubscribing_removes_the_topic(client: Client) -> None:
    await join_channel(client, TOPIC)

    await channel_for(client, TOPIC).leave()

    assert TOPIC not in client.topic_subscriptions.copy()


@each_protocol
async def test_leaving_after_disconnection_is_terminal_and_idempotent(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, auto_reconnect=False) as client:
        channel = await join_channel(client, TOPIC)
        await phoenix_server.close_all_clients()
        assert await wait_for_condition(lambda: client.connection is None)

        await channel.leave()
        await channel.leave()

        assert not channel.joined
        with pytest.raises(ChannelError):
            await channel.join()


async def test_unsubscribing_while_the_server_closes_raises_a_connection_error(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await join_channel(client, TOPIC)
        server_close = await start_server_close(phoenix_server, client)

        with pytest.raises(SocketError):
            await channel_for(client, TOPIC).leave()
        await asyncio.wait_for(server_close, ASYNC_TIMEOUT_S)


@each_protocol
async def test_a_subscribed_topic_receives_server_events(
    phoenix_server: FakePhoenixServer,
    client: Client,
    received: asyncio.Queue[Message],
) -> None:
    payload = {"user_id": 123, "message": "Hello from server!"}
    await join_channel(client, TOPIC, received.put)

    await deliver(phoenix_server, client, payload=payload)

    message = await expect_message(received)
    assert message.topic == TOPIC
    assert message.event == EVENT
    assert message.payload == payload


async def test_a_v2_broadcast_without_join_ref_reaches_handlers(
    phoenix_server: FakePhoenixServer,
    subscribed_handlers: TopicRecorder,
) -> None:
    payload = {"message": "broadcast"}

    await phoenix_server.simulate_server_event(TOPIC, EVENT, payload)

    message = await expect_message(subscribed_handlers.messages)
    assert message.topic == TOPIC
    assert message.event == EVENT
    assert message.payload == payload
    assert message.join_ref is None
    assert message.ref is None
    assert await expect_message(subscribed_handlers.event_payloads) == payload


@each_protocol
async def test_a_channel_push_with_current_join_ref_reaches_handlers(
    phoenix_server: FakePhoenixServer,
    subscribed_handlers: TopicRecorder,
) -> None:
    payload = {"message": "channel push"}

    await phoenix_server.simulate_channel_push(TOPIC, EVENT, payload)

    message = await expect_message(subscribed_handlers.messages)
    assert message.topic == TOPIC
    assert message.event == EVENT
    assert message.payload == payload
    assert (
        message.join_ref
        == phoenix_server.members[(only_client_id(phoenix_server), TOPIC)]
    )
    assert message.ref is None
    assert await expect_message(subscribed_handlers.event_payloads) == payload


async def test_a_v2_channel_push_uses_each_members_join_ref(
    phoenix_server: FakePhoenixServer,
    subscribed_handlers: TopicRecorder,
) -> None:
    payload = {"message": "one push to two members"}
    async with make_client(phoenix_server) as other_client:
        # Different join histories expose accidental reuse of another member's ref.
        await join_channel(other_client, OTHER_TOPIC)
        other_handlers = await subscribe_with_handlers(other_client)

        await phoenix_server.simulate_channel_push(TOPIC, EVENT, payload)

        for recorder in (subscribed_handlers, other_handlers):
            message = await expect_message(recorder.messages)
            assert message.payload == payload
            assert (
                message.join_ref
                == recorder.client.topic_subscriptions.copy()[TOPIC].join_ref
            )
            assert await expect_message(recorder.event_payloads) == payload


@each_protocol
async def test_unsubscribing_lets_the_running_callback_finish_and_drops_queued_events(
    phoenix_server: FakePhoenixServer, client: Client
) -> None:
    queued_events = 10
    handled: list[object] = []
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()

    async def blocking_callback(message: Message) -> None:
        assert isinstance(message.payload, dict)
        handled.append(message.payload["event_id"])
        callback_started.set()
        await release_callback.wait()

    await join_channel(client, TOPIC, blocking_callback)
    for event_id in range(queued_events):
        await deliver(phoenix_server, client, payload={"event_id": event_id})
    await asyncio.wait_for(callback_started.wait(), ASYNC_TIMEOUT_S)
    subscription = client.topic_subscriptions.copy()[TOPIC]
    assert await wait_for_condition(
        lambda: subscription.queue.qsize() == queued_events - 1
    )

    unsubscribe = asyncio.create_task(channel_for(client, TOPIC).leave())
    assert await wait_for_condition(subscription.leave_requested.is_set)
    assert not unsubscribe.done()
    assert TOPIC in client.topic_subscriptions.copy()

    release_callback.set()
    await asyncio.wait_for(unsubscribe, ASYNC_TIMEOUT_S)

    assert TOPIC not in client.topic_subscriptions.copy()
    assert handled == [0]


@each_protocol
async def test_each_topic_delivers_to_its_own_callback(
    phoenix_server: FakePhoenixServer, client: Client
) -> None:
    received_a: asyncio.Queue[Message] = asyncio.Queue()
    received_b: asyncio.Queue[Message] = asyncio.Queue()
    await join_channel(client, TOPIC, received_a.put)
    await join_channel(client, OTHER_TOPIC, received_b.put)

    await deliver(phoenix_server, client, TOPIC, payload={"topic_id": "a"})
    await deliver(phoenix_server, client, OTHER_TOPIC, payload={"topic_id": "b"})

    assert (await expect_message(received_a)).payload == {"topic_id": "a"}
    assert (await expect_message(received_b)).payload == {"topic_id": "b"}
    assert received_a.empty()
    assert received_b.empty()


@each_protocol
async def test_messages_are_delivered_in_order(
    phoenix_server: FakePhoenixServer,
    client: Client,
    received: asyncio.Queue[Message],
) -> None:
    sequence = list(range(5))
    await join_channel(client, TOPIC, received.put)

    for sequence_id in sequence:
        await deliver(phoenix_server, client, payload={"sequence_id": sequence_id})

    delivered = []
    for _ in sequence:
        message = await expect_message(received)
        assert isinstance(message.payload, dict)
        delivered.append(message.payload["sequence_id"])
    assert delivered == sequence


@each_protocol
async def test_shutdown_unsubscribes_every_topic_and_closes_the_connection(
    client: Client,
) -> None:
    await join_channel(client, TOPIC)
    await join_channel(client, OTHER_TOPIC)

    await client.shutdown("test shutdown")

    assert client.topic_subscriptions.copy() == {}
    assert client.connection is None


@each_protocol
async def test_an_event_handler_runs_alongside_the_topic_callback_until_removed(
    phoenix_server: FakePhoenixServer,
    client: Client,
    received: asyncio.Queue[Message],
) -> None:
    event = EVENT
    handled_payloads: asyncio.Queue[Payload] = asyncio.Queue()
    await join_channel(client, TOPIC, received.put)

    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    assert handled_payloads.empty()

    channel_for(client, TOPIC).on(event, handled_payloads.put)
    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    await expect_message(handled_payloads)

    channel_for(client, TOPIC).off(event)
    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    assert handled_payloads.empty()


async def handle_payload(payload: Payload) -> None:
    pass


async def handle_message(message: Message) -> None:
    pass


async def test_an_unjoined_channel_accepts_handler_registration_and_removal(
    client: Client,
) -> None:
    channel = client.channel(TOPIC)
    assert channel.on(EVENT, handle_payload) is handle_payload
    assert channel.on_any(handle_message) is handle_message
    channel.off(EVENT)
    channel.off(EVENT)
    channel.off_any()
    channel.off_any()
    assert not channel.joined


async def test_leaving_a_created_channel_is_idempotent(client: Client) -> None:
    channel = client.channel(TOPIC)
    await channel.leave()
    await channel.leave()
    with pytest.raises(ChannelError):
        await channel.join()


async def test_a_join_the_server_never_answers_times_out(
    phoenix_server: FakePhoenixServer,
) -> None:
    phoenix_server.unanswered_join_ids.add(phoenix_server.next_client_id)

    async with make_client(phoenix_server, join_timeout_s=JOIN_TIMEOUT_S) as client:
        with pytest.raises(ChannelError, match="Timed out"):
            await join_channel(client, TOPIC)

        assert not channel_for(client, TOPIC).joined
        with pytest.raises(ChannelError):
            await client.channel(TOPIC).join()


@each_protocol
async def test_a_failing_callback_skips_its_event_handler_and_later_messages_arrive(
    phoenix_server: FakePhoenixServer,
    client: Client,
    received: asyncio.Queue[Message],
) -> None:
    handled_payloads: asyncio.Queue[Payload] = asyncio.Queue()

    async def fail_on_the_first_message(message: Message) -> None:
        assert isinstance(message.payload, dict)
        if message.payload["n"] == 0:
            raise RuntimeError("callback boom")
        await received.put(message)

    await join_channel(client, TOPIC, fail_on_the_first_message)
    channel_for(client, TOPIC).on(EVENT, handled_payloads.put)

    await deliver(phoenix_server, client, payload={"n": 0})
    await deliver(phoenix_server, client, payload={"n": 1})

    assert (await expect_message(received)).payload == {"n": 1}
    assert await expect_message(handled_payloads) == {"n": 1}


@each_protocol
async def test_an_event_handler_runs_without_a_topic_callback(
    phoenix_server: FakePhoenixServer, client: Client
) -> None:
    handled_payloads: asyncio.Queue[Payload] = asyncio.Queue()
    await join_channel(client, TOPIC)
    channel_for(client, TOPIC).on(EVENT, handled_payloads.put)

    await deliver(phoenix_server, client, payload={"n": 0})

    assert await expect_message(handled_payloads) == {"n": 0}


@each_protocol
async def test_a_full_topic_queue_drops_its_oldest_message(
    phoenix_server: FakePhoenixServer,
) -> None:
    handled: list[object] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def held_on_the_first_message(message: Message) -> None:
        assert isinstance(message.payload, dict)
        handled.append(message.payload["id"])
        if not first_started.is_set():
            first_started.set()
            await release_first.wait()

    async with make_client(phoenix_server, max_topic_queue_size=1) as client:
        await join_channel(client, TOPIC, held_on_the_first_message)
        first_id, later_ids = 1, (2, 3, 4)
        await deliver(phoenix_server, client, payload={"id": first_id})
        await asyncio.wait_for(first_started.wait(), ASYNC_TIMEOUT_S)

        # With one queue slot, each later message pushes out the one before it.
        for message_id in later_ids:
            await deliver(phoenix_server, client, payload={"id": message_id})
        subscription = client.topic_subscriptions.copy()[TOPIC]
        assert await wait_for_condition(
            lambda: subscription.dropped_message_count == len(later_ids) - 1
        )
        release_first.set()

        assert await wait_for_condition(lambda: handled == [first_id, later_ids[-1]])


async def test_a_message_from_an_older_join_is_not_delivered(
    phoenix_server: FakePhoenixServer,
    subscribed_handlers: TopicRecorder,
) -> None:
    await phoenix_server.simulate_server_event(
        TOPIC, EVENT, {"join": "older"}, join_ref="older-join"
    )
    await phoenix_server.simulate_channel_push(TOPIC, EVENT, {"join": "current"})

    assert (await expect_message(subscribed_handlers.messages)).payload == {
        "join": "current"
    }
    assert await expect_message(subscribed_handlers.event_payloads) == {
        "join": "current"
    }
    assert subscribed_handlers.deliveries == [({"join": "current"}, True)]


async def test_a_message_without_handlers_is_skipped_and_later_ones_still_arrive(
    phoenix_server: FakePhoenixServer,
    client: Client,
    received: asyncio.Queue[Message],
    caplog: pytest.LogCaptureFixture,
) -> None:
    await join_channel(client, TOPIC)
    await deliver(phoenix_server, client, payload={"n": 0})
    # The skip is only visible as this warning; wait for it before adding a handler.
    assert await wait_for_condition(lambda: "No handler found" in caplog.text)

    channel_for(client, TOPIC).on_any(received.put)
    await deliver(phoenix_server, client, payload={"n": 1})

    assert (await expect_message(received)).payload == {"n": 1}
