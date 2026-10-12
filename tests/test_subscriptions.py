from __future__ import annotations

import asyncio
import asyncio.log
import gc
from collections.abc import Callable

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from tests.fake_server import FakePhoenixServer
from tests.support import (
    ASYNC_TIMEOUT_S,
    EVENT,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    REJECTED_TOPIC,
    TOPIC,
    deliver,
    each_protocol,
    expect_message,
    make_client,
    start_server_close,
    wait_for_condition,
)


@each_protocol
async def test_subscribing_registers_the_topic_with_its_callback(
    client: PHXChannelsClient, received: asyncio.Queue[ChannelMessage]
) -> None:
    await client.subscribe_to_topic(TOPIC, received.put)

    subscription = client.get_current_subscriptions()[TOPIC]
    assert subscription.name == TOPIC
    assert subscription.async_callback == received.put


async def test_subscribing_before_connecting_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    with pytest.raises(PHXConnectionError):
        await make_client(phoenix_server).subscribe_to_topic(TOPIC)


async def test_subscribing_while_the_server_closes_raises_a_connection_error(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        server_close = await start_server_close(phoenix_server, client)

        with pytest.raises(PHXConnectionError):
            await client.subscribe_to_topic(TOPIC)
        await asyncio.wait_for(server_close, ASYNC_TIMEOUT_S)


@each_protocol
async def test_subscribing_to_a_topic_the_server_rejects_raises(
    client: PHXChannelsClient,
) -> None:
    with pytest.raises(PHXTopicError, match="unmatched topic"):
        await client.subscribe_to_topic(REJECTED_TOPIC)


async def test_rejected_join_leaves_no_unretrieved_future_error(
    phoenix_server: FakePhoenixServer, caplog: pytest.LogCaptureFixture
) -> None:
    # Run in its own frame: a live client or bound exception keeps the
    # subscription, and its futures, from being collected.
    async def subscribe_to_rejected_topic() -> None:
        async with make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(REJECTED_TOPIC)

    await subscribe_to_rejected_topic()
    gc.collect()

    assert not [
        record for record in caplog.records if record.name == asyncio.log.logger.name
    ]


@each_protocol
async def test_subscribing_twice_to_a_topic_raises(client: PHXChannelsClient) -> None:
    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(PHXTopicError, match=f"^Topic {TOPIC} already subscribed$"):
        await client.subscribe_to_topic(TOPIC)


@each_protocol
async def test_unsubscribing_removes_the_topic(client: PHXChannelsClient) -> None:
    await client.subscribe_to_topic(TOPIC)

    await client.unsubscribe_from_topic(TOPIC)

    assert TOPIC not in client.get_current_subscriptions()


@each_protocol
async def test_unsubscribing_while_disconnected_fails_fast_and_keeps_the_topic(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, auto_reconnect=False) as client:
        await client.subscribe_to_topic(TOPIC)
        await phoenix_server.close_all_clients()
        assert await wait_for_condition(lambda: client.connection is None)

        with pytest.raises(PHXConnectionError):
            await client.unsubscribe_from_topic(TOPIC)

        assert TOPIC in client.get_current_subscriptions()


async def test_unsubscribing_while_the_server_closes_raises_a_connection_error(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC)
        server_close = await start_server_close(phoenix_server, client)

        with pytest.raises(PHXConnectionError):
            await client.unsubscribe_from_topic(TOPIC)
        await asyncio.wait_for(server_close, ASYNC_TIMEOUT_S)


@each_protocol
async def test_a_subscribed_topic_receives_server_events(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    payload = {"user_id": 123, "message": "Hello from server!"}
    await client.subscribe_to_topic(TOPIC, received.put)

    await deliver(phoenix_server, client, payload=payload)

    message = await expect_message(received)
    assert message.topic == TOPIC
    assert message.event == EVENT
    assert message.payload == payload


@each_protocol
async def test_unsubscribing_lets_the_running_callback_finish_and_drops_queued_events(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    queued_events = 10
    handled: list[object] = []
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()

    async def blocking_callback(message: ChannelMessage) -> None:
        handled.append(message.payload["event_id"])
        callback_started.set()
        await release_callback.wait()

    await client.subscribe_to_topic(TOPIC, blocking_callback)
    for event_id in range(queued_events):
        await deliver(phoenix_server, client, payload={"event_id": event_id})
    await asyncio.wait_for(callback_started.wait(), ASYNC_TIMEOUT_S)
    subscription = client.get_current_subscriptions()[TOPIC]
    assert await wait_for_condition(
        lambda: subscription.queue.qsize() == queued_events - 1
    )

    unsubscribe = asyncio.create_task(client.unsubscribe_from_topic(TOPIC))
    assert await wait_for_condition(subscription.leave_requested.is_set)
    assert not unsubscribe.done()
    assert TOPIC in client.get_current_subscriptions()

    release_callback.set()
    await asyncio.wait_for(unsubscribe, ASYNC_TIMEOUT_S)

    assert TOPIC not in client.get_current_subscriptions()
    assert handled == [0]


@each_protocol
async def test_each_topic_delivers_to_its_own_callback(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    received_a: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    received_b: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    await client.subscribe_to_topic(TOPIC, received_a.put)
    await client.subscribe_to_topic(OTHER_TOPIC, received_b.put)

    await deliver(phoenix_server, client, TOPIC, payload={"topic_id": "a"})
    await deliver(phoenix_server, client, OTHER_TOPIC, payload={"topic_id": "b"})

    assert (await expect_message(received_a)).payload == {"topic_id": "a"}
    assert (await expect_message(received_b)).payload == {"topic_id": "b"}
    assert received_a.empty()
    assert received_b.empty()


@each_protocol
async def test_messages_are_delivered_in_order(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    sequence = list(range(5))
    await client.subscribe_to_topic(TOPIC, received.put)

    for sequence_id in sequence:
        await deliver(phoenix_server, client, payload={"sequence_id": sequence_id})

    assert [
        (await expect_message(received)).payload["sequence_id"] for _ in sequence
    ] == sequence


@each_protocol
async def test_shutdown_unsubscribes_every_topic_and_closes_the_connection(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)
    await client.subscribe_to_topic(OTHER_TOPIC)

    await client.shutdown("test shutdown")

    assert client.get_current_subscriptions() == {}
    assert client.connection is None


@each_protocol
async def test_an_event_handler_runs_alongside_the_topic_callback_until_removed(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    event = Event(EVENT)
    handled_payloads: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    await client.subscribe_to_topic(TOPIC, received.put)

    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    assert handled_payloads.empty()

    client.add_event_handler(TOPIC, event, handled_payloads.put)
    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    await expect_message(handled_payloads)

    client.remove_event_handler(TOPIC, event)
    await deliver(phoenix_server, client, event=event)
    await expect_message(received)
    assert handled_payloads.empty()


async def handle_payload(payload: dict[str, object]) -> None:
    pass


async def handle_message(message: ChannelMessage) -> None:
    pass


async def test_an_event_handler_can_be_set_read_listed_and_removed(
    client: PHXChannelsClient,
) -> None:
    event = Event("custom_event")
    await client.subscribe_to_topic(TOPIC)

    client.add_event_handler(TOPIC, event, handle_payload)
    assert client.get_event_handler(TOPIC, event) is handle_payload
    assert event in client.list_event_handlers(TOPIC)

    client.remove_event_handler(TOPIC, event)
    assert not client.has_event_handler(TOPIC, event)


async def test_a_message_handler_can_be_set_read_and_removed(
    client: PHXChannelsClient,
) -> None:
    await client.subscribe_to_topic(TOPIC)

    client.set_message_handler(TOPIC, handle_message)
    assert client.get_message_handler(TOPIC) is handle_message

    client.remove_message_handler(TOPIC)
    assert not client.has_message_handler(TOPIC)


@pytest.mark.parametrize(
    "use_handler_api",
    [
        pytest.param(
            lambda c: c.add_event_handler(TOPIC, Event(EVENT), handle_payload),
            id="add_event_handler",
        ),
        pytest.param(
            lambda c: c.remove_event_handler(TOPIC, Event(EVENT)),
            id="remove_event_handler",
        ),
        pytest.param(
            lambda c: c.get_event_handler(TOPIC, Event(EVENT)),
            id="get_event_handler",
        ),
        pytest.param(lambda c: c.list_event_handlers(TOPIC), id="list_event_handlers"),
        pytest.param(
            lambda c: c.set_message_handler(TOPIC, handle_message),
            id="set_message_handler",
        ),
        pytest.param(
            lambda c: c.remove_message_handler(TOPIC), id="remove_message_handler"
        ),
        pytest.param(lambda c: c.get_message_handler(TOPIC), id="get_message_handler"),
    ],
)
async def test_handler_apis_reject_an_unknown_topic(
    client: PHXChannelsClient, use_handler_api: Callable[[PHXChannelsClient], object]
) -> None:
    with pytest.raises(PHXTopicError, match=f"Topic {TOPIC} not subscribed"):
        use_handler_api(client)


async def test_handler_queries_report_nothing_for_an_unknown_topic(
    client: PHXChannelsClient,
) -> None:
    assert not client.has_event_handler(TOPIC, Event("x"))
    assert not client.has_message_handler(TOPIC)


async def test_unsubscribing_from_an_unknown_topic_raises(
    client: PHXChannelsClient,
) -> None:
    with pytest.raises(PHXTopicError, match=f"Topic {TOPIC} not subscribed"):
        await client.unsubscribe_from_topic(TOPIC)


async def test_a_join_the_server_never_answers_times_out(
    phoenix_server: FakePhoenixServer,
) -> None:
    phoenix_server.unanswered_join_ids.add(phoenix_server.next_client_id)

    async with make_client(phoenix_server, join_timeout_s=JOIN_TIMEOUT_S) as client:
        with pytest.raises(PHXTopicError, match="Timed out"):
            await client.subscribe_to_topic(TOPIC)

        assert TOPIC not in client.get_current_subscriptions()


@each_protocol
async def test_a_failing_callback_skips_its_event_handler_and_later_messages_arrive(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    handled_payloads: asyncio.Queue[dict[str, object]] = asyncio.Queue()

    async def fail_on_the_first_message(message: ChannelMessage) -> None:
        if message.payload["n"] == 0:
            raise RuntimeError("callback boom")
        await received.put(message)

    await client.subscribe_to_topic(TOPIC, fail_on_the_first_message)
    client.add_event_handler(TOPIC, Event(EVENT), handled_payloads.put)

    await deliver(phoenix_server, client, payload={"n": 0})
    await deliver(phoenix_server, client, payload={"n": 1})

    assert (await expect_message(received)).payload == {"n": 1}
    assert await expect_message(handled_payloads) == {"n": 1}


@each_protocol
async def test_an_event_handler_runs_without_a_topic_callback(
    phoenix_server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    handled_payloads: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    await client.subscribe_to_topic(TOPIC)
    client.add_event_handler(TOPIC, Event(EVENT), handled_payloads.put)

    await deliver(phoenix_server, client, payload={"n": 0})

    assert await expect_message(handled_payloads) == {"n": 0}


@each_protocol
async def test_a_full_topic_queue_drops_its_oldest_message(
    phoenix_server: FakePhoenixServer,
) -> None:
    handled: list[object] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def held_on_the_first_message(message: ChannelMessage) -> None:
        handled.append(message.payload["id"])
        if not first_started.is_set():
            first_started.set()
            await release_first.wait()

    async with make_client(phoenix_server, max_topic_queue_size=1) as client:
        await client.subscribe_to_topic(TOPIC, held_on_the_first_message)
        first_id, later_ids = 1, (2, 3, 4)
        await deliver(phoenix_server, client, payload={"id": first_id})
        await asyncio.wait_for(first_started.wait(), ASYNC_TIMEOUT_S)

        # With one queue slot, each later message pushes out the one before it.
        for message_id in later_ids:
            await deliver(phoenix_server, client, payload={"id": message_id})
        subscription = client.get_current_subscriptions()[TOPIC]
        assert await wait_for_condition(
            lambda: subscription.dropped_message_count == len(later_ids) - 1
        )
        release_first.set()

        assert await wait_for_condition(lambda: handled == [first_id, later_ids[-1]])


async def test_a_message_from_an_older_join_is_not_delivered(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    await client.subscribe_to_topic(TOPIC, received.put)

    await phoenix_server.simulate_server_event(
        TOPIC, EVENT, {"join": "older"}, join_ref="older-join"
    )
    await deliver(phoenix_server, client, payload={"join": "current"})

    assert (await expect_message(received)).payload == {"join": "current"}


async def test_a_message_without_handlers_is_skipped_and_later_ones_still_arrive(
    phoenix_server: FakePhoenixServer,
    client: PHXChannelsClient,
    received: asyncio.Queue[ChannelMessage],
    caplog: pytest.LogCaptureFixture,
) -> None:
    await client.subscribe_to_topic(TOPIC)
    await deliver(phoenix_server, client, payload={"n": 0})
    # The skip is only visible as this warning; wait for it before adding a handler.
    assert await wait_for_condition(lambda: "No handler found" in caplog.text)

    client.set_message_handler(TOPIC, received.put)
    await deliver(phoenix_server, client, payload={"n": 1})

    assert (await expect_message(received)).payload == {"n": 1}
