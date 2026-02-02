import asyncio
import logging
import pytest
from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.exceptions import PHXTopicError
from .conftest import FakePhoenixServerV2

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_subscribe_to_topic_succeeds_when_subscribing_to_valid_topic(
    phoenix_server: FakePhoenixServerV2,
):
    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:

        async def test_callback(message: ChannelMessage):
            logger.debug("Received message: %s", message)

        await client.subscribe_to_topic("test-topic", test_callback)

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions

        topic_subscription = subscriptions["test-topic"]
        assert topic_subscription.name == "test-topic"
        assert topic_subscription.async_callback == test_callback

        assert topic_subscription.subscription_ready.done()
        assert not topic_subscription.subscription_ready.exception()


@pytest.mark.asyncio
async def test_subscribe_to_topic_raises_phxtopicerror_when_subscribing_to_unmatched_topic(
    phoenix_server: FakePhoenixServerV2,
):
    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:

        async def test_callback(message: ChannelMessage):
            logger.debug("Received message: %s", message)

        with pytest.raises(PHXTopicError) as exc_info:
            await client.subscribe_to_topic("invalid-topic", test_callback)

        assert "unmatched topic" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_subscribe_to_topic_raises_phxtopicerror_when_subscribing_to_already_subscribed_topic(
    phoenix_server: FakePhoenixServerV2,
):
    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:

        async def test_callback(message: ChannelMessage):
            logger.debug("Received message: %s", message)

        await client.subscribe_to_topic("test-topic", test_callback)

        with pytest.raises(PHXTopicError) as exc_info:
            await client.subscribe_to_topic("test-topic", test_callback)

        expected_message = "Topic test-topic already subscribed"
        assert str(exc_info.value) == expected_message


@pytest.mark.asyncio
async def test_unsubscribe_from_topic_succeeds_when_unsubscribing_from_subscribed_topic(
    phoenix_server: FakePhoenixServerV2,
):
    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:

        async def test_callback(message: ChannelMessage):
            logger.debug("Received message: %s", message)

        await client.subscribe_to_topic("test-topic", test_callback)

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions

        await client.unsubscribe_from_topic("test-topic")

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" not in subscriptions


@pytest.mark.asyncio
async def test_callback_receives_message_when_server_sends_message_to_subscribed_topic(
    phoenix_server: FakePhoenixServerV2,
):
    received_messages = []
    callback_event = asyncio.Event()

    async def test_callback(message: ChannelMessage):
        received_messages.append(message)
        callback_event.set()

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        # Subscribe to topic
        await client.subscribe_to_topic("test-topic", test_callback)

        test_payload = {"user_id": 123, "message": "Hello from server!"}
        await phoenix_server.simulate_server_event(
            "test-topic", "new_message", test_payload, join_ref="1"
        )

        await callback_event.wait()

        # Verify callback was called with correct message
        assert len(received_messages) == 1
        message = received_messages[0]

        # Check message structure
        assert hasattr(message, "topic")
        assert hasattr(message, "event")
        assert hasattr(message, "payload")

        # Check message content
        assert message.topic == "test-topic"
        # Event could be either a PHXEvent enum or a string for custom events
        if hasattr(message.event, "value"):
            assert message.event.value == "new_message"
        else:
            assert message.event == "new_message"
        assert message.payload == test_payload


@pytest.mark.asyncio
async def test_unsubscribe_from_topic_gracefully_allows_callback_to_finish_but_ignores_queued_events(
    phoenix_server: FakePhoenixServerV2,
):
    ARBITRARY_NUMBER_OF_EVENTS_THAT_WILL_SIMULATE_IGNORING_THEM_UNTIL_REACHING_THE_LEAVE_EVENT = 10

    received_messages = []
    callback_control_event = asyncio.Event()  # Controls when callback can complete
    first_callback_started = asyncio.Event()  # Signals when first callback has started

    async def test_callback(message: ChannelMessage):
        received_messages.append(message)
        if len(received_messages) == 1:  # Only set on first callback
            first_callback_started.set()
        # Wait for test to signal callback can complete
        await callback_control_event.wait()

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        # 1. Subscribe to topic successfully
        await client.subscribe_to_topic("test-topic", test_callback)

        # 2. Send burst of events asynchronously using gather
        event_tasks = [
            phoenix_server.simulate_server_event(
                "test-topic", "burst_event", {"event_id": i}, join_ref="1"
            )
            for i in range(
                ARBITRARY_NUMBER_OF_EVENTS_THAT_WILL_SIMULATE_IGNORING_THEM_UNTIL_REACHING_THE_LEAVE_EVENT
            )
        ]
        await asyncio.gather(*event_tasks)

        # Wait for first callback to start (which blocks on callback_control_event)
        await first_callback_started.wait()

        # 3. Assert that queue size is correct (total events - 1 being processed)
        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions
        topic_subscription = subscriptions["test-topic"]
        assert (
            topic_subscription.queue.qsize()
            == ARBITRARY_NUMBER_OF_EVENTS_THAT_WILL_SIMULATE_IGNORING_THEM_UNTIL_REACHING_THE_LEAVE_EVENT
            - 1
        )

        # 4. Unsubscribe from topic
        unsubscribe_task = asyncio.create_task(
            client.unsubscribe_from_topic("test-topic")
        )

        # Give the unsubscribe task a moment to process and set leave_requested
        await asyncio.sleep(0)

        # 5. Assert that leave was requested, callback is not done, and topic is still in subscriptions
        assert topic_subscription.leave_requested.is_set()
        assert topic_subscription.current_callback_task is not None
        assert not topic_subscription.current_callback_task.done()
        assert "test-topic" in client.get_current_subscriptions()

        # 6. Set the event to allow callback to complete
        callback_control_event.set()

        # 7. Wait for unsubscribe to complete and verify cleanup
        await unsubscribe_task
        assert "test-topic" not in client.get_current_subscriptions()

        # 8. Verify that only 1 message was processed since the callback was blocked
        assert len(received_messages) == 1
        assert received_messages[0].payload["event_id"] == 0


@pytest.mark.asyncio
async def test_two_topics_with_different_callbacks(phoenix_server: FakePhoenixServerV2):
    """Test subscribing to two topics with different callbacks that have unique behavior."""

    messages_a = []
    messages_b = []
    callback_a_event = asyncio.Event()
    callback_b_event = asyncio.Event()

    async def callback_a(message: ChannelMessage):
        messages_a.append(message)
        callback_a_event.set()

    async def callback_b(message: ChannelMessage):
        messages_b.append(message)
        callback_b_event.set()

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback_a)
        await client.subscribe_to_topic("test-topic-b", callback_b)

        payload_a = {"topic_id": "a"}
        payload_b = {"topic_id": "b"}

        await phoenix_server.simulate_server_event(
            "test-topic", "event1", payload_a, join_ref="1"
        )
        await phoenix_server.simulate_server_event(
            "test-topic-b", "event2", payload_b, join_ref="2"
        )

        await callback_a_event.wait()
        await callback_b_event.wait()

        assert len(messages_a) == 1
        assert len(messages_b) == 1
        assert messages_a[0].payload == payload_a
        assert messages_b[0].payload == payload_b


@pytest.mark.asyncio
async def test_messages_are_handled_in_correct_order(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that messages sent in a burst are handled in the correct sequential order."""

    received_messages = []
    all_messages_received = asyncio.Event()
    expected_message_count = 5

    async def ordered_callback(message: ChannelMessage):
        received_messages.append(message.payload["sequence_id"])
        if len(received_messages) == expected_message_count:
            all_messages_received.set()

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", ordered_callback)

        message_tasks = [
            phoenix_server.simulate_server_event(
                "test-topic",
                "sequence_event",
                {"sequence_id": i, "data": f"message_{i}"},
                join_ref="1",
            )
            for i in range(expected_message_count)
        ]

        await asyncio.gather(*message_tasks)

        await all_messages_received.wait()

        assert len(received_messages) == expected_message_count
        assert received_messages == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_shutdown_unsubscribes_from_all_topics_and_cleans_up_resources(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that shutdown properly unsubscribes from all topics and cleans up resources."""

    async def test_callback(message: ChannelMessage):
        pass

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    )

    try:
        await client.__aenter__()

        await client.subscribe_to_topic("test-topic", test_callback)
        await client.subscribe_to_topic("test-topic-b", test_callback)

        subscriptions = client.get_current_subscriptions()
        assert len(subscriptions) == 2
        assert "test-topic" in subscriptions
        assert "test-topic-b" in subscriptions
        assert client.connection is not None
        assert client._message_routing_task is not None

        await client.shutdown("test shutdown")

        subscriptions = client.get_current_subscriptions()
        assert len(subscriptions) == 0

        assert client.connection is None

    except Exception:
        if client.connection:
            await client.shutdown("cleanup after failure")
        raise


@pytest.mark.asyncio
async def test_dynamic_event_handler_management_with_counter(
    phoenix_server: FakePhoenixServerV2,
):
    """Test dynamic add/remove of event handlers with counter."""

    handler_count = 0
    message_handler_count = 0
    message_handler_event = asyncio.Event()
    specific_event = asyncio.Event()

    async def message_handler(message: ChannelMessage):
        nonlocal message_handler_count
        message_handler_count += 1
        message_handler_event.set()

    async def count_handler(payload):
        nonlocal handler_count
        handler_count += 1
        specific_event.set()

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", message_handler)

        # Event goes to message handler only
        await phoenix_server.simulate_server_event(
            "test-topic", "count_me", {}, join_ref="1"
        )
        await message_handler_event.wait()
        message_handler_event.clear()
        assert handler_count == 0
        assert message_handler_count == 1

        # Add specific handler, event goes to both message handler and specific handler
        client.add_event_handler("test-topic", Event("count_me"), count_handler)
        await phoenix_server.simulate_server_event(
            "test-topic", "count_me", {}, join_ref="1"
        )
        # Wait for both handlers to complete
        await message_handler_event.wait()
        await specific_event.wait()
        message_handler_event.clear()
        specific_event.clear()
        assert handler_count == 1
        assert message_handler_count == 2  # Both handlers ran

        # Remove specific handler, event goes to message handler only again
        client.remove_event_handler("test-topic", Event("count_me"))
        await phoenix_server.simulate_server_event(
            "test-topic", "count_me", {}, join_ref="1"
        )
        await message_handler_event.wait()
        assert handler_count == 1
        assert message_handler_count == 3  # Only message handler ran this time


@pytest.mark.asyncio
async def test_run_forever_exits_when_connection_closes(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that run_forever() returns when the WebSocket connection closes and cleanup happens properly."""

    async def test_callback(_: ChannelMessage):
        pass

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    )

    async with client:
        await client.subscribe_to_topic("test-topic", test_callback)

        # Verify topic is subscribed
        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions
        assert client.connection is not None

        run_forever_task = asyncio.create_task(client.run_forever())

        assert phoenix_server.client_websocket is not None
        await phoenix_server.client_websocket.close()

        # run_forever() should exit (proving it detected the closure)
        # If this times out, it means run_forever() is hanging and not detecting the closure
        try:
            await asyncio.wait_for(run_forever_task, timeout=1.0)
        except asyncio.TimeoutError:
            pytest.fail(
                "run_forever() did not exit after connection closed - it's hanging!"
            )

    # After exiting context manager, verify cleanup happened
    subscriptions = client.get_current_subscriptions()
    assert len(subscriptions) == 0
    assert client.connection is None
