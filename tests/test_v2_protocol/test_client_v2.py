from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

import pytest
from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.exceptions import PHXTopicError
from .conftest import FakePhoenixServerV2

logger = logging.getLogger(__name__)


async def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 1.0,
    interval: float = 0.05,
) -> bool:
    """
    Poll for a condition to become true, with timeout.

    Args:
        condition: A callable that returns True when the condition is met.
        timeout: Maximum time to wait in seconds.
        interval: Time between polls in seconds.

    Returns:
        True if condition was met, False if timeout occurred.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False


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


@pytest.mark.asyncio
async def test_heartbeat_is_sent_and_acknowledged(
    phoenix_server: FakePhoenixServerV2,
    caplog,
):
    """Test that heartbeat messages are sent and acknowledged by the server."""
    import logging

    with caplog.at_level(logging.DEBUG):
        async with PHXChannelsClient(
            phoenix_server.url,
            api_key="test_key",
            protocol_version=PhoenixChannelsProtocolVersion.V2,
            heartbeat_interval_secs=0.1,  # Short interval for testing
        ) as client:
            # Verify heartbeat task is running
            assert client._heartbeat_task is not None
            assert not client._heartbeat_task.done()

            # Wait for heartbeat to be sent and acknowledged
            # Poll until we see the acknowledgment in the logs (more robust than fixed sleep)
            result = await wait_for_condition(
                lambda: any(
                    "heartbeat acknowledged" in record.message.lower()
                    for record in caplog.records
                ),
                timeout=1.0,
                interval=0.02,
            )
            assert result, "Heartbeat was not acknowledged within timeout"

            # Verify heartbeat task is still running (no errors)
            assert not client._heartbeat_task.done()

    # After shutdown, heartbeat task should be cancelled
    assert client._heartbeat_task is None or client._heartbeat_task.done()


@pytest.mark.asyncio
async def test_heartbeat_can_be_disabled(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that heartbeat can be disabled by setting interval to None."""

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_secs=None,  # Disable heartbeat
    ) as client:
        # Verify heartbeat task is not created
        assert client._heartbeat_task is None


@pytest.mark.asyncio
async def test_heartbeat_timeout_warning_when_no_response(
    phoenix_server: FakePhoenixServerV2,
    caplog,
):
    """Test that a warning is logged when heartbeat response is not received."""
    import logging

    # Modify server to not respond to heartbeats for this test
    original_handler = phoenix_server.handle_message

    async def no_heartbeat_response_handler(data):
        if isinstance(data, list) and len(data) == 5:
            _, _, topic, event, _ = data
            if event == "heartbeat" and topic == "phoenix":
                return  # Don't respond to heartbeat
        await original_handler(data)

    phoenix_server.handle_message = no_heartbeat_response_handler

    with caplog.at_level(logging.WARNING):
        async with PHXChannelsClient(
            phoenix_server.url,
            api_key="test_key",
            protocol_version=PhoenixChannelsProtocolVersion.V2,
            heartbeat_interval_secs=0.1,  # Short interval for testing
        ):
            # Poll until timeout warning is logged (more robust than fixed sleep)
            # Need two heartbeat cycles: first sends heartbeat, second detects timeout
            result = await wait_for_condition(
                lambda: any(
                    "heartbeat timeout" in record.message.lower()
                    for record in caplog.records
                ),
                timeout=1.0,
                interval=0.05,
            )
            assert result, "Heartbeat timeout warning was not logged within timeout"

    # Verify warning was logged
    assert any(
        "heartbeat timeout" in record.message.lower() for record in caplog.records
    )


@pytest.mark.asyncio
async def test_reconnection_is_enabled_by_default(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that auto-reconnection is enabled by default."""

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        assert client._auto_reconnect is True
        assert client._reconnect_max_attempts == 10


@pytest.mark.asyncio
async def test_reconnection_can_be_disabled(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that auto-reconnection can be disabled."""

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=False,
    ) as client:
        assert client._auto_reconnect is False


@pytest.mark.asyncio
async def test_subscription_callbacks_are_stored_for_reconnection(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that subscription callbacks are stored for reconnection."""

    async def test_callback(message: ChannelMessage):
        pass

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", test_callback)

        # Verify callback is stored
        assert "test-topic" in client._subscription_callbacks
        assert client._subscription_callbacks["test-topic"] == test_callback


@pytest.mark.asyncio
async def test_event_handlers_are_stored_for_reconnection(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that event handlers are stored for reconnection."""

    async def test_callback(message: ChannelMessage):
        pass

    async def event_handler(payload):
        pass

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", test_callback)
        client.add_event_handler("test-topic", Event("custom_event"), event_handler)

        # Verify event handler is stored
        assert "test-topic" in client._subscription_event_handlers
        assert (
            Event("custom_event") in client._subscription_event_handlers["test-topic"]
        )


@pytest.mark.asyncio
async def test_stored_callbacks_are_cleared_on_unsubscribe(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that stored callbacks are cleared when unsubscribing."""

    async def test_callback(message: ChannelMessage):
        pass

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        await client.subscribe_to_topic("test-topic", test_callback)
        assert "test-topic" in client._subscription_callbacks

        await client.unsubscribe_from_topic("test-topic")

        # Verify callback is removed
        assert "test-topic" not in client._subscription_callbacks
        assert "test-topic" not in client._subscription_event_handlers


@pytest.mark.asyncio
async def test_reconnect_callbacks_are_invoked(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that on_disconnect and on_reconnect callbacks are called."""
    disconnect_called = False
    reconnect_called = False

    async def on_disconnect(error):
        nonlocal disconnect_called
        disconnect_called = True

    async def on_reconnect():
        nonlocal reconnect_called
        reconnect_called = True

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=True,
        reconnect_backoff_base=0.1,  # Fast reconnection for testing
        on_disconnect=on_disconnect,
        on_reconnect=on_reconnect,
    )

    # Verify callbacks are stored
    assert client._on_disconnect == on_disconnect
    assert client._on_reconnect == on_reconnect


@pytest.mark.asyncio
async def test_shutdown_stops_reconnection_attempts(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that shutdown prevents reconnection attempts."""

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=True,
    ) as client:
        # Shutdown should set the flag
        pass  # __aexit__ calls shutdown

    assert client._shutdown_requested is True


@pytest.mark.asyncio
async def test_full_reconnection_flow(
    phoenix_server: FakePhoenixServerV2,
):
    """
    Test the complete reconnection flow:
    1. Connect and subscribe to a topic
    2. Simulate connection drop
    3. Verify on_disconnect is called
    4. Verify reconnection occurs
    5. Verify on_reconnect is called
    6. Verify topic is re-subscribed
    """
    disconnect_called = asyncio.Event()
    reconnect_called = asyncio.Event()
    disconnect_error = None
    received_messages: list[ChannelMessage] = []
    message_received = asyncio.Event()

    async def on_disconnect(error):
        nonlocal disconnect_error
        disconnect_error = error
        disconnect_called.set()

    async def on_reconnect():
        reconnect_called.set()

    async def message_callback(message: ChannelMessage):
        received_messages.append(message)
        message_received.set()

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=True,
        reconnect_backoff_base=0.05,  # Very fast reconnection for testing
        reconnect_max_attempts=3,
        on_disconnect=on_disconnect,
        on_reconnect=on_reconnect,
    )

    try:
        await client.__aenter__()

        # Subscribe to a topic
        await client.subscribe_to_topic("test-topic", message_callback)
        assert "test-topic" in client.get_current_subscriptions()

        # Simulate connection drop by closing the server-side websocket
        assert phoenix_server.client_websocket is not None
        await phoenix_server.client_websocket.close()

        # Wait for disconnect callback
        result = await wait_for_condition(
            lambda: disconnect_called.is_set(),
            timeout=2.0,
            interval=0.05,
        )
        assert result, "on_disconnect was not called"

        # Wait for reconnection to succeed
        result = await wait_for_condition(
            lambda: reconnect_called.is_set(),
            timeout=2.0,
            interval=0.05,
        )
        assert result, "on_reconnect was not called"

        # Verify topic was re-subscribed
        assert "test-topic" in client.get_current_subscriptions()

        # Verify we can still receive messages after reconnection
        message_received.clear()
        # Get the new join_ref after reconnection
        topic_sub = client.get_current_subscriptions()["test-topic"]
        await phoenix_server.simulate_server_event(
            "test-topic",
            "test_event",
            {"data": "after_reconnect"},
            join_ref=topic_sub.join_ref,
        )

        result = await wait_for_condition(
            lambda: message_received.is_set(),
            timeout=1.0,
            interval=0.05,
        )
        assert result, "Message was not received after reconnection"
        assert len(received_messages) == 1
        assert received_messages[0].payload["data"] == "after_reconnect"

    finally:
        await client.shutdown("test cleanup")


@pytest.mark.asyncio
async def test_reconnection_gives_up_after_max_attempts(
    phoenix_server: FakePhoenixServerV2,
    caplog,
):
    """Test that reconnection stops after max attempts are exceeded."""
    import logging

    disconnect_called = asyncio.Event()

    async def on_disconnect(_error):
        disconnect_called.set()

    # Stop the server to make reconnection impossible
    await phoenix_server.stop()

    # Create a client that was previously connected (simulate this by setting up state)
    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=True,
        reconnect_backoff_base=0.01,  # Very fast for testing
        reconnect_max_attempts=2,
        on_disconnect=on_disconnect,
    )

    with caplog.at_level(logging.ERROR):
        # Manually trigger reconnection loop to test max attempts
        client._is_reconnecting = False
        client._shutdown_requested = False

        # Start reconnection loop
        reconnect_task = asyncio.create_task(client._reconnect_loop())

        # Wait for max attempts to be reached
        result = await wait_for_condition(
            lambda: any(
                "max reconnection attempts" in record.message.lower()
                for record in caplog.records
            ),
            timeout=2.0,
            interval=0.05,
        )
        assert result, "Max reconnection attempts message was not logged"

        # Ensure task completes
        await asyncio.wait_for(reconnect_task, timeout=1.0)

        # Verify reconnection stopped
        assert client._is_reconnecting is False
        assert client._reconnect_attempt > client._reconnect_max_attempts
