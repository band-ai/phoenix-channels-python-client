from __future__ import annotations

import asyncio
import asyncio.log
import gc
import logging
from collections.abc import Callable

import pytest
from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.phx_messages import ChannelMessage, Event
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from websockets.frames import CloseCode

from tests.conftest import (
    API_KEY,
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    LEAVE_TIMEOUT_S,
    make_client,
    wait_for_condition,
)
from tests.test_v2_protocol.conftest import FakePhoenixServer as FakePhoenixServerV2

logger = logging.getLogger(__name__)

REJECTED_TOPIC = "invalid-topic"
EVENT = "test_event"


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
            await client.subscribe_to_topic(REJECTED_TOPIC, test_callback)

        assert "unmatched topic" in str(exc_info.value).lower()


async def test_rejected_join_leaves_no_unretrieved_future_error(
    phoenix_server: FakePhoenixServerV2, caplog: pytest.LogCaptureFixture
):
    # Run in its own frame: a live client or bound exception keeps the
    # subscription, and its futures, from being collected.
    async def subscribe_to_rejected_topic() -> None:
        async with make_client(phoenix_server) as client:
            with pytest.raises(PHXTopicError):
                await client.subscribe_to_topic(REJECTED_TOPIC, None)

    await subscribe_to_rejected_topic()
    gc.collect()

    assert not [
        record for record in caplog.records if record.name == asyncio.log.logger.name
    ]


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

        assert await wait_for_condition(topic_subscription.leave_requested.is_set)

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
        reconnect_policy=ReconnectPolicy(
            base_delay_s=0.01,
            factor=2.0,
            max_delay_s=0.05,
            stable_reset_s=0.1,
            service_restart_min_delay_s=0.01,
            service_restart_max_delay_s=0.02,
            try_again_later_min_delay_s=0.03,
            try_again_later_max_delay_s=0.05,
            rapid_disconnect_uptime_s=0.05,
            rapid_window_s=1.0,
            rapid_first_min_delay_s=0.01,
            rapid_second_min_delay_s=0.02,
            rapid_cooldown_base_s=0.03,
            rapid_cooldown_step_s=0.01,
            rapid_cooldown_max_s=0.05,
            rapid_hold_down_jitter_low_ratio=0.5,
            rapid_hold_down_jitter_high_ratio=1.0,
        ),
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


async def test_service_restart_close_triggers_reconnect_and_rejoin(
    phoenix_server: FakePhoenixServerV2,
):
    reconnected_message = asyncio.Event()

    async def test_callback(message: ChannelMessage):
        if message.event == Event("after_reconnect"):
            reconnected_message.set()

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    )

    async with client:
        await client.subscribe_to_topic("test-topic", test_callback)
        await phoenix_server.close_all_clients(code=1012, reason="service restart")

        async def _probe_after_reconnect() -> bool:
            subscriptions = client.get_current_subscriptions()
            topic_subscription = subscriptions.get("test-topic")
            if client.connection is None or topic_subscription is None:
                return False
            await phoenix_server.simulate_server_event(
                "test-topic",
                "after_reconnect",
                {"ok": True},
                join_ref=topic_subscription.join_ref,
            )
            try:
                await asyncio.wait_for(reconnected_message.wait(), timeout=0.1)
                return True
            except asyncio.TimeoutError:
                return False

        deadline = asyncio.get_running_loop().time() + 5.0
        result = False
        while asyncio.get_running_loop().time() < deadline:
            if await _probe_after_reconnect():
                result = True
                break
            await asyncio.sleep(0.05)

        assert result, "Client did not reconnect after service restart"
        assert "test-topic" in client.get_current_subscriptions()


async def test_auto_reconnect_can_be_disabled_for_service_restart_close(
    phoenix_server: FakePhoenixServerV2,
):
    async def test_callback(_: ChannelMessage):
        pass

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=False,
    )

    async with client:
        await client.subscribe_to_topic("test-topic", test_callback)
        await phoenix_server.close_all_clients(code=1012, reason="service restart")
        assert await wait_for_condition(lambda: client.connection is None)


async def test_try_again_later_close_code_uses_cooldown_override(
    phoenix_server: FakePhoenixServerV2,
):
    async def test_callback(_: ChannelMessage):
        pass

    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.05,
        stable_reset_s=0.1,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.02,
        try_again_later_min_delay_s=0.2,
        try_again_later_max_delay_s=0.25,
        rapid_disconnect_uptime_s=0.05,
        rapid_window_s=1.0,
        rapid_first_min_delay_s=0.01,
        rapid_second_min_delay_s=0.02,
        rapid_cooldown_base_s=0.03,
        rapid_cooldown_step_s=0.01,
        rapid_cooldown_max_s=0.05,
        rapid_hold_down_jitter_low_ratio=0.5,
        rapid_hold_down_jitter_high_ratio=1.0,
    )

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        reconnect_policy=policy,
    )

    async with client:
        await client.subscribe_to_topic("test-topic", test_callback)
        before = client._conn_generation
        await phoenix_server.close_all_clients(code=1013, reason="try again later")
        # cooldown should delay reconnect enough that generation doesn't change immediately
        await asyncio.sleep(0.05)
        assert client._conn_generation == before


async def test_reconnection_is_enabled_by_default(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that auto-reconnection is enabled by default."""

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
    ) as client:
        assert client.auto_reconnect is True


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
        assert client.auto_reconnect is False


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

        subscription = client.get_current_subscriptions()["test-topic"]
        assert subscription.async_callback == test_callback


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

        handlers = client.list_event_handlers("test-topic")
        assert Event("custom_event") in handlers


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
        assert "test-topic" in client.get_current_subscriptions()

        await client.unsubscribe_from_topic("test-topic")

        assert "test-topic" not in client.get_current_subscriptions()


# Long enough for a local join reply, short enough to time out an unanswered one.
REJOIN_TIMEOUT_S = 0.2


def rejoin_settled(
    client: PHXChannelsClient, topic: str = FakePhoenixServerV2.TOPIC
) -> Callable[[], bool]:
    """True once the topic's post-reconnect join has an outcome, or it is gone."""

    def settled() -> bool:
        subscription = client.get_current_subscriptions().get(topic)
        return subscription is None or (
            subscription.conn_generation > 1 and subscription.current_join_ready.done()
        )

    return settled


class CallbackFailure(Exception):
    pass


class BusyCallback:
    """A topic callback that stays busy until released, then fails."""

    def __init__(self) -> None:
        self.running = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, message: ChannelMessage) -> None:
        self.running.set()
        await self.release.wait()
        raise CallbackFailure


async def reconnect_while_draining(
    server: FakePhoenixServerV2, client: PHXChannelsClient, busy: BusyCallback
) -> None:
    topic = FakePhoenixServerV2.TOPIC
    join_ref = client.get_current_subscriptions()[topic].join_ref
    await server.simulate_server_event(topic, EVENT, {}, join_ref=join_ref)
    await asyncio.wait_for(busy.running.wait(), ASYNC_TIMEOUT_S)

    generation = client._conn_generation
    await server.close_all_clients(code=CloseCode.SERVICE_RESTART)
    # Nothing yields from the reconnect until the first topic's drain.
    assert await wait_for_condition(lambda: client._conn_generation > generation)


async def assert_delivers_after_rejoin(
    server: FakePhoenixServerV2,
    client: PHXChannelsClient,
    topic: str,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    assert await wait_for_condition(rejoin_settled(client, topic))
    join_ref = client.get_current_subscriptions()[topic].join_ref
    await server.simulate_server_event(topic, EVENT, {}, join_ref=join_ref)
    await asyncio.wait_for(received.get(), ASYNC_TIMEOUT_S)


async def test_transient_rejoin_failure_keeps_subscription_registered(
    phoenix_server: FakePhoenixServerV2,
):
    async def test_callback(_: ChannelMessage):
        pass

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key=API_KEY,
        reconnect_policy=FAST_RECONNECT,
        join_timeout_s=REJOIN_TIMEOUT_S,
    )

    async with client:
        await client.subscribe_to_topic(FakePhoenixServerV2.TOPIC, test_callback)
        reconnected_id = max(phoenix_server.current_client_ids()) + 1
        phoenix_server.unanswered_join_ids.add(reconnected_id)

        await phoenix_server.close_all_clients(code=CloseCode.SERVICE_RESTART)

        assert await wait_for_condition(rejoin_settled(client))
        assert FakePhoenixServerV2.TOPIC in client.get_current_subscriptions()


async def test_rejected_rejoin_unregisters_the_topic(
    phoenix_server: FakePhoenixServerV2,
):
    async def test_callback(_: ChannelMessage):
        pass

    client = PHXChannelsClient(
        phoenix_server.url, api_key=API_KEY, reconnect_policy=FAST_RECONNECT
    )

    async with client:
        await client.subscribe_to_topic(FakePhoenixServerV2.TOPIC, test_callback)
        reconnected_id = max(phoenix_server.current_client_ids()) + 1
        phoenix_server.fail_join_ids.add(reconnected_id)

        await phoenix_server.close_all_clients(code=CloseCode.SERVICE_RESTART)

        assert await wait_for_condition(rejoin_settled(client))
        assert FakePhoenixServerV2.TOPIC not in client.get_current_subscriptions()


async def test_unsubscribing_during_a_rejoin_drain_keeps_the_client_running(
    phoenix_server: FakePhoenixServerV2,
):
    busy = BusyCallback()
    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        leave_timeout_s=LEAVE_TIMEOUT_S,
        # Outlasts the leave, so the unsubscribe cancels the callback mid-drain.
        callback_drain_timeout_s=ASYNC_TIMEOUT_S,
    )

    async with client:
        await client.subscribe_to_topic(FakePhoenixServerV2.TOPIC, busy)
        await client.subscribe_to_topic(FakePhoenixServerV2.OTHER_TOPIC, received.put)
        await reconnect_while_draining(phoenix_server, client, busy)
        run = asyncio.create_task(client.run_forever(install_signal_handlers=False))

        with pytest.raises(PHXTopicError):
            await client.unsubscribe_from_topic(FakePhoenixServerV2.TOPIC)

        await assert_delivers_after_rejoin(
            phoenix_server, client, FakePhoenixServerV2.OTHER_TOPIC, received
        )
        assert FakePhoenixServerV2.TOPIC not in client.get_current_subscriptions()

    assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None


async def test_a_callback_failing_while_the_rejoin_drains_it_does_not_stop_the_rejoin(
    phoenix_server: FakePhoenixServerV2,
):
    busy = BusyCallback()
    received: asyncio.Queue[ChannelMessage] = asyncio.Queue()
    client = make_client(phoenix_server, reconnect_policy=FAST_RECONNECT)

    async with client:
        await client.subscribe_to_topic(FakePhoenixServerV2.TOPIC, busy)
        await client.subscribe_to_topic(FakePhoenixServerV2.OTHER_TOPIC, received.put)
        await reconnect_while_draining(phoenix_server, client, busy)

        busy.release.set()

        assert await wait_for_condition(rejoin_settled(client))
        assert FakePhoenixServerV2.TOPIC in client.get_current_subscriptions()
        await assert_delivers_after_rejoin(
            phoenix_server, client, FakePhoenixServerV2.OTHER_TOPIC, received
        )


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

    assert client._shutdown_event.is_set() is True


async def test_full_reconnection_flow(
    phoenix_server: FakePhoenixServerV2,
):
    """Test reconnect flow: disconnect, reconnect, rejoin, and receive messages."""
    received_messages: list[ChannelMessage] = []
    message_received = asyncio.Event()

    async def message_callback(message: ChannelMessage):
        received_messages.append(message)
        message_received.set()

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        auto_reconnect=True,
        reconnect_policy=ReconnectPolicy(
            base_delay_s=0.01,
            factor=2.0,
            max_delay_s=0.05,
            stable_reset_s=0.1,
            service_restart_min_delay_s=0.01,
            service_restart_max_delay_s=0.02,
            try_again_later_min_delay_s=0.03,
            try_again_later_max_delay_s=0.05,
            rapid_disconnect_uptime_s=0.05,
            rapid_window_s=1.0,
            rapid_first_min_delay_s=0.01,
            rapid_second_min_delay_s=0.02,
            rapid_cooldown_base_s=0.03,
            rapid_cooldown_step_s=0.01,
            rapid_cooldown_max_s=0.05,
            rapid_hold_down_jitter_low_ratio=0.5,
            rapid_hold_down_jitter_high_ratio=1.0,
        ),
    )

    try:
        await client.__aenter__()

        # Subscribe to a topic
        await client.subscribe_to_topic("test-topic", message_callback)
        assert "test-topic" in client.get_current_subscriptions()

        # Simulate reconnectable service restart close.
        await phoenix_server.close_all_clients(code=1012, reason="service restart")

        # Wait for reconnection to succeed
        result = await wait_for_condition(
            lambda: client.connection is not None
            and "test-topic" in client.get_current_subscriptions(),
            timeout=2.0,
            interval=0.05,
        )
        assert result, "Client did not reconnect and rejoin topic in time"

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


@pytest.mark.parametrize("install_signal_handlers", [True, False])
async def test_rapid_disconnect_suppression_stops_reconnect_attempts(
    phoenix_server: FakePhoenixServerV2,
    install_signal_handlers: bool,
):
    async def test_callback(_: ChannelMessage):
        pass

    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.05,
        stable_reset_s=0.1,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.02,
        try_again_later_min_delay_s=0.03,
        try_again_later_max_delay_s=0.05,
        rapid_disconnect_uptime_s=0.05,
        rapid_window_s=1.0,
        rapid_first_min_delay_s=0.01,
        rapid_second_min_delay_s=0.02,
        rapid_cooldown_base_s=0.03,
        rapid_cooldown_step_s=0.01,
        rapid_cooldown_max_s=0.05,
        rapid_suppress_disconnect_count=3,
        rapid_hold_down_jitter_low_ratio=0.5,
        rapid_hold_down_jitter_high_ratio=1.0,
    )

    phoenix_server.close_on_join_ids.update({1, 2, 3, 4, 5})
    phoenix_server.close_on_join_code = 1012
    phoenix_server.close_on_join_reason = "forced close on join"

    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        reconnect_policy=policy,
    )

    with pytest.raises(PHXConnectionError):
        async with client:
            await client.subscribe_to_topic("test-topic", test_callback)
            await client.run_forever(install_signal_handlers=install_signal_handlers)


# ---------------------------------------------------------------------------
# Heartbeat tests
# ---------------------------------------------------------------------------


async def test_heartbeat_is_sent_periodically(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that the client sends heartbeat messages at the configured interval."""
    acks = 0

    def count_ack() -> None:
        nonlocal acks
        acks += 1

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_s=0.1,
        on_heartbeat_ack=count_ack,
    ):
        assert await wait_for_condition(lambda: acks >= 2)


async def test_heartbeat_can_be_disabled(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that heartbeats can be disabled by passing None."""
    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_s=None,
    ) as client:
        assert client._heartbeat_interval_s is None
        assert client._heartbeat_task is None
        await asyncio.sleep(0.1)
        # No heartbeat task should have been created
        assert client._heartbeat_task is None


async def test_heartbeat_task_is_cleaned_up_on_shutdown(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that the heartbeat task is cancelled during shutdown."""
    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_s=0.1,
    )

    async with client:
        assert await wait_for_condition(lambda: client._heartbeat_task is not None)

    # After context exit, heartbeat task should be cleaned up
    assert client._heartbeat_task is None
    assert client._pending_heartbeat_ref is None


async def test_heartbeat_invalid_interval_raises_valueerror():
    """Test that a non-positive heartbeat interval raises ValueError."""
    with pytest.raises(ValueError, match="heartbeat_interval_s must be > 0"):
        PHXChannelsClient(
            "ws://localhost:9999/socket/websocket",
            api_key="test_key",
            heartbeat_interval_s=0,
        )

    with pytest.raises(ValueError, match="heartbeat_interval_s must be > 0"):
        PHXChannelsClient(
            "ws://localhost:9999/socket/websocket",
            api_key="test_key",
            heartbeat_interval_s=-1.0,
        )


async def test_heartbeat_survives_reconnection(
    phoenix_server: FakePhoenixServerV2,
):
    """Test that heartbeat is restarted after reconnection."""
    client = PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        heartbeat_interval_s=0.1,
        reconnect_policy=ReconnectPolicy(
            base_delay_s=0.01,
            factor=2.0,
            max_delay_s=0.05,
            stable_reset_s=0.1,
            service_restart_min_delay_s=0.01,
            service_restart_max_delay_s=0.02,
            try_again_later_min_delay_s=0.03,
            try_again_later_max_delay_s=0.05,
            rapid_disconnect_uptime_s=0.05,
            rapid_window_s=1.0,
            rapid_first_min_delay_s=0.01,
            rapid_second_min_delay_s=0.02,
            rapid_cooldown_base_s=0.03,
            rapid_cooldown_step_s=0.01,
            rapid_cooldown_max_s=0.05,
            rapid_hold_down_jitter_low_ratio=0.5,
            rapid_hold_down_jitter_high_ratio=1.0,
        ),
    )

    async with client:
        assert await wait_for_condition(lambda: client._heartbeat_task is not None)

        # Trigger reconnection
        await phoenix_server.close_all_clients(code=1012, reason="service restart")

        # Wait for reconnection
        result = await wait_for_condition(
            lambda: client.connection is not None,
            timeout=2.0,
            interval=0.05,
        )
        assert result, "Client did not reconnect after service restart"

        # Heartbeat runs again after reconnection and the server acknowledges it.
        assert await wait_for_condition(
            lambda: client._heartbeat_task is not None
            and not client._heartbeat_task.done()
            and client._pending_heartbeat_ref is None
        )


async def test_additional_headers_are_sent_on_the_ws_handshake(
    phoenix_server: FakePhoenixServerV2,
):
    """`additional_headers` ride the WebSocket handshake, so a caller can send the
    API key as an `x-api-key` header (for proxy in-header injection) rather than
    only in the URL query."""

    async def _noop(_message: ChannelMessage) -> None:
        return None

    async with PHXChannelsClient(
        phoenix_server.url,
        api_key="test_key",
        protocol_version=PhoenixChannelsProtocolVersion.V2,
        additional_headers={"x-api-key": "header-only-value"},
    ) as client:
        # subscribing forces a live connection, so the server sees the handshake
        await client.subscribe_to_topic("test-topic", _noop)

    server_ws = phoenix_server.client_websocket
    assert server_ws is not None
    request = server_ws.request
    assert request is not None
    assert request.headers["x-api-key"] == "header-only-value"
    # the header is additive; the existing api_key query param is untouched
    assert "api_key=test_key" in client.channel_socket_url
