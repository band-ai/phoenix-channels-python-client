from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from functools import partial

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import Message, PHXEvent, UserEvent

from .conftest import FakePhoenixServerV1 as FakePhoenixServer


V1Client = partial(
    PHXChannelsClient, protocol_version=PhoenixChannelsProtocolVersion.V1
)


def _event_name(message: Message) -> str:
    if hasattr(message.event, "value"):
        return str(message.event.value)
    return str(message.event)


async def _wait_until(
    predicate, *, timeout_s: float = 2.0, interval_s: float = 0.01
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise asyncio.TimeoutError("Condition was not met before timeout")
        await asyncio.sleep(interval_s)


def _test_reconnect_policy() -> ReconnectPolicy:
    return ReconnectPolicy(
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
    )


async def test_websocket_auth_uses_header_not_query_param(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(
        f"{phoenix_server.url}?api_key=stale&debug=true",
        api_key="test_key",
    ):
        pass

    assert phoenix_server.list_request_api_keys() == ["test_key"]
    paths = phoenix_server.list_request_paths()
    assert len(paths) == 1
    assert "debug=true" in paths[0]
    assert "vsn=1.0.0" in paths[0]
    assert "api_key" not in paths[0]
    assert "test_key" not in paths[0]


async def test_subscribe_to_topic_succeeds_when_subscribing_to_valid_topic(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(phoenix_server.url, api_key="test_key") as client:

        async def test_callback(message: Message) -> None:
            _ = message

        await client.subscribe_to_topic("test-topic", test_callback)

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions

        topic_subscription = subscriptions["test-topic"]
        assert topic_subscription.name == "test-topic"
        assert topic_subscription.async_callback == test_callback

        assert topic_subscription.subscription_ready.done()
        assert not topic_subscription.subscription_ready.exception()


async def test_subscribe_to_topic_raises_phxconnectionerror_when_disconnected(
    phoenix_server: FakePhoenixServer,
):
    client = V1Client(phoenix_server.url, api_key="test_key")

    async def test_callback(message: Message) -> None:
        _ = message

    with pytest.raises(PHXConnectionError):
        await client.subscribe_to_topic("test-topic", test_callback)


async def test_subscribe_to_topic_raises_phxtopicerror_when_subscribing_to_unmatched_topic(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(phoenix_server.url, api_key="test_key") as client:

        async def test_callback(message: Message) -> None:
            _ = message

        with pytest.raises(PHXTopicError) as exc_info:
            await client.subscribe_to_topic("invalid-topic", test_callback)

        assert "unmatched topic" in str(exc_info.value).lower()


async def test_subscribe_to_topic_raises_phxtopicerror_when_subscribing_to_already_subscribed_topic(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(phoenix_server.url, api_key="test_key") as client:

        async def test_callback(message: Message) -> None:
            _ = message

        await client.subscribe_to_topic("test-topic", test_callback)

        with pytest.raises(PHXTopicError) as exc_info:
            await client.subscribe_to_topic("test-topic", test_callback)

        expected_message = "Topic test-topic already subscribed"
        assert str(exc_info.value) == expected_message


async def test_unsubscribe_from_topic_succeeds_when_unsubscribing_from_subscribed_topic(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(phoenix_server.url, api_key="test_key") as client:

        async def test_callback(message: Message) -> None:
            _ = message

        await client.subscribe_to_topic("test-topic", test_callback)

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions

        await client.unsubscribe_from_topic("test-topic")

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" not in subscriptions


async def test_unsubscribe_fail_fast_when_disconnected_keeps_subscription(
    phoenix_server: FakePhoenixServer,
):
    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        auto_reconnect=False,
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:

        async def test_callback(message: Message) -> None:
            _ = message

        await client.subscribe_to_topic("test-topic", test_callback)
        await phoenix_server.close_all_clients()

        await _wait_until(lambda: client.connection is None, timeout_s=1.0)

        with pytest.raises(PHXConnectionError):
            await client.unsubscribe_from_topic("test-topic")

        assert "test-topic" in client.get_current_subscriptions()


async def test_callback_receives_message_when_server_sends_message_to_subscribed_topic(
    phoenix_server: FakePhoenixServer,
):
    received_messages: list[Message] = []
    callback_event = asyncio.Event()

    async def test_callback(message: Message) -> None:
        received_messages.append(message)
        callback_event.set()

    async with V1Client(phoenix_server.url, api_key="test_key") as client:
        await client.subscribe_to_topic("test-topic", test_callback)

        test_payload = {"user_id": 123, "message": "Hello from server!"}
        await phoenix_server.simulate_server_event(
            "test-topic", "new_message", test_payload, join_ref="1"
        )

        await callback_event.wait()

        assert len(received_messages) == 1
        message = received_messages[0]

        assert hasattr(message, "topic")
        assert hasattr(message, "event")
        assert hasattr(message, "payload")

        assert message.topic == "test-topic"
        assert _event_name(message) == "new_message"
        assert message.payload == test_payload


async def test_unsubscribe_from_topic_gracefully_allows_callback_to_finish_but_ignores_queued_events(
    phoenix_server: FakePhoenixServer,
):
    event_count = 10

    received_messages: list[Message] = []
    callback_control_event = asyncio.Event()
    first_callback_started = asyncio.Event()
    first_callback_completed = asyncio.Event()

    async def test_callback(message: Message) -> None:
        received_messages.append(message)
        if len(received_messages) == 1:
            first_callback_started.set()
        await callback_control_event.wait()
        if len(received_messages) == 1:
            first_callback_completed.set()

    async with V1Client(phoenix_server.url, api_key="test_key") as client:
        await client.subscribe_to_topic("test-topic", test_callback)

        event_tasks = [
            phoenix_server.simulate_server_event(
                "test-topic", "burst_event", {"event_id": i}, join_ref="1"
            )
            for i in range(event_count)
        ]
        await asyncio.gather(*event_tasks)

        await first_callback_started.wait()

        subscriptions = client.get_current_subscriptions()
        assert "test-topic" in subscriptions
        topic_subscription = subscriptions["test-topic"]
        assert topic_subscription.queue.qsize() == event_count - 1

        unsubscribe_task = asyncio.create_task(
            client.unsubscribe_from_topic("test-topic")
        )

        await asyncio.sleep(0)

        assert topic_subscription.leave_requested.is_set()
        assert not unsubscribe_task.done()
        assert not first_callback_completed.is_set()
        assert "test-topic" in client.get_current_subscriptions()

        callback_control_event.set()

        await unsubscribe_task
        assert first_callback_completed.is_set()
        assert "test-topic" not in client.get_current_subscriptions()

        assert len(received_messages) == 1
        assert received_messages[0].payload["event_id"] == 0


async def test_two_topics_with_different_callbacks(phoenix_server: FakePhoenixServer):
    messages_a: list[Message] = []
    messages_b: list[Message] = []
    callback_a_event = asyncio.Event()
    callback_b_event = asyncio.Event()

    async def callback_a(message: Message) -> None:
        messages_a.append(message)
        callback_a_event.set()

    async def callback_b(message: Message) -> None:
        messages_b.append(message)
        callback_b_event.set()

    async with V1Client(phoenix_server.url, api_key="test_key") as client:
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


async def test_messages_are_handled_in_correct_order(phoenix_server: FakePhoenixServer):
    received_messages: list[int] = []
    all_messages_received = asyncio.Event()
    expected_message_count = 5

    async def ordered_callback(message: Message) -> None:
        received_messages.append(message.payload["sequence_id"])
        if len(received_messages) == expected_message_count:
            all_messages_received.set()

    async with V1Client(phoenix_server.url, api_key="test_key") as client:
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
    phoenix_server: FakePhoenixServer,
):
    async def test_callback(message: Message) -> None:
        _ = message

    client = V1Client(phoenix_server.url, api_key="test_key")

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
    phoenix_server: FakePhoenixServer,
):
    handler_count = 0
    message_handler_count = 0
    message_handler_event = asyncio.Event()
    specific_event = asyncio.Event()

    async def message_handler(message: Message) -> None:
        nonlocal message_handler_count
        _ = message
        message_handler_count += 1
        message_handler_event.set()

    async def count_handler(payload: dict[str, object]) -> None:
        nonlocal handler_count
        _ = payload
        handler_count += 1
        specific_event.set()

    async with V1Client(phoenix_server.url, api_key="test_key") as client:
        await client.subscribe_to_topic("test-topic", message_handler)

        await phoenix_server.simulate_server_event(
            "test-topic", "count_me", {}, join_ref="1"
        )
        await message_handler_event.wait()
        message_handler_event.clear()
        assert handler_count == 0
        assert message_handler_count == 1

        client.add_event_handler("test-topic", UserEvent("count_me"), count_handler)
        await phoenix_server.simulate_server_event(
            "test-topic", UserEvent("count_me"), {}, join_ref="1"
        )
        await message_handler_event.wait()
        await specific_event.wait()
        message_handler_event.clear()
        specific_event.clear()
        assert handler_count == 1
        assert message_handler_count == 2

        client.remove_event_handler("test-topic", UserEvent("count_me"))
        await phoenix_server.simulate_server_event(
            "test-topic", UserEvent("count_me"), {}, join_ref="1"
        )
        await message_handler_event.wait()
        assert handler_count == 1
        assert message_handler_count == 3


async def test_reconnect_resubscribes_and_receives_messages(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.close_on_join_ids.add(1)

    received_messages: list[Message] = []
    callback_event = asyncio.Event()

    async def callback(message: Message) -> None:
        if _event_name(message) == "post_reconnect":
            received_messages.append(message)
            callback_event.set()

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
        callback_drain_timeout_s=0.2,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)

        original_join_ref = client.get_current_subscriptions()["test-topic"].join_ref

        await _wait_until(
            lambda: client.get_current_subscriptions()["test-topic"].join_ref
            != original_join_ref,
            timeout_s=2.0,
        )

        await _wait_until(
            lambda: client.get_current_subscriptions()[
                "test-topic"
            ].current_join_ready.done(),
            timeout_s=2.0,
        )
        assert len(phoenix_server.list_request_paths()) >= 2
        assert all(
            "api_key" not in path for path in phoenix_server.list_request_paths()
        )
        assert all(
            "test_key" not in path for path in phoenix_server.list_request_paths()
        )
        assert phoenix_server.list_request_api_keys()[-2:] == ["test_key", "test_key"]

        current_join_ref = client.get_current_subscriptions()["test-topic"].join_ref

        await phoenix_server.simulate_server_event(
            "test-topic",
            "post_reconnect",
            {"status": "ok"},
            join_ref=current_join_ref,
        )

        await asyncio.wait_for(callback_event.wait(), timeout=1.0)

    assert len(received_messages) == 1
    assert received_messages[0].payload == {"status": "ok"}


async def test_stale_queued_messages_are_dropped_after_reconnect(
    phoenix_server: FakePhoenixServer,
):
    received_ids: list[int] = []
    callback_gate = asyncio.Event()
    first_started = asyncio.Event()

    async def callback(message: Message) -> None:
        message_id = int(message.payload["id"])
        received_ids.append(message_id)
        if message_id == 1:
            first_started.set()
            await callback_gate.wait()

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)

        old_join_ref = client.get_current_subscriptions()["test-topic"].join_ref

        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 1}, join_ref=old_join_ref
        )
        await asyncio.wait_for(first_started.wait(), timeout=1.0)

        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 2}, join_ref=old_join_ref
        )

        await phoenix_server.close_all_clients()

        await _wait_until(
            lambda: client.get_current_subscriptions()["test-topic"].join_ref
            != old_join_ref,
            timeout_s=3.0,
        )

        callback_gate.set()

        await _wait_until(
            lambda: client.get_current_subscriptions()[
                "test-topic"
            ].current_join_ready.done(),
            timeout_s=2.0,
        )

        new_join_ref = client.get_current_subscriptions()["test-topic"].join_ref
        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 3}, join_ref=new_join_ref
        )

        await _wait_until(lambda: 3 in received_ids, timeout_s=1.0)

    assert received_ids == [1, 3]


async def test_reconnect_partial_recovery_unregisters_failed_topic_only(
    phoenix_server: FakePhoenixServer,
):
    callback_event = asyncio.Event()
    received_messages: list[Message] = []

    async def callback_a(message: Message) -> None:
        if _event_name(message) == "after_reconnect":
            received_messages.append(message)
            callback_event.set()

    async def callback_b(message: Message) -> None:
        _ = message

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback_a)
        await client.subscribe_to_topic("test-topic-b", callback_b)

        old_join_ref = client.get_current_subscriptions()["test-topic"].join_ref
        phoenix_server.fail_join_targets.add((2, "test-topic-b"))

        await phoenix_server.close_all_clients()

        await _wait_until(
            lambda: "test-topic-b" not in client.get_current_subscriptions(),
            timeout_s=2.0,
        )
        await _wait_until(
            lambda: client.get_current_subscriptions()["test-topic"].join_ref
            != old_join_ref,
            timeout_s=2.0,
        )
        await _wait_until(
            lambda: client.get_current_subscriptions()[
                "test-topic"
            ].current_join_ready.done(),
            timeout_s=2.0,
        )

        new_join_ref = client.get_current_subscriptions()["test-topic"].join_ref
        await phoenix_server.simulate_server_event(
            "test-topic",
            "after_reconnect",
            {"ok": True},
            join_ref=new_join_ref,
        )
        await asyncio.wait_for(callback_event.wait(), timeout=1.0)

    assert len(received_messages) == 1
    assert received_messages[0].payload == {"ok": True}


async def test_transient_rejoin_failure_keeps_subscription_and_recovers(
    phoenix_server: FakePhoenixServer,
    monkeypatch: pytest.MonkeyPatch,
):
    callback_event = asyncio.Event()

    async def callback(message: Message) -> None:
        if _event_name(message) == "after_recover":
            callback_event.set()

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)

        protocol_handler = client.get_protocol_handler()
        original_send = protocol_handler.send_message
        inject_rejoin_failure = False

        async def flaky_send(websocket, message: Message) -> None:
            nonlocal inject_rejoin_failure
            if inject_rejoin_failure and message.event == PHXEvent.join:
                inject_rejoin_failure = False
                await websocket.close(code=1012, reason="restart during rejoin")
                raise PHXConnectionError("injected transient rejoin failure")
            await original_send(websocket, message)

        monkeypatch.setattr(protocol_handler, "send_message", flaky_send)

        old_join_ref = client.get_current_subscriptions()["test-topic"].join_ref
        inject_rejoin_failure = True
        await phoenix_server.close_all_clients(code=1012, reason="service restart")

        await _wait_until(
            lambda: client.get_current_subscriptions()["test-topic"].join_ref
            != old_join_ref,
            timeout_s=3.0,
        )
        await _wait_until(
            lambda: client.get_current_subscriptions()[
                "test-topic"
            ].current_join_ready.done(),
            timeout_s=3.0,
        )

        assert "test-topic" in client.get_current_subscriptions()

        new_join_ref = client.get_current_subscriptions()["test-topic"].join_ref
        await phoenix_server.simulate_server_event(
            "test-topic",
            "after_recover",
            {"ok": True},
            join_ref=new_join_ref,
        )
        await asyncio.wait_for(callback_event.wait(), timeout=1.0)


async def test_reconnect_is_suppressed_after_rapid_disconnect_threshold(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.close_on_join_ids.update({1, 2, 3, 4, 5, 6, 7})

    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.05,
        stable_reset_s=0.1,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.02,
        rapid_disconnect_uptime_s=1.0,
        rapid_window_s=2.0,
        rapid_first_min_delay_s=0.01,
        rapid_second_min_delay_s=0.02,
        rapid_cooldown_base_s=0.03,
        rapid_cooldown_step_s=0.01,
        rapid_cooldown_max_s=0.05,
        rapid_suppress_disconnect_count=3,
        rapid_hold_down_jitter_low_ratio=0.5,
        rapid_hold_down_jitter_high_ratio=1.0,
    )

    async def callback(message: Message) -> None:
        _ = message

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=policy,
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)

        with pytest.raises(PHXConnectionError, match="Reconnect suppressed"):
            await asyncio.wait_for(client.run_forever(), timeout=2.0)


async def test_topic_queue_drops_oldest_when_full(
    phoenix_server: FakePhoenixServer,
):
    async def callback(message: Message) -> None:
        _ = message

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        max_topic_queue_size=1,
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)
        subscription = client.get_current_subscriptions()["test-topic"]
        join_ref = subscription.join_ref

        assert subscription.process_topic_messages_task is not None
        subscription.process_topic_messages_task.cancel()
        with suppress(asyncio.CancelledError):
            await subscription.process_topic_messages_task

        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 1}, join_ref=join_ref
        )
        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 2}, join_ref=join_ref
        )
        await phoenix_server.simulate_server_event(
            "test-topic", "work", {"id": 3}, join_ref=join_ref
        )

        await _wait_until(lambda: subscription.queue.qsize() == 1, timeout_s=1.0)
        queued_message = subscription.queue.get_nowait()

        assert queued_message.payload["id"] == 3
        assert subscription.dropped_message_count == 2


async def test_close_code_1008_stops_reconnect_with_terminal_error(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.close_on_join_ids.add(1)
    phoenix_server.close_on_join_code = 1008
    phoenix_server.close_on_join_reason = "policy violation"

    async def callback(message: Message) -> None:
        _ = message

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)
        with pytest.raises(PHXConnectionError, match="terminal close code 1008"):
            await asyncio.wait_for(client.run_forever(), timeout=2.0)


async def test_normal_close_does_not_reconnect_by_default(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.close_on_join_ids.add(1)
    phoenix_server.close_on_join_code = 1000
    phoenix_server.close_on_join_reason = "normal closure"

    async def callback(message: Message) -> None:
        _ = message

    async with V1Client(
        phoenix_server.url,
        api_key="test_key",
        reconnect_policy=_test_reconnect_policy(),
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client:
        await client.subscribe_to_topic("test-topic", callback)
        await asyncio.wait_for(client.run_forever(), timeout=2.0)

    assert phoenix_server.get_connection_attempts("/socket/websocket") == 1


def _jain_index(values: list[float]) -> float:
    if not values:
        return 0.0
    numerator = sum(values) ** 2
    denominator = len(values) * sum(value * value for value in values)
    if denominator <= 0:
        return 0.0
    return numerator / denominator


async def test_reconnect_contention_has_bounded_rate_and_fairness(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.enforce_single_connection_per_api_key = True
    phoenix_server.duplicate_close_code = 1013
    phoenix_server.duplicate_close_reason = "try again later"

    url_a = f"ws://{phoenix_server.host}:{phoenix_server.port}/socket/websocket-a"
    url_b = f"ws://{phoenix_server.host}:{phoenix_server.port}/socket/websocket-b"
    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.1,
        stable_reset_s=1.0,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.03,
        try_again_later_min_delay_s=0.04,
        try_again_later_max_delay_s=0.08,
        rapid_disconnect_uptime_s=0.2,
        rapid_window_s=2.0,
        rapid_first_min_delay_s=0.02,
        rapid_second_min_delay_s=0.04,
        rapid_cooldown_base_s=0.06,
        rapid_cooldown_step_s=0.02,
        rapid_cooldown_max_s=0.1,
        rapid_suppress_disconnect_count=20,
        rapid_hold_down_jitter_low_ratio=0.2,
        rapid_hold_down_jitter_high_ratio=1.0,
    )

    async def callback(message: Message) -> None:
        _ = message

    async with V1Client(
        url_a,
        api_key="shared-agent",
        reconnect_policy=policy,
        join_timeout_s=1.0,
        leave_timeout_s=1.0,
    ) as client_a:
        await client_a.subscribe_to_topic("test-topic", callback)

        async with V1Client(
            url_b,
            api_key="shared-agent",
            reconnect_policy=policy,
            join_timeout_s=1.0,
            leave_timeout_s=1.0,
        ) as client_b:
            await client_b.subscribe_to_topic("test-topic", callback)

            task_a = asyncio.create_task(client_a.run_forever())
            task_b = asyncio.create_task(client_b.run_forever())
            observation_s = 1.2
            await asyncio.sleep(observation_s)

            await asyncio.gather(
                client_a.shutdown("test contention done"),
                client_b.shutdown("test contention done"),
                return_exceptions=True,
            )

            await asyncio.gather(task_a, task_b, return_exceptions=True)

    attempts_a = float(phoenix_server.get_connection_attempts("/socket/websocket-a"))
    attempts_b = float(phoenix_server.get_connection_attempts("/socket/websocket-b"))
    rates = [attempts_a / 1.2, attempts_b / 1.2]
    fairness = _jain_index(rates)

    assert all(math.isfinite(rate) for rate in rates)
    assert max(rates) <= 30.0
    assert fairness >= 0.6
