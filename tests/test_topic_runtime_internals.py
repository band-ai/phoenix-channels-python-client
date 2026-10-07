"""Topic runtime branches the fake server can't reach: races and defensive paths."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import PHXEvent, UserEvent
from phoenix_channels_python_client.topic_subscription import TopicProcessingState
from phoenix_channels_python_client.utils import make_message

from tests.harness import (
    FakeTopicProtocolHandler,
    TopicRuntimeHarness,
    make_subscription,
)
from tests.support import ASYNC_TIMEOUT_S, TOPIC

USER_EVENT = UserEvent("user:event")


async def test_a_subscription_without_event_handlers_reports_none() -> None:
    assert make_subscription().has_event_handler(PHXEvent.reply) is False


async def test_the_processing_state_follows_the_join_then_the_leave() -> None:
    runtime = TopicRuntimeHarness()
    topic = make_subscription()

    assert (
        runtime._determine_processing_state(topic)
        is TopicProcessingState.WAITING_FOR_JOIN
    )
    topic.current_join_ready.set_result(None)
    assert (
        runtime._determine_processing_state(topic)
        is TopicProcessingState.NORMAL_PROCESSING
    )
    topic.leave_requested.set()
    assert (
        runtime._determine_processing_state(topic)
        is TopicProcessingState.PROCESSING_LEAVE
    )


async def test_a_non_reply_while_joining_leaves_the_join_pending() -> None:
    topic = make_subscription()

    await TopicRuntimeHarness()._handle_join_response_mode(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert not topic.current_join_ready.done()


async def test_a_join_error_without_a_reason_fails_the_join() -> None:
    topic = make_subscription()
    reply = make_message(
        PHXEvent.reply, TOPIC, payload={"status": "error", "response": "not-a-dict"}
    )

    await TopicRuntimeHarness()._handle_join_response_mode(topic, reply)

    assert isinstance(topic.current_join_ready.exception(), PHXTopicError)


async def test_a_non_reply_while_leaving_leaves_the_leave_pending() -> None:
    topic = make_subscription()

    await TopicRuntimeHarness()._handle_leave_mode(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert not topic.unsubscribe_completed.done()


async def test_a_failed_leave_reply_fails_the_leave() -> None:
    topic = make_subscription()
    reply = make_message(PHXEvent.reply, TOPIC, payload={"status": "error"})

    await TopicRuntimeHarness()._handle_leave_mode(topic, reply)

    assert isinstance(topic.unsubscribe_completed.exception(), PHXTopicError)


async def test_a_message_without_handlers_starts_no_callback() -> None:
    topic = make_subscription()

    await TopicRuntimeHarness()._handle_normal_message_mode(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert topic.current_callback_task is None


async def test_setting_an_error_on_a_finished_future_keeps_its_result() -> None:
    future = asyncio.get_running_loop().create_future()
    future.set_result(None)

    TopicRuntimeHarness()._set_future_exception(future, PHXConnectionError("late"))

    assert future.result() is None


async def test_unregistering_an_unknown_topic_does_nothing() -> None:
    runtime = TopicRuntimeHarness()

    await runtime._unregister_topic("missing-topic")

    assert runtime._topic_subscriptions == {}


async def test_draining_a_topic_queue_empties_it() -> None:
    topic = make_subscription()
    topic.queue.put_nowait(make_message(USER_EVENT, TOPIC, payload={}))

    TopicRuntimeHarness()._drain_topic_queue(topic)

    assert topic.queue.empty()


async def test_a_connection_lost_before_the_join_is_sent_fails_the_subscribe() -> None:
    runtime = TopicRuntimeHarness()

    def lose_connection_after_the_check(_: str) -> None:
        runtime.connection = None

    runtime._ensure_can_send = lose_connection_after_the_check  # type: ignore[method-assign]

    with pytest.raises(
        PHXConnectionError, match="Connection lost before join could be sent"
    ):
        await runtime.subscribe_to_topic(TOPIC)
    assert TOPIC not in runtime._topic_subscriptions


async def test_unsubscribing_without_a_connection_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.connection = None
    runtime._ensure_can_send = lambda _: None  # type: ignore[method-assign]

    await runtime.unsubscribe_from_topic(topic.name)

    assert topic.name in runtime._topic_subscriptions


async def test_a_rejoin_without_a_connection_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.connection = None

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime._topic_subscriptions


async def test_a_rejoin_failing_during_shutdown_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    assert isinstance(runtime._protocol_handler, FakeTopicProtocolHandler)
    runtime._protocol_handler.raise_on_send = RuntimeError("send fail")
    runtime._shutdown_event.set()
    runtime._state = ClientState.SHUTTING_DOWN

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime._topic_subscriptions


async def test_a_rejoin_skips_a_topic_being_left() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    topic.leave_requested.set()

    await runtime._rejoin_topics(generation=2)

    assert topic.join_ref == "1"


async def test_the_processor_exits_for_an_unknown_topic() -> None:
    await asyncio.wait_for(
        TopicRuntimeHarness()._process_topic_messages("missing-topic"),
        ASYNC_TIMEOUT_S,
    )


async def test_the_processor_skips_an_older_join_and_stops_on_the_leave_reply() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription(join_ref="new"))
    topic.current_join_ready.set_result(None)
    topic.leave_requested.set()
    topic.queue.put_nowait(
        make_message(PHXEvent.reply, TOPIC, payload={"status": "error"}, join_ref="old")
    )
    topic.queue.put_nowait(
        make_message(PHXEvent.reply, TOPIC, payload={"status": "ok"}, join_ref="new")
    )

    await asyncio.wait_for(runtime._process_topic_messages(TOPIC), ASYNC_TIMEOUT_S)

    assert topic.unsubscribe_completed.result() is None


async def test_a_processor_error_unregisters_the_topic_with_that_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    failure = RuntimeError("state boom")

    def fail(_: Any) -> Any:
        raise failure

    monkeypatch.setattr(runtime, "_determine_processing_state", fail)
    topic.queue.put_nowait(
        make_message(PHXEvent.reply, TOPIC, payload={"status": "ok"}, join_ref="1")
    )

    await asyncio.wait_for(runtime._process_topic_messages(TOPIC), ASYNC_TIMEOUT_S)

    assert TOPIC not in runtime._topic_subscriptions
    assert topic.current_join_ready.exception() is failure
