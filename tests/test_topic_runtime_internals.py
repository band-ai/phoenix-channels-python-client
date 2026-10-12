"""Topic runtime branches the fake server can't reach: races and defensive paths."""

from __future__ import annotations

import asyncio
from typing import NoReturn

import pytest

from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import PHXEvent, UserEvent
from phoenix_channels_python_client.utils import make_message
from tests.fake_server import ReplyStatus
from tests.harness import (
    DEFAULT_JOIN_REF,
    TopicRuntimeHarness,
    make_subscription,
)
from tests.support import ASYNC_TIMEOUT_S, TOPIC, wait_forever

USER_EVENT = UserEvent("user:event")


async def test_a_subscription_without_event_handlers_reports_none() -> None:
    assert make_subscription().has_event_handler(PHXEvent.reply) is False


async def test_a_non_reply_while_joining_leaves_the_join_pending() -> None:
    topic = make_subscription()

    await TopicRuntimeHarness()._handle_join_response_mode(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert not topic.current_join_ready.done()


async def test_a_join_error_without_a_reason_fails_the_join() -> None:
    topic = make_subscription()
    reply = make_message(
        PHXEvent.reply,
        TOPIC,
        payload={"status": ReplyStatus.ERROR, "response": "not-a-dict"},
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
    reply = make_message(PHXEvent.reply, TOPIC, payload={"status": ReplyStatus.ERROR})

    await TopicRuntimeHarness()._handle_leave_mode(topic, reply)

    assert isinstance(topic.unsubscribe_completed.exception(), PHXTopicError)


async def test_unregistering_an_unknown_topic_does_nothing() -> None:
    runtime = TopicRuntimeHarness()

    await runtime._unregister_topic("missing-topic")

    assert runtime._topic_subscriptions == {}


async def test_a_connection_lost_before_the_join_is_sent_fails_the_subscribe() -> None:
    runtime = TopicRuntimeHarness()
    # An uncontended lock doesn't yield, so holding it opens the window.
    async with runtime._topics_lock:
        subscribe = asyncio.create_task(runtime.subscribe_to_topic(TOPIC))
        await asyncio.sleep(0)
        assert not subscribe.done()
        runtime.connection = None

    with pytest.raises(
        PHXConnectionError, match="Connection lost before join could be sent"
    ):
        await asyncio.wait_for(subscribe, ASYNC_TIMEOUT_S)
    assert TOPIC not in runtime._topic_subscriptions


async def test_a_rejoin_without_a_connection_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.connection = None

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime._topic_subscriptions


async def test_a_rejoin_failing_during_shutdown_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.fake_handler.raise_on_send = RuntimeError("send fail")
    runtime._shutdown_event.set()
    runtime._state = ClientState.SHUTTING_DOWN

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime._topic_subscriptions


async def test_a_rejoin_skips_a_topic_being_left() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    topic.leave_requested.set()

    await runtime._rejoin_topics(generation=2)

    assert runtime.fake_handler.sent == []


async def test_a_rejoin_skips_a_topic_unregistered_while_its_task_stops() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())

    async def unregister_when_stopped() -> None:
        # Stands in for a shutdown landing while the rejoin stops this task.
        try:
            await wait_forever()
        finally:
            runtime._topic_subscriptions.pop(topic.name)

    task = asyncio.create_task(unregister_when_stopped())
    topic.process_topic_messages_task = task
    # Let the task start, so the rejoin's cancel lands inside its try.
    await asyncio.sleep(0)

    await runtime._rejoin_topics(generation=2)

    assert runtime.fake_handler.sent == []
    assert topic.join_ref == DEFAULT_JOIN_REF
    assert topic.process_topic_messages_task is task


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
        make_message(
            PHXEvent.reply, TOPIC, payload={"status": ReplyStatus.ERROR}, join_ref="old"
        )
    )
    topic.queue.put_nowait(
        make_message(
            PHXEvent.reply, TOPIC, payload={"status": ReplyStatus.OK}, join_ref="new"
        )
    )

    await asyncio.wait_for(runtime._process_topic_messages(TOPIC), ASYNC_TIMEOUT_S)

    assert topic.unsubscribe_completed.result() is None


async def test_a_processor_error_unregisters_the_topic_with_that_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    failure = RuntimeError("state boom")

    def fail(_: object) -> NoReturn:
        raise failure

    monkeypatch.setattr(runtime, "_determine_processing_state", fail)
    topic.queue.put_nowait(
        make_message(
            PHXEvent.reply,
            TOPIC,
            payload={"status": ReplyStatus.OK},
            join_ref=DEFAULT_JOIN_REF,
        )
    )

    await asyncio.wait_for(runtime._process_topic_messages(TOPIC), ASYNC_TIMEOUT_S)

    assert TOPIC not in runtime._topic_subscriptions
    assert topic.current_join_ready.exception() is failure
