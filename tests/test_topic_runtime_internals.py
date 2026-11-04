"""Topic runtime branches the fake server can't reach: races and defensive paths."""

from __future__ import annotations

import asyncio
from typing import NoReturn

import pytest

from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import ChannelError, SocketError
from phoenix_channels_python_client.messages import Message, SystemEvent
from phoenix_channels_python_client.topic_subscription import (
    ChannelCause,
    ChannelState,
)
from phoenix_channels_python_client.utils import make_message
from tests.fake_server import ReplyStatus
from tests.harness import (
    DEFAULT_JOIN_REF,
    TopicRuntimeHarness,
    make_subscription,
)
from tests.support import (
    ASYNC_TIMEOUT_S,
    TOPIC,
    LostTopics,
    is_joined,
    join_channel,
    wait_for_condition,
    wait_forever,
)

USER_EVENT = "user:event"


async def test_a_subscription_without_event_handlers_reports_none() -> None:
    assert make_subscription().has_event_handler(SystemEvent.reply) is False


async def test_a_non_reply_while_joining_leaves_the_join_pending() -> None:
    topic = make_subscription()

    TopicRuntimeHarness()._handle_join_response_mode(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert not topic.current_join_ready.done()


async def test_a_join_error_without_a_reason_fails_the_join() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    reply = make_message(
        SystemEvent.reply,
        TOPIC,
        payload={"status": ReplyStatus.ERROR, "response": "not-a-dict"},
        ref=topic.join_ref,
    )

    runtime._handle_join_response_mode(topic, reply)

    assert isinstance(topic.current_join_ready.exception(), ChannelError)


async def test_a_non_reply_while_leaving_leaves_the_leave_pending() -> None:
    topic = make_subscription()

    topic.leave_ref = "leave"
    topic.state = ChannelState.LEFT
    TopicRuntimeHarness()._handle_channel_control(
        topic, make_message(USER_EVENT, TOPIC, payload={})
    )

    assert not topic.unsubscribe_completed.done()


async def test_a_failed_leave_reply_fails_the_leave() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    topic.leave_ref = "leave"
    topic.state = ChannelState.LEFT
    reply = make_message(
        SystemEvent.reply,
        TOPIC,
        payload={"status": ReplyStatus.ERROR},
        ref=topic.leave_ref,
    )

    runtime._handle_channel_control(topic, reply)

    assert isinstance(topic.unsubscribe_completed.exception(), ChannelError)


async def test_unregistering_an_unknown_topic_does_nothing() -> None:
    runtime = TopicRuntimeHarness()

    await runtime._unregister_topic("missing-topic")

    assert runtime.topic_subscriptions == {}


async def test_a_connection_lost_before_the_join_is_sent_fails_the_subscribe() -> None:
    runtime = TopicRuntimeHarness()
    # An uncontended lock doesn't yield, so holding it opens the window.
    async with runtime.topics_lock:
        subscribe = asyncio.create_task(join_channel(runtime, TOPIC))
        await asyncio.sleep(0)
        assert not subscribe.done()
        runtime.connection = None

    with pytest.raises(SocketError, match="Cannot join"):
        await asyncio.wait_for(subscribe, ASYNC_TIMEOUT_S)
    assert TOPIC not in runtime.topic_subscriptions
    assert runtime.sent_messages == []


async def test_a_rejoin_without_a_connection_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.connection = None

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime.topic_subscriptions


async def test_a_rejoin_failing_during_shutdown_keeps_the_topic() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    runtime.shutdown_event.set()
    runtime.state = ClientState.SHUTTING_DOWN

    await runtime._rejoin_topics(generation=2)

    assert topic.name in runtime.topic_subscriptions


async def test_a_rejoin_skips_a_topic_being_left() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())
    topic.leave_requested.set()

    await runtime._rejoin_topics(generation=2)

    assert runtime.sent_messages == []


async def test_a_rejoin_skips_a_topic_unregistered_while_its_task_stops() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription())

    async def unregister_when_stopped() -> None:
        # Stands in for a shutdown landing while the rejoin stops this task.
        try:
            await wait_forever()
        finally:
            runtime.topic_subscriptions.pop(topic.name)

    task = asyncio.create_task(unregister_when_stopped())
    topic.process_topic_messages_task = task
    # Let the task start, so the rejoin's cancel lands inside its try.
    await asyncio.sleep(0)

    await runtime._rejoin_topics(generation=2)

    assert runtime.sent_messages == []
    assert topic.join_ref == DEFAULT_JOIN_REF
    assert topic.process_topic_messages_task is task


async def test_the_processor_exits_for_an_unknown_topic() -> None:
    await asyncio.wait_for(
        TopicRuntimeHarness()._process_topic_messages("missing-topic"),
        ASYNC_TIMEOUT_S,
    )


async def test_the_processor_skips_an_older_join_and_delivers_the_current_one() -> None:
    runtime = TopicRuntimeHarness()
    topic = runtime.register(make_subscription(join_ref="new"))
    received: asyncio.Queue[Message] = asyncio.Queue()
    topic.async_callback = received.put
    runtime._set_subscription_ready(topic)
    for join_ref in ("old", "new"):
        topic.queue.put_nowait(make_message(USER_EVENT, TOPIC, join_ref=join_ref))

    processor = asyncio.create_task(runtime._process_topic_messages(TOPIC))
    try:
        message = await asyncio.wait_for(received.get(), ASYNC_TIMEOUT_S)
        assert message.join_ref == "new"
        assert received.empty()
    finally:
        processor.cancel()
        await asyncio.gather(processor, return_exceptions=True)


async def test_a_processor_queue_failure_retires_the_channel_and_reports_it_lost() -> (
    None
):
    failure = RuntimeError("queue failure")

    class FailedQueue(asyncio.Queue[Message]):
        async def get(self) -> Message:
            raise failure

    lost = LostTopics()
    runtime = TopicRuntimeHarness(on_topic_lost=lost)
    topic = runtime.register(make_subscription(queue=FailedQueue()))
    runtime._set_subscription_ready(topic)
    processor = asyncio.create_task(runtime._process_topic_messages(TOPIC))
    topic.process_topic_messages_task = processor

    assert await wait_for_condition(lambda: len(lost.lost) == 1)
    await asyncio.wait({processor})

    assert TOPIC not in runtime.topic_subscriptions
    error = topic.current_join_ready.exception()
    assert isinstance(error, ChannelError)
    assert error.__cause__ is failure
    assert lost.lost == [(TOPIC, error)]


async def test_each_channel_rejoin_attempt_backs_off_further() -> None:
    timed_out_rejoins = 3
    runtime = TopicRuntimeHarness(waits_before_stop=timed_out_rejoins + 1)
    topic = runtime.register(make_subscription(conn_generation=runtime.conn_generation))
    runtime._transition_channel(topic, ChannelCause.CRASH, ChannelError("crashed"))

    try:
        await asyncio.wait_for(
            runtime._recover_channel(topic, generation=runtime.conn_generation),
            ASYNC_TIMEOUT_S,
        )
    finally:
        runtime.connection = None
        await runtime._unregister_topic(TOPIC)

    # Each unanswered rejoin times out, so every backoff is one attempt further.
    assert runtime.rejoin_backoff_attempts == list(range(timed_out_rejoins + 1))
    # The shutdown during the last backoff stops the rejoin it would have sent.
    sent = [message.event for message in runtime.sent_messages]
    assert sent == [SystemEvent.join] * timed_out_rejoins


async def test_reader_observed_close_prevents_queued_application_delivery() -> None:
    received: list[str] = []

    async def on_message(message: Message) -> None:
        received.append(str(message.event))

    lost = LostTopics()
    runtime = TopicRuntimeHarness(on_topic_lost=lost)
    topic = runtime.register(make_subscription())
    topic.async_callback = on_message
    runtime._set_subscription_ready(topic)
    topic.queue.put_nowait(make_message(USER_EVENT, TOPIC, join_ref=topic.join_ref))
    runtime._handle_channel_control(
        topic,
        make_message(
            SystemEvent.close, TOPIC, join_ref=topic.join_ref, ref=topic.join_ref
        ),
    )
    processor = asyncio.create_task(runtime._process_topic_messages(TOPIC))

    assert await wait_for_condition(lambda: bool(lost.lost))
    assert received == []
    assert not is_joined(runtime, TOPIC)
    lost.only_error()

    processor.cancel()
    await asyncio.gather(processor, return_exceptions=True)


async def test_losing_a_replaced_topic_reports_nothing_and_keeps_the_replacement() -> (
    None
):
    lost = LostTopics()
    runtime = TopicRuntimeHarness(on_topic_lost=lost)
    replaced = make_subscription()
    replacement = runtime.register(make_subscription())

    await runtime._lose_topic(replaced, ChannelError("lost"))

    assert runtime.topic_subscriptions.copy()[TOPIC] is replacement
    assert not lost.lost


async def test_a_failed_lifecycle_task_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fail() -> NoReturn:
        raise RuntimeError

    task = TopicRuntimeHarness()._track_lifecycle_task(fail())
    await asyncio.wait({task}, timeout=ASYNC_TIMEOUT_S)
    await asyncio.sleep(0)  # done-callbacks run on the next loop turn

    assert "Channel lifecycle task failed" in caplog.text
