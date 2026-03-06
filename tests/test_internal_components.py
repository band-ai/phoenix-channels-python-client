from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, cast

import pytest
from websockets import ClientConnection

from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectDecision,
    ReconnectPolicy,
    reconnect_policy_is_invalid,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import PHXEvent, UserEvent
from phoenix_channels_python_client.protocol_handler import (
    PHXProtocolHandler,
    PhoenixChannelsProtocolVersion,
)
from phoenix_channels_python_client.supervisor import SupervisorMixin
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from phoenix_channels_python_client.topic_subscription import TopicSubscription
from phoenix_channels_python_client.utils import make_message


@dataclass
class _FakeConnection:
    messages: list[str]

    def __post_init__(self) -> None:
        self._index = 0
        self.sent: list[str] = []

    def __aiter__(self) -> _FakeConnection:
        return self

    async def __anext__(self) -> str:
        if self._index >= len(self.messages):
            raise StopAsyncIteration
        value = self.messages[self._index]
        self._index += 1
        return value

    async def send(self, text: str) -> None:
        self.sent.append(text)


class _QueueRaisesOnDrop(asyncio.Queue[Any]):
    def full(self) -> bool:
        return True

    def get_nowait(self) -> Any:
        raise asyncio.QueueEmpty

    async def put(self, item: Any) -> None:
        cast(Any, self)._queue.append(item)


@dataclass
class _FakeSocket:
    close_code: int | None = None
    close_reason: str | None = None
    close_raises: bool = False

    def __post_init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        if self.close_raises:
            raise RuntimeError("close boom")


class _FakeRoutingProtocolHandler:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def process_websocket_messages(
        self,
        connection: ClientConnection,
        subscriptions: dict[str, TopicSubscription],
        conn_generation: int,
        **kwargs: Any,
    ) -> None:
        del connection, subscriptions, conn_generation, kwargs
        if self.error is not None:
            raise self.error


class _FakeTopicProtocolHandler(PHXProtocolHandler):
    def __init__(self, protocol_version: PhoenixChannelsProtocolVersion) -> None:
        super().__init__(protocol_version)
        self.raise_on_send: Exception | None = None

    async def send_message(self, websocket: ClientConnection, message: Any) -> None:
        del websocket, message
        if self.raise_on_send is not None:
            raise self.raise_on_send


class _TopicRuntimeHarness(TopicRuntimeMixin):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.connection: ClientConnection | None = cast(ClientConnection, _FakeSocket())
        self._state = ClientState.CONNECTED
        self._ref_counter = 0
        self._conn_generation = 1
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self._protocol_handler = _FakeTopicProtocolHandler(
            PhoenixChannelsProtocolVersion.V2
        )
        self._topics_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self.join_timeout_s = 0.01
        self.leave_timeout_s = 0.01
        self.max_topic_queue_size = 10
        self.callback_drain_timeout_s = 0.01


class _SupervisorHarness(SupervisorMixin):
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.channel_socket_url = "ws://unit-test/socket"
        self.channel_socket_url_redacted = "ws://unit-test/socket?api_key=***"
        self.auto_reconnect = True
        self.reconnect_policy = ReconnectPolicy(stable_reset_s=0.0)
        self.connection: ClientConnection | None = None
        self._topic_subscriptions: dict[str, TopicSubscription] = {}
        self._protocol_handler: Any = _FakeRoutingProtocolHandler()
        self._shutdown_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._conn_generation = 0
        self._state = ClientState.CONNECTING
        self._supervisor_task: asyncio.Task[None] | None = None
        self._message_routing_task: asyncio.Task[None] | None = None
        self._initial_connection_future: asyncio.Future[None] | None = (
            asyncio.get_running_loop().create_future()
        )
        self._rapid_disconnects = deque[float]()
        self._terminal_error: Exception | None = None
        self._heartbeat_interval_s: float | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending_heartbeat_ref: str | None = None
        self._ref_counter = 0
        self._on_reconnect = None
        self._on_disconnect = None

        self.transition_history: list[ClientState] = []
        self.disconnect_uptimes: list[float] = []
        self.delay_attempts: list[int] = []
        self.wait_delays: list[float] = []
        self.shutdown_reasons: list[str] = []
        self.suppress_values: list[bool] = []
        self.disconnect_decision = ReconnectDecision(should_reconnect=True)
        self.rejoin_error: Exception | None = None
        self.extract_close_result: tuple[int | None, str] = (None, "")

    async def _rejoin_topics(self, generation: int) -> None:
        del generation
        if self.rejoin_error is not None:
            raise self.rejoin_error

    def _record_disconnect(self, connection_uptime_s: float) -> None:
        self.disconnect_uptimes.append(connection_uptime_s)

    def _should_suppress_reconnect(self) -> bool:
        if self.suppress_values:
            return self.suppress_values.pop(0)
        return False

    def _compute_reconnect_delay(self, attempt: int) -> float:
        self.delay_attempts.append(attempt)
        return 0.001

    def _extract_close_details(
        self, *, connection: ClientConnection, routing_error: Exception | None
    ) -> tuple[int | None, str]:
        del connection, routing_error
        return self.extract_close_result

    def _classify_disconnect(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision:
        del close_code, close_reason
        return self.disconnect_decision

    def _apply_disconnect_delay_override(
        self, computed_delay_s: float, decision: ReconnectDecision
    ) -> float:
        del decision
        return computed_delay_s

    def _transition_state(self, new_state: ClientState) -> None:
        self._state = new_state
        self.transition_history.append(new_state)

    async def _wait_for_shutdown_or_timeout(self, delay_s: float) -> None:
        self.wait_delays.append(delay_s)
        self._shutdown_event.set()

    async def shutdown(self, reason: str) -> None:
        self.shutdown_reasons.append(reason)
        self._shutdown_event.set()


def test_transition_client_state_allows_noop_and_rejects_invalid() -> None:
    assert (
        transition_client_state(ClientState.CLOSED, ClientState.CLOSED)
        == ClientState.CLOSED
    )
    with pytest.raises(RuntimeError):
        transition_client_state(ClientState.CLOSED, ClientState.CONNECTED)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay_s": -1},
        {"factor": 0},
        {"max_delay_s": -1},
        {"stable_reset_s": 0},
        {"service_restart_min_delay_s": -1},
        {"service_restart_max_delay_s": 0, "service_restart_min_delay_s": 1},
        {"try_again_later_min_delay_s": -1},
        {"try_again_later_max_delay_s": 0, "try_again_later_min_delay_s": 1},
        {"rapid_disconnect_uptime_s": -1},
        {"rapid_window_s": 0},
        {"rapid_first_min_delay_s": -1},
        {"rapid_second_min_delay_s": -1},
        {"rapid_cooldown_base_s": -1},
        {"rapid_cooldown_step_s": -1},
        {"rapid_cooldown_max_s": 0, "rapid_cooldown_base_s": 1},
        {"rapid_suppress_disconnect_count": -1},
        {"rapid_hold_down_jitter_low_ratio": -1},
        {
            "rapid_hold_down_jitter_high_ratio": 0.1,
            "rapid_hold_down_jitter_low_ratio": 0.2,
        },
    ],
)
def test_reconnect_policy_is_invalid_covers_all_invalid_guards(
    kwargs: dict[str, Any],
) -> None:
    policy = ReconnectPolicy(**kwargs)
    assert reconnect_policy_is_invalid(policy) is True


def test_reconnect_policy_is_invalid_accepts_defaults() -> None:
    assert reconnect_policy_is_invalid(ReconnectPolicy()) is False


def test_protocol_handler_parse_v2_and_v1_success() -> None:
    v2 = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    msg2 = v2.parse_message(json.dumps(["jr", "r1", "test-topic", "hello", {"k": "v"}]))
    assert msg2.topic == "test-topic"
    assert str(msg2.event) == "hello"
    assert msg2.join_ref == "jr"

    v1 = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1)
    msg1 = v1.parse_message(
        json.dumps(
            {"topic": "test-topic", "event": "hello", "ref": "1", "payload": {"x": 1}}
        )
    )
    assert msg1.topic == "test-topic"
    assert str(msg1.event) == "hello"
    assert msg1.ref == "1"


@pytest.mark.parametrize(
    "handler,raw,expected_exception",
    [
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2),
            json.dumps({"bad": "shape"}),
            TypeError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2),
            json.dumps([1, 2, 3]),
            ValueError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2),
            json.dumps([None, None, "", "evt", {}]),
            TypeError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1),
            json.dumps([1, 2, 3]),
            TypeError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1),
            json.dumps({"topic": "", "event": "x", "payload": {}}),
            TypeError,
        ),
        (
            PHXProtocolHandler(PhoenixChannelsProtocolVersion.V1),
            json.dumps({"topic": "t", "event": "", "payload": {}}),
            TypeError,
        ),
    ],
)
def test_protocol_handler_parse_invalid_shapes_raise(
    handler: PHXProtocolHandler, raw: str, expected_exception: type[Exception]
) -> None:
    with pytest.raises(expected_exception):
        handler.parse_message(raw)


def test_protocol_handler_parse_unexpected_exception_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)

    def _boom(_: str | bytes) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "phoenix_channels_python_client.protocol_handler.json.loads", _boom
    )
    with pytest.raises(ValueError):
        handler.parse_message("ignored")


def test_protocol_handler_serialize_and_send_message() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    message = handler.parse_message(
        json.dumps(["jr", "r1", "test-topic", "evt", {"x": 1}])
    )
    serialized = handler.serialize_message(message)
    assert serialized.startswith("[")

    connection = _FakeConnection(messages=[])
    asyncio.run(handler.send_message(cast(ClientConnection, connection), message))
    assert len(connection.sent) == 1


def test_protocol_handler_serialize_raises_type_error_on_bad_payload() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    message = handler.parse_message(json.dumps(["jr", "r1", "test-topic", "evt", {}]))
    message.payload["bad"] = set([1])  # type: ignore[index]
    with pytest.raises(TypeError):
        handler.serialize_message(message)


@pytest.mark.asyncio
async def test_process_websocket_messages_filters_join_ref() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10)
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="new",
        process_topic_messages_task=None,
        conn_generation=2,
    )

    stale_join_ref = json.dumps(["old", "1", "test-topic", "evt", {"a": 2}])
    good = json.dumps(["new", "1", "test-topic", "evt", {"a": 3}])
    connection = _FakeConnection(messages=[stale_join_ref, good])

    await handler.process_websocket_messages(
        cast(ClientConnection, connection), {"test-topic": sub}, conn_generation=2
    )
    assert queue.qsize() == 1
    only = queue.get_nowait()
    assert only.payload == {"a": 3}


@pytest.mark.asyncio
async def test_process_websocket_messages_filters_stale_generation() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10)
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="jr",
        process_topic_messages_task=None,
        conn_generation=2,
    )
    msg = json.dumps(["jr", "1", "test-topic", "evt", {"x": 1}])
    connection = _FakeConnection(messages=[msg])
    await handler.process_websocket_messages(
        cast(ClientConnection, connection), {"test-topic": sub}, 1
    )
    assert queue.qsize() == 0


@pytest.mark.asyncio
async def test_process_websocket_messages_drop_path_handles_queueempty() -> None:
    handler = PHXProtocolHandler(PhoenixChannelsProtocolVersion.V2)
    queue = _QueueRaisesOnDrop()
    sub = TopicSubscription(
        name="test-topic",
        async_callback=None,
        queue=queue,
        join_ref="jr",
        process_topic_messages_task=None,
        conn_generation=1,
    )
    sub.dropped_message_count = 99

    msg = json.dumps(["jr", "1", "test-topic", "evt", {"x": 1}])
    connection = _FakeConnection(messages=[msg])
    await handler.process_websocket_messages(
        cast(ClientConnection, connection), {"test-topic": sub}, 1
    )
    assert sub.dropped_message_count == 99


@pytest.mark.asyncio
async def test_topic_subscription_has_event_handler_false_when_missing() -> None:
    subscription = TopicSubscription(
        name="room:lobby",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="1",
        process_topic_messages_task=None,
    )
    assert subscription.has_event_handler(PHXEvent.reply) is False


@pytest.mark.asyncio
async def test_topic_runtime_processing_state_and_join_leave_error_paths() -> None:
    runtime = _TopicRuntimeHarness()
    state_topic = TopicSubscription(
        name="room:lobby",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-1",
        process_topic_messages_task=None,
    )

    assert runtime._determine_processing_state(state_topic).value == "waiting_for_join"
    state_topic.current_join_ready.set_result(None)
    state_topic.leave_requested.set()
    assert runtime._determine_processing_state(state_topic).value == "processing_leave"
    state_topic.leave_requested.clear()
    assert runtime._determine_processing_state(state_topic).value == "normal_processing"

    join_topic = TopicSubscription(
        name="room:join",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-2",
        process_topic_messages_task=None,
    )

    non_reply = make_message(UserEvent("user:event"), "room:lobby", payload={})
    await runtime._handle_join_response_mode(join_topic, non_reply)
    assert join_topic.subscription_ready.done() is False

    bad_join_reply = make_message(
        PHXEvent.reply,
        "room:lobby",
        payload={"status": "error", "response": "not-a-dict"},
    )
    await runtime._handle_join_response_mode(join_topic, bad_join_reply)
    assert isinstance(join_topic.current_join_ready.exception(), PHXTopicError)

    leave_topic = TopicSubscription(
        name="room:leave",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-3",
        process_topic_messages_task=None,
    )

    non_reply_leave = make_message(UserEvent("user:event"), "room:lobby", payload={})
    await runtime._handle_leave_mode(leave_topic, non_reply_leave)
    assert leave_topic.unsubscribe_completed.done() is False

    failed_leave_reply = make_message(
        PHXEvent.reply, "room:lobby", payload={"status": "error"}
    )
    await runtime._handle_leave_mode(leave_topic, failed_leave_reply)
    assert isinstance(leave_topic.unsubscribe_completed.exception(), PHXTopicError)


@pytest.mark.asyncio
async def test_topic_runtime_normal_message_mode_and_unregister_helpers() -> None:
    runtime = _TopicRuntimeHarness()
    topic = TopicSubscription(
        name="room:lobby",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-1",
        process_topic_messages_task=None,
    )
    message = make_message(UserEvent("custom"), "room:lobby", payload={"x": 1})

    await runtime._handle_normal_message_mode(topic, message)
    assert topic.current_callback_task is None

    async def failing_handler(_: Any) -> None:
        raise RuntimeError("callback boom")

    topic.async_callback = cast(Any, failing_handler)
    await runtime._handle_normal_message_mode(topic, message)
    assert topic.current_callback_task is None

    ready_future = asyncio.get_running_loop().create_future()
    ready_future.set_result(None)
    runtime._set_future_exception(ready_future, PHXConnectionError("ignored"))
    assert ready_future.result() is None

    await runtime._unregister_topic("missing-topic")
    runtime._drain_topic_queue(topic)
    topic.queue.put_nowait(message)
    runtime._drain_topic_queue(topic)
    assert topic.queue.empty()

    runtime._state = ClientState.CLOSED
    runtime.connection = None
    with pytest.raises(PHXConnectionError):
        runtime._ensure_can_send("subscribe")


@pytest.mark.asyncio
async def test_topic_runtime_subscribe_unsubscribe_and_rejoin_edge_cases() -> None:
    runtime = _TopicRuntimeHarness()

    with pytest.raises(PHXTopicError):
        await runtime.unsubscribe_from_topic("missing-topic")

    with pytest.raises(PHXTopicError):
        await runtime.subscribe_to_topic("room:lobby")

    runtime._ensure_can_send = lambda _: setattr(runtime, "connection", None)  # type: ignore[method-assign]
    with pytest.raises(
        PHXConnectionError, match="Connection lost before join could be sent"
    ):
        await runtime.subscribe_to_topic("room:race")
    assert "room:race" not in runtime._topic_subscriptions

    topic = TopicSubscription(
        name="room:guard",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-guard",
        process_topic_messages_task=None,
    )
    runtime._topic_subscriptions[topic.name] = topic
    runtime._state = ClientState.CONNECTED
    runtime.connection = None
    runtime._ensure_can_send = lambda _: None  # type: ignore[method-assign]
    await runtime.unsubscribe_from_topic(topic.name)
    assert topic.name in runtime._topic_subscriptions

    active = TopicSubscription(
        name="room:active",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-active",
        process_topic_messages_task=asyncio.create_task(asyncio.sleep(10)),
    )
    runtime._topic_subscriptions[active.name] = active
    runtime._shutdown_event.clear()
    runtime._state = ClientState.CONNECTED
    runtime.connection = None
    await runtime._rejoin_topics(generation=2)
    assert active.name in runtime._topic_subscriptions

    shutting_down = TopicSubscription(
        name="room:shutdown",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-shutdown",
        process_topic_messages_task=asyncio.create_task(asyncio.sleep(10)),
    )
    runtime._topic_subscriptions = {shutting_down.name: shutting_down}
    runtime.connection = cast(ClientConnection, _FakeSocket())
    cast(
        _FakeTopicProtocolHandler, runtime._protocol_handler
    ).raise_on_send = RuntimeError("send fail")
    runtime._shutdown_event.set()
    runtime._state = ClientState.SHUTTING_DOWN
    await runtime._rejoin_topics(generation=3)
    assert shutting_down.name in runtime._topic_subscriptions


@pytest.mark.asyncio
async def test_topic_runtime_process_topic_messages_special_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _TopicRuntimeHarness()

    await runtime._process_topic_messages("missing-topic")

    topic = TopicSubscription(
        name="room:queue",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="new",
        process_topic_messages_task=None,
    )
    topic.current_join_ready.set_result(None)
    topic.leave_requested.set()
    runtime._topic_subscriptions[topic.name] = topic

    stale = make_message(PHXEvent.reply, topic.name, payload={}, join_ref="old")
    leave_ok = make_message(
        PHXEvent.reply, topic.name, payload={"status": "ok"}, join_ref=topic.join_ref
    )
    topic.queue.put_nowait(stale)
    topic.queue.put_nowait(leave_ok)
    await asyncio.wait_for(runtime._process_topic_messages(topic.name), timeout=0.2)

    called: dict[str, Any] = {}

    async def fake_unregister(name: str, error: Exception | None = None) -> None:
        called["name"] = name
        called["error"] = error

    def boom_state(_: TopicSubscription) -> Any:
        raise RuntimeError("state boom")

    monkeypatch.setattr(runtime, "_unregister_topic", fake_unregister)
    monkeypatch.setattr(runtime, "_determine_processing_state", boom_state)

    topic2 = TopicSubscription(
        name="room:error",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-2",
        process_topic_messages_task=None,
    )
    runtime._topic_subscriptions[topic2.name] = topic2
    topic2.queue.put_nowait(
        make_message(
            PHXEvent.reply,
            topic2.name,
            payload={"status": "ok"},
            join_ref=topic2.join_ref,
        )
    )
    await asyncio.wait_for(runtime._process_topic_messages(topic2.name), timeout=0.2)
    assert called["name"] == topic2.name
    assert isinstance(called["error"], RuntimeError)


@pytest.mark.asyncio
async def test_topic_runtime_missing_topic_handler_guards() -> None:
    runtime = _TopicRuntimeHarness()
    handler = cast(Any, lambda payload: payload)

    with pytest.raises(PHXTopicError):
        runtime.add_event_handler("missing", PHXEvent.reply, handler)
    with pytest.raises(PHXTopicError):
        runtime.remove_event_handler("missing", PHXEvent.reply)
    with pytest.raises(PHXTopicError):
        runtime.get_event_handler("missing", PHXEvent.reply)
    assert runtime.has_event_handler("missing", PHXEvent.reply) is False
    with pytest.raises(PHXTopicError):
        runtime.list_event_handlers("missing")
    with pytest.raises(PHXTopicError):
        runtime.set_message_handler("missing", cast(Any, handler))
    with pytest.raises(PHXTopicError):
        runtime.remove_message_handler("missing")
    with pytest.raises(PHXTopicError):
        runtime.get_message_handler("missing")
    assert runtime.has_message_handler("missing") is False


@pytest.mark.asyncio
async def test_topic_runtime_existing_topic_handlers_and_rejoin_skip_leave_requested() -> (
    None
):
    runtime = _TopicRuntimeHarness()
    topic = TopicSubscription(
        name="room:existing",
        async_callback=None,
        queue=asyncio.Queue(),
        join_ref="join-existing",
        process_topic_messages_task=None,
    )
    topic.leave_requested.set()
    runtime._topic_subscriptions[topic.name] = topic

    async def event_handler(_: dict[str, Any]) -> None:
        return None

    async def message_handler(_: Any) -> None:
        return None

    runtime.add_event_handler(topic.name, PHXEvent.reply, event_handler)
    assert runtime.get_event_handler(topic.name, PHXEvent.reply) is event_handler
    assert runtime.has_event_handler(topic.name, PHXEvent.reply) is True

    runtime.set_message_handler(topic.name, cast(Any, message_handler))
    assert runtime.get_message_handler(topic.name) is message_handler
    assert runtime.has_message_handler(topic.name) is True

    runtime.remove_event_handler(topic.name, PHXEvent.reply)
    assert runtime.has_event_handler(topic.name, PHXEvent.reply) is False

    runtime.remove_message_handler(topic.name)
    assert runtime.has_message_handler(topic.name) is False

    original_join_ref = topic.join_ref
    await runtime._rejoin_topics(generation=8)
    assert topic.join_ref == original_join_ref


@pytest.mark.asyncio
async def test_supervisor_connect_failures_and_terminal_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()
    harness.auto_reconnect = False

    async def fail_connect(_: str) -> ClientConnection:
        raise RuntimeError("connect fail")

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect", fail_connect
    )
    await harness._supervisor_loop()
    assert harness._initial_connection_future is not None
    assert isinstance(
        harness._initial_connection_future.exception(), PHXConnectionError
    )

    suppressed = _SupervisorHarness()
    suppressed.suppress_values = [True]
    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect", fail_connect
    )
    await suppressed._supervisor_loop()
    assert isinstance(suppressed._terminal_error, PHXConnectionError)


@pytest.mark.asyncio
async def test_supervisor_initial_connect_retries_before_failing_enter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()
    attempts = 0

    async def fail_then_connect(_: str) -> ClientConnection:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient connect fail")
        return cast(ClientConnection, _FakeSocket())

    wait_calls = 0

    async def wait_without_immediate_shutdown(delay_s: float) -> None:
        del delay_s
        nonlocal wait_calls
        wait_calls += 1
        if wait_calls >= 2:
            harness._shutdown_event.set()

    harness._wait_for_shutdown_or_timeout = wait_without_immediate_shutdown  # type: ignore[method-assign]
    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect", fail_then_connect
    )

    await harness._supervisor_loop()

    assert attempts >= 2
    assert harness._initial_connection_future is not None
    assert harness._initial_connection_future.done() is True
    assert harness._initial_connection_future.result() is None


@pytest.mark.asyncio
async def test_supervisor_routing_failure_disconnect_decisions_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()
    harness._protocol_handler = _FakeRoutingProtocolHandler(
        error=RuntimeError("routing boom")
    )
    harness.rejoin_error = RuntimeError("rejoin boom")
    harness._rapid_disconnects.extend([1.0, 2.0])  # type: ignore[attr-defined]

    socket = _FakeSocket(close_code=1012, close_reason="restart")

    async def connect_once(_: str) -> ClientConnection:
        return cast(ClientConnection, socket)

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect", connect_once
    )
    await harness._supervisor_loop()

    assert harness.connection is None
    assert harness.wait_delays
    assert harness.disconnect_uptimes
    assert not harness._rapid_disconnects  # type: ignore[attr-defined]

    no_reconnect = _SupervisorHarness()
    no_reconnect._protocol_handler = _FakeRoutingProtocolHandler()
    no_reconnect.disconnect_decision = ReconnectDecision(should_reconnect=False)
    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect",
        lambda _: asyncio.sleep(0, result=cast(ClientConnection, _FakeSocket())),
    )
    await no_reconnect._supervisor_loop()
    assert ClientState.CLOSED in no_reconnect.transition_history


@pytest.mark.asyncio
async def test_supervisor_initial_future_stop_and_cleanup_exceptions() -> None:
    harness = _SupervisorHarness()
    harness._shutdown_event.set()
    await harness._supervisor_loop()
    assert harness._initial_connection_future is not None
    assert isinstance(
        harness._initial_connection_future.exception(), PHXConnectionError
    )

    cleanup = _SupervisorHarness()
    cleanup.connection = cast(ClientConnection, _FakeSocket(close_raises=True))
    cleanup._message_routing_task = asyncio.create_task(asyncio.sleep(10))
    await cleanup._cleanup_connection()
    assert cleanup.connection is None
    assert cleanup._message_routing_task is None


@pytest.mark.asyncio
async def test_supervisor_run_forever_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _SupervisorHarness()
    harness._supervisor_task = None
    with pytest.raises(PHXConnectionError):
        await harness.run_forever()

    signaled = _SupervisorHarness()
    signaled._supervisor_task = asyncio.create_task(asyncio.sleep(10))

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_args: None)
    monkeypatch.setattr(loop, "remove_signal_handler", lambda *_args: None)

    async def wait_signal_first(
        tasks: Any, return_when: Any
    ) -> tuple[set[Any], set[Any]]:
        del return_when
        return {tasks[0]}, {tasks[1]}

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.asyncio.wait", wait_signal_first
    )
    await signaled.run_forever()
    assert signaled.shutdown_reasons == ["Signal received"]

    fallback = _SupervisorHarness()
    fallback._supervisor_task = asyncio.create_task(asyncio.sleep(10))

    monkeypatch.setattr(
        loop,
        "add_signal_handler",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("not-main-thread")),
    )
    monkeypatch.setattr(loop, "remove_signal_handler", lambda *_args: None)
    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.asyncio.wait", wait_signal_first
    )
    await fallback.run_forever()
    assert fallback.shutdown_reasons == ["Signal received"]

    errored = _SupervisorHarness()
    errored._supervisor_task = asyncio.create_task(asyncio.sleep(0))
    errored._terminal_error = PHXConnectionError("terminal")

    async def wait_supervisor_first(
        tasks: Any, return_when: Any
    ) -> tuple[set[Any], set[Any]]:
        del return_when
        return {tasks[1]}, {tasks[0]}

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.asyncio.wait", wait_supervisor_first
    )
    with pytest.raises(PHXConnectionError):
        await errored.run_forever()


@pytest.mark.asyncio
async def test_supervisor_on_disconnect_callback_fires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()
    harness.auto_reconnect = False

    disconnect_errors: list[Exception | None] = []

    async def on_disconnect(error: Exception | None) -> None:
        disconnect_errors.append(error)

    harness._on_disconnect = on_disconnect

    routing_error = RuntimeError("routing boom")
    harness._protocol_handler = _FakeRoutingProtocolHandler(error=routing_error)

    socket = _FakeSocket(close_code=1000, close_reason="normal")

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect",
        lambda _: asyncio.sleep(0, result=cast(ClientConnection, socket)),
    )
    await harness._supervisor_loop()

    assert len(disconnect_errors) == 1
    assert disconnect_errors[0] is routing_error


@pytest.mark.asyncio
async def test_supervisor_on_reconnect_callback_fires_on_generation_gt_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()

    reconnect_count = 0
    connect_count = 0

    async def on_reconnect() -> None:
        nonlocal reconnect_count
        reconnect_count += 1

    harness._on_reconnect = on_reconnect

    async def connect_and_shutdown(_: str) -> ClientConnection:
        nonlocal connect_count
        connect_count += 1
        if connect_count >= 2:
            harness._shutdown_event.set()
        return cast(ClientConnection, _FakeSocket())

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect", connect_and_shutdown
    )
    harness._wait_for_shutdown_or_timeout = lambda _: asyncio.sleep(0)  # type: ignore[method-assign,assignment]

    await harness._supervisor_loop()

    assert reconnect_count == 1


@pytest.mark.asyncio
async def test_supervisor_callback_exception_does_not_crash_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _SupervisorHarness()
    harness.auto_reconnect = False

    async def bad_disconnect(_: Exception | None) -> None:
        raise ValueError("callback boom")

    async def bad_reconnect() -> None:
        raise ValueError("callback boom")

    harness._on_disconnect = bad_disconnect
    harness._on_reconnect = bad_reconnect

    harness._protocol_handler = _FakeRoutingProtocolHandler(
        error=RuntimeError("routing")
    )

    monkeypatch.setattr(
        "phoenix_channels_python_client.supervisor.connect",
        lambda _: asyncio.sleep(0, result=cast(ClientConnection, _FakeSocket())),
    )

    await harness._supervisor_loop()
    assert harness.connection is None
