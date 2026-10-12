"""Supervisor branches the fake server can't reach, driven through a harness."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest
from websockets import ClientConnection

from phoenix_channels_python_client.client_types import ReconnectDecision
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.phx_messages import (
    PHOENIX_TOPIC,
    ChannelMessage,
    PHXEvent,
)
from phoenix_channels_python_client.utils import make_message
from tests.harness import (
    HARNESS_RECONNECT_DELAY_S,
    HARNESS_STABLE_RESET_S,
    FakeSocket,
    SupervisorHarness,
    connect_to,
    fake_connection,
)
from tests.support import wait_forever

HEARTBEAT_REF = "5"

# Past the harness's stable reset, with margin for the loop clock's resolution.
STABLE_UPTIME_S = 2 * HARNESS_STABLE_RESET_S

Connect = Callable[[str], Awaitable[ClientConnection]]


@pytest.fixture
def use_connect(monkeypatch: pytest.MonkeyPatch) -> Callable[[Connect], None]:
    """Swap the supervisor's ``websockets.connect`` for a stand-in."""

    def use(connect: Connect) -> None:
        monkeypatch.setattr(
            "phoenix_channels_python_client.supervisor.connect", connect
        )

    return use


async def fail_to_connect(_: str) -> ClientConnection:
    raise RuntimeError("connect fail")


def heartbeat_reply(ref: str) -> ChannelMessage:
    return make_message(topic=PHOENIX_TOPIC, event=PHXEvent.reply, payload={}, ref=ref)


async def test_a_suppressed_reconnect_after_a_failed_connect_is_terminal(
    use_connect: Callable[[Connect], None],
) -> None:
    harness = SupervisorHarness()
    harness.suppress_values = [True]
    use_connect(fail_to_connect)

    await harness._supervisor_loop()

    assert isinstance(harness._terminal_error, PHXConnectionError)


async def test_the_initial_connect_retries_before_succeeding(
    use_connect: Callable[[Connect], None],
) -> None:
    harness = SupervisorHarness()
    failures = 1
    attempts = 0

    async def fail_then_connect(_: str) -> ClientConnection:
        nonlocal attempts
        attempts += 1
        if attempts <= failures:
            raise RuntimeError("transient connect fail")
        return fake_connection()

    waits = 0

    async def stop_on_the_second_wait(delay_s: float) -> None:
        nonlocal waits
        del delay_s
        waits += 1
        if waits > failures:
            harness._shutdown_event.set()

    harness._wait_for_shutdown_or_timeout = stop_on_the_second_wait  # type: ignore[method-assign]
    use_connect(fail_then_connect)

    await harness._supervisor_loop()

    assert attempts > failures
    assert harness._initial_connection_future is not None
    assert harness._initial_connection_future.result() is None


async def test_a_rejoin_error_does_not_stop_the_supervisor(
    use_connect: Callable[[Connect], None],
) -> None:
    harness = SupervisorHarness()
    harness.rejoin_error = RuntimeError("rejoin boom")
    use_connect(connect_to)

    await harness._supervisor_loop()

    assert harness._initial_connection_future is not None
    assert harness._initial_connection_future.result() is None
    assert harness.wait_delays == [HARNESS_RECONNECT_DELAY_S]


async def test_a_connection_outliving_stable_reset_clears_the_rapid_history(
    use_connect: Callable[[Connect], None],
) -> None:
    harness = SupervisorHarness(connection_uptime_s=STABLE_UPTIME_S)
    harness._rapid_disconnects.extend([1.0, 2.0])
    use_connect(connect_to)

    await harness._supervisor_loop()

    assert not harness._rapid_disconnects


async def test_a_forced_close_reconnects_without_classifying_the_disconnect(
    use_connect: Callable[[Connect], None],
) -> None:
    harness = SupervisorHarness(forced_close_pending=True)
    # What the real _classify_disconnect would decide for whatever code the
    # remote happened to echo back; the forced-close path must not consult it.
    harness.disconnect_decision = ReconnectDecision(should_reconnect=False)
    use_connect(connect_to)

    await harness._supervisor_loop()

    assert harness.wait_delays == [HARNESS_RECONNECT_DELAY_S]
    assert harness._forced_close_pending is False


async def test_shutdown_before_connecting_fails_the_initial_connection() -> None:
    harness = SupervisorHarness()
    harness._shutdown_event.set()

    await harness._supervisor_loop()

    assert harness._initial_connection_future is not None
    assert isinstance(
        harness._initial_connection_future.exception(), PHXConnectionError
    )


async def test_cleanup_survives_a_socket_that_fails_to_close() -> None:
    harness = SupervisorHarness()
    harness.connection = fake_connection(FakeSocket(close_raises=True))
    harness._message_routing_task = asyncio.create_task(wait_forever())

    await harness._cleanup_connection()

    assert harness.connection is None
    assert harness._message_routing_task is None


@pytest.mark.parametrize("install_signal_handlers", [True, False])
async def test_run_forever_raises_the_supervisors_failure(
    *, install_signal_handlers: bool
) -> None:
    failure = RuntimeError()

    async def fail() -> None:
        raise failure

    harness = SupervisorHarness()
    harness._supervisor_task = asyncio.create_task(fail())

    with pytest.raises(RuntimeError) as raised:
        await harness.run_forever(install_signal_handlers=install_signal_handlers)
    assert raised.value is failure


async def test_a_heartbeat_reply_fires_the_ack_callback() -> None:
    harness = SupervisorHarness(pending_heartbeat_ref=HEARTBEAT_REF)
    acks: list[None] = []
    harness._on_heartbeat_ack = lambda: acks.append(None)

    harness._handle_heartbeat_response(heartbeat_reply(HEARTBEAT_REF))

    assert len(acks) == 1
    assert harness._pending_heartbeat_ref is None


async def test_a_reply_to_another_heartbeat_is_ignored() -> None:
    harness = SupervisorHarness(pending_heartbeat_ref=HEARTBEAT_REF)
    acks: list[None] = []
    harness._on_heartbeat_ack = lambda: acks.append(None)

    harness._handle_heartbeat_response(heartbeat_reply(f"not-{HEARTBEAT_REF}"))

    assert acks == []
    assert harness._pending_heartbeat_ref == HEARTBEAT_REF


async def test_a_raising_ack_callback_still_clears_the_heartbeat() -> None:
    harness = SupervisorHarness(pending_heartbeat_ref=HEARTBEAT_REF)

    def bad_heartbeat_ack() -> None:
        raise ValueError("callback boom")

    harness._on_heartbeat_ack = bad_heartbeat_ack

    harness._handle_heartbeat_response(heartbeat_reply(HEARTBEAT_REF))

    assert harness._pending_heartbeat_ref is None


async def test_an_async_ack_callback_is_not_run() -> None:
    harness = SupervisorHarness(pending_heartbeat_ref=HEARTBEAT_REF)
    ran: list[bool] = []

    async def async_on_heartbeat_ack() -> None:
        ran.append(True)

    harness._on_heartbeat_ack = async_on_heartbeat_ack  # type: ignore[assignment]

    harness._handle_heartbeat_response(heartbeat_reply(HEARTBEAT_REF))

    assert harness._pending_heartbeat_ref is None
    assert ran == []
