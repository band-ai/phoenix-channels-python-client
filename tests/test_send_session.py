"""Exercise send/result ownership with a gated transport coroutine."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import cast

import pytest
import pytest_asyncio
from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.exceptions import ChannelError, SocketError
from phoenix_channels_python_client.send_session import SessionSends
from tests.clock import Clock
from tests.scheduling import TaskLifecycleGate

OPERATION_TIMEOUT_S = 10.0


@dataclass
class SendProbe:
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    failures: list[Exception] = field(default_factory=list)
    failed: asyncio.Event = field(default_factory=asyncio.Event)
    current: bool = True
    error: BaseException | None = None
    outcome: asyncio.Future[str] = field(default_factory=asyncio.Future)
    tasks: list[asyncio.Task[str]] = field(default_factory=list)
    session: SessionSends = field(init=False)

    def __post_init__(self) -> None:
        # This unit owns I/O lifetime; the session never calls the socket itself.
        self.session = SessionSends(
            cast(ClientConnection, object()),
            1,
            is_current=lambda: self.current,
            on_failure=self.fail_session,
        )

    def fail_session(self, error: Exception) -> None:
        self.failures.append(error)
        self.failed.set()

    async def send(self) -> None:
        self.started.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error

    def start(self, *, eligible: bool = True) -> asyncio.Task[str]:
        task = asyncio.create_task(
            self.session.send_and_wait(
                self.send,
                self.outcome,
                timeout_s=OPERATION_TIMEOUT_S,
                timeout_error=TimeoutError,
                eligible=lambda: eligible,
                ineligible_error=lambda: ChannelError("Not eligible"),
            )
        )
        self.tasks.append(task)
        return task


@pytest_asyncio.fixture
async def probe() -> AsyncIterator[SendProbe]:
    probe = SendProbe()
    try:
        yield probe
    finally:
        probe.release.set()
        if not probe.outcome.done():
            probe.outcome.cancel()
        await asyncio.gather(*probe.tasks, return_exceptions=True)
        await probe.session.finish()


async def test_a_reply_wakes_the_caller_before_send_finishes(probe: SendProbe) -> None:
    caller = probe.start()
    await probe.started.wait()

    probe.outcome.set_result("reply")

    assert await caller == "reply"
    assert probe.session.has_pending(probe.outcome)
    probe.release.set()
    await probe.session.wait_for_send(probe.outcome)
    assert not probe.session.has_pending(probe.outcome)
    assert probe.failures == []


@pytest.mark.clock
async def test_no_reply_after_a_completed_send_does_not_fail_the_session(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()
    probe.release.set()
    await probe.session.wait_for_send(probe.outcome)

    await clock.advance(OPERATION_TIMEOUT_S)

    with pytest.raises(TimeoutError):
        await caller
    assert probe.failures == []


@pytest.mark.clock
async def test_a_stalled_send_times_out_its_operation_and_fails_the_session(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()

    await clock.advance(OPERATION_TIMEOUT_S)

    with pytest.raises(TimeoutError):
        await caller
    assert [type(error) for error in probe.failures] == [SocketError]
    assert probe.session.has_pending(probe.outcome)


@pytest.mark.clock
async def test_a_stalled_send_cannot_overwrite_an_earlier_reply(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()
    probe.outcome.set_result("reply")
    assert await caller == "reply"

    await clock.advance(OPERATION_TIMEOUT_S)
    await probe.failed.wait()

    assert probe.outcome.result() == "reply"
    assert [type(error) for error in probe.failures] == [SocketError]


async def test_cancellation_before_submission_emits_nothing(probe: SendProbe) -> None:
    gate = TaskLifecycleGate(SessionSends._send.__code__, hold_start=True)
    with gate.installed():
        caller = probe.start()
        await gate.started.wait()

        caller.cancel()

        with pytest.raises(asyncio.CancelledError):
            await caller
        await probe.session.wait_for_send(probe.outcome)
        assert not probe.started.is_set()
        assert probe.outcome.cancelled()
        assert probe.failures == []


@pytest.mark.clock
async def test_cancellation_after_submission_keeps_a_bounded_send_owner(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()

    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    assert probe.outcome.cancelled()
    assert probe.session.has_pending(probe.outcome)
    await clock.advance(OPERATION_TIMEOUT_S)
    await probe.failed.wait()
    assert [type(error) for error in probe.failures] == [SocketError]


@pytest.mark.clock
async def test_transport_loss_after_a_reply_still_fails_its_session(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()
    probe.outcome.set_result("reply")
    assert await caller == "reply"

    probe.error = SocketError("Transport closed")
    probe.release.set()
    await probe.session.wait_for_send(probe.outcome)
    await clock.advance(OPERATION_TIMEOUT_S)
    await probe.failed.wait()

    assert probe.outcome.result() == "reply"
    assert [type(error) for error in probe.failures] == [SocketError]
    assert isinstance(probe.failures[0].__cause__, SocketError)


async def test_a_websocket_close_fails_the_waiter_and_its_session(
    probe: SendProbe,
) -> None:
    probe.error = ConnectionClosed(None, None)
    probe.release.set()

    with pytest.raises(SocketError):
        await probe.start()

    assert [type(error) for error in probe.failures] == [SocketError]
    assert probe.failures[0].__cause__ is probe.error


@pytest.mark.clock
async def test_timeout_before_submission_never_fails_the_session(
    probe: SendProbe, clock: Clock
) -> None:
    gate = TaskLifecycleGate(SessionSends._send.__code__, hold_start=True)
    with gate.installed():
        caller = probe.start()
        await gate.started.wait()

        await clock.advance(OPERATION_TIMEOUT_S)

        with pytest.raises(TimeoutError):
            await caller
        await probe.session.wait_for_send(probe.outcome)
        assert not probe.started.is_set()
        assert probe.failures == []


async def test_a_reply_before_submission_prevents_an_obsolete_send(
    probe: SendProbe,
) -> None:
    gate = TaskLifecycleGate(SessionSends._send.__code__, hold_start=True)
    with gate.installed():
        caller = probe.start()
        await gate.started.wait()
        probe.outcome.set_result("already settled")

        assert await caller == "already settled"
        gate.start.set()
        await probe.session.wait_for_send(probe.outcome)

        assert not probe.started.is_set()
        assert probe.failures == []


async def test_unexpected_sender_cancellation_fails_the_operation(
    probe: SendProbe,
) -> None:
    probe.error = asyncio.CancelledError()
    probe.release.set()

    with pytest.raises(SocketError, match="send was cancelled"):
        await probe.start()

    assert [type(error) for error in probe.failures] == [SocketError]


@pytest.mark.clock
async def test_a_retired_connection_timer_cannot_fail_a_new_session(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()
    probe.current = False
    probe.outcome.set_exception(SocketError("Old connection stopped"))
    with pytest.raises(SocketError, match="Old connection stopped"):
        await caller

    await clock.advance(OPERATION_TIMEOUT_S)
    probe.error = SocketError("Old sender stopped")
    probe.release.set()
    await probe.session.wait_for_send(probe.outcome)

    assert probe.failures == []


async def test_ineligible_operations_never_submit(probe: SendProbe) -> None:
    with pytest.raises(ChannelError, match="Not eligible"):
        await probe.start(eligible=False)

    assert not probe.started.is_set()
    assert probe.failures == []


async def test_non_transport_send_failure_stays_operation_local(
    probe: SendProbe,
) -> None:
    probe.error = ValueError("Invalid outgoing operation")
    probe.release.set()

    with pytest.raises(ValueError, match="Invalid outgoing operation"):
        await probe.start()

    assert probe.failures == []


async def test_an_already_settled_operation_cannot_submit(probe: SendProbe) -> None:
    probe.outcome.set_result("already done")

    assert await probe.start() == "already done"
    assert not probe.started.is_set()


@pytest.mark.clock
async def test_finishing_after_socket_closure_cancels_the_old_watchdog(
    probe: SendProbe, clock: Clock
) -> None:
    caller = probe.start()
    await probe.started.wait()
    probe.outcome.set_exception(SocketError("Session closed"))
    with pytest.raises(SocketError, match="Session closed"):
        await caller
    probe.release.set()

    await probe.session.finish()
    await clock.advance(OPERATION_TIMEOUT_S)

    assert not probe.session.has_pending(probe.outcome)
    assert probe.failures == []


@pytest.mark.parametrize("timeout_s", [0.0, -1.0, float("inf"), float("nan")])
async def test_an_invalid_deadline_does_not_submit(
    probe: SendProbe, timeout_s: float
) -> None:
    with pytest.raises(ValueError, match="finite and > 0"):
        await probe.session.send_and_wait(
            probe.send,
            probe.outcome,
            timeout_s=timeout_s,
            timeout_error=TimeoutError,
            eligible=lambda: True,
            ineligible_error=ChannelError,
        )

    assert not probe.started.is_set()
