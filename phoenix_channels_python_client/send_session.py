from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.exceptions import SocketError

T = TypeVar("T")


@dataclass(eq=False)
class SendOperation:
    outcome: asyncio.Future[Any]
    timeout_error: Callable[[], Exception]
    task: asyncio.Task[None] | None = None
    timer: asyncio.TimerHandle | None = None
    submitted: bool = False


def _fail(outcome: asyncio.Future[Any], error: BaseException) -> None:
    if not outcome.done():
        outcome.set_exception(error)


class SessionSends:
    """Own channel sends after their caller's result or cancellation."""

    def __init__(
        self,
        connection: ClientConnection,
        generation: int,
        *,
        is_current: Callable[[], bool],
        on_failure: Callable[[Exception], None],
    ) -> None:
        self.connection = connection
        self.generation = generation
        self.is_current = is_current
        self.on_failure = on_failure
        self.operations: dict[asyncio.Future[Any], SendOperation] = {}
        self.retired = False
        self.failed = False

    async def send_and_wait(  # noqa: PLR0913 -- one admitted operation's policies
        self,
        send: Callable[[], Awaitable[None]],
        outcome: asyncio.Future[T],
        *,
        timeout_s: float,
        timeout_error: Callable[[], Exception],
        eligible: Callable[[], bool],
        ineligible_error: Callable[[], Exception],
    ) -> T:
        if not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("timeout_s must be finite and > 0")
        if outcome in self.operations:
            raise ValueError("The operation already has a send")
        if outcome.done():
            return await asyncio.shield(outcome)

        operation = SendOperation(outcome, timeout_error)
        self.operations[outcome] = operation
        task = operation.task = asyncio.create_task(
            self._send(operation, send, eligible, ineligible_error)
        )
        task.add_done_callback(lambda completed: self._send_done(operation, completed))
        outcome.add_done_callback(lambda _: self._outcome_done(operation))
        operation.timer = asyncio.get_running_loop().call_later(
            timeout_s, self._expire, operation
        )

        try:
            return await asyncio.shield(outcome)
        except asyncio.CancelledError:
            if not outcome.done():
                outcome.cancel()
            if not operation.submitted:
                task.cancel()
            raise

    async def _send(
        self,
        operation: SendOperation,
        send: Callable[[], Awaitable[None]],
        eligible: Callable[[], bool],
        ineligible_error: Callable[[], Exception],
    ) -> None:
        try:
            if operation.outcome.done():
                return
            if self.retired or self.failed or not self.is_current() or not eligible():
                _fail(operation.outcome, ineligible_error())
                return
            operation.submitted = True
            await send()
        except (ConnectionClosed, SocketError) as exc:
            error = SocketError("Connection lost while sending a channel operation")
            error.__cause__ = exc
            _fail(operation.outcome, error)
            self._notify_failure(error)
        except asyncio.CancelledError:
            error = SocketError("Channel send was cancelled")
            _fail(operation.outcome, error)
            if operation.submitted:
                self._notify_failure(error)
            raise
        except Exception as exc:  # noqa: BLE001 -- settle this operation's waiter
            _fail(operation.outcome, exc)

    def _notify_failure(self, error: Exception) -> None:
        if self.retired or self.failed or not self.is_current():
            return
        self.failed = True
        self.on_failure(error)

    def _expire(self, operation: SendOperation) -> None:
        if self.retired or not self.is_current():
            return
        if not operation.outcome.done():
            _fail(operation.outcome, operation.timeout_error())
        if operation.task is None or operation.task.done():
            self._discard(operation)
            return
        if not operation.submitted:
            operation.task.cancel()
            return
        self._notify_failure(SocketError("Channel operation write deadline expired"))

    def _send_done(self, operation: SendOperation, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            _fail(operation.outcome, error)
        if operation.outcome.done():
            self._discard(operation)

    def _outcome_done(self, operation: SendOperation) -> None:
        if not operation.outcome.cancelled():
            operation.outcome.exception()
        if operation.task is not None and operation.task.done():
            self._discard(operation)

    def _discard(self, operation: SendOperation) -> None:
        if operation.timer is not None:
            operation.timer.cancel()
        if self.operations.get(operation.outcome) is operation:
            self.operations.pop(operation.outcome)

    def has_pending(self, outcome: asyncio.Future[Any]) -> bool:
        operation = self.operations.get(outcome)
        return (
            operation is not None
            and operation.task is not None
            and not operation.task.done()
        )

    async def wait_for_send(self, outcome: asyncio.Future[Any]) -> None:
        operation = self.operations.get(outcome)
        if operation is not None and operation.task is not None:
            await asyncio.wait({operation.task})

    async def finish(self) -> None:
        """Retrieve sends after the owner has closed their captured connection."""
        self.retired = True
        operations = tuple(self.operations.values())
        tasks: list[asyncio.Task[None]] = []
        for operation in operations:
            if operation.timer is not None:
                operation.timer.cancel()
            if operation.task is not None:
                if not operation.submitted:
                    operation.task.cancel()
                tasks.append(operation.task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.operations.clear()
