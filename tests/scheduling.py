"""Native event-loop observation and one-task wakeup gates."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import Context, copy_context
from types import CodeType
from typing import Any
from unittest.mock import patch


class TaskWakeupGate:
    """Hold one native wakeup so initial caller cleanup cannot win scheduling.

    Initial admission has no application callback boundary. Retaining its native
    callback exposes registered-but-never-established recovery without replacing
    client methods or driving its coroutine.
    """

    def __init__(self, task: asyncio.Task[None]) -> None:
        self.task = task
        self.held = asyncio.Event()
        self._callback: Callable[..., object] | None = None
        self._args: tuple[object, ...] = ()
        self._context: Context | None = None
        self._handle: asyncio.Handle | None = None
        self._released = False

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        schedule = loop.call_soon

        def call_soon(
            callback: Callable[..., object],
            *args: object,
            context: Context | None = None,
        ) -> asyncio.Handle:
            if (
                self._handle is None
                and getattr(callback, "__self__", None) is self.task
            ):
                self._callback = callback
                self._args = args
                self._context = context if context is not None else copy_context()
                self._handle = asyncio.Handle(
                    callback, args, loop, context=self._context
                )
                self.held.set()
                return self._handle
            return schedule(callback, *args, context=context)

        try:
            with patch.object(loop, "call_soon", call_soon):
                yield
        finally:
            self.release()

    def release(self) -> None:
        if self._released or self._handle is None:
            return
        self._released = True
        asyncio.get_running_loop().call_soon(self._invoke, context=self._context)

    def _invoke(self) -> None:
        if self._handle is not None and not self._handle.cancelled():
            assert self._callback is not None
            self._callback(*self._args)


class TaskTimerObserver:
    """Observe a real coroutine's timer registration without changing its deadline."""

    def __init__(self, code: CodeType) -> None:
        self.code = code
        self.registered: asyncio.Queue[asyncio.Task[Any]] = asyncio.Queue()

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        schedule = loop.call_later

        def call_later(
            delay: float,
            callback: Callable[..., object],
            *args: object,
            context: Context | None = None,
        ) -> asyncio.TimerHandle:
            handle = schedule(delay, callback, *args, context=context)
            task = asyncio.current_task()
            if (
                task is not None
                and getattr(task.get_coro(), "cr_code", None) is self.code
            ):
                self.registered.put_nowait(task)
            return handle

        with patch.object(loop, "call_later", call_later):
            yield
