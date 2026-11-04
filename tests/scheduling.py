"""Native event-loop observation and one-task wakeup gates."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine, Generator, Iterator
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
        self.callback: Callable[..., object] | None = None
        self.args: tuple[object, ...] = ()
        self.context: Context | None = None
        self.handle: asyncio.Handle | None = None
        self.released = False

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        schedule = loop.call_soon

        def call_soon(
            callback: Callable[..., object],
            *args: object,
            context: Context | None = None,
        ) -> asyncio.Handle:
            if self.handle is None and getattr(callback, "__self__", None) is self.task:
                self.callback = callback
                self.args = args
                self.context = context if context is not None else copy_context()
                self.handle = asyncio.Handle(callback, args, loop, context=self.context)
                self.held.set()
                return self.handle
            return schedule(callback, *args, context=context)

        try:
            with patch.object(loop, "call_soon", call_soon):
                yield
        finally:
            self.release()

    def release(self) -> None:
        if self.released or self.handle is None:
            return
        self.released = True
        asyncio.get_running_loop().call_soon(self._invoke, context=self.context)

    def _invoke(self) -> None:
        if self.handle is not None and not self.handle.cancelled():
            assert self.callback is not None
            self.callback(*self.args)


class TaskTimerObserver:
    """Observe a real coroutine's timer registration without changing its deadline."""

    def __init__(self, code: CodeType) -> None:
        self.code = code
        self.registered: asyncio.Queue[asyncio.Task[Any]] = asyncio.Queue()

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        schedule = loop.call_at

        def call_at(
            when: float,
            callback: Callable[..., object],
            *args: object,
            context: Context | None = None,
        ) -> asyncio.TimerHandle:
            handle = schedule(when, callback, *args, context=context)
            task = asyncio.current_task()
            if (
                task is not None
                and getattr(task.get_coro(), "cr_code", None) is self.code
            ):
                self.registered.put_nowait(task)
            return handle

        with patch.object(loop, "call_at", call_at):
            yield


class TaskLifecycleGate:
    """Hold one real task's start or finalizer without replacing its coroutine."""

    def __init__(
        self, code: CodeType, *, hold_start: bool = False, hold_exit: bool = False
    ) -> None:
        self.code = code
        self.started = asyncio.Event()
        self.exiting = asyncio.Event()
        self.start = asyncio.Event()
        self.finish = asyncio.Event()
        if not hold_start:
            self.start.set()
        if not hold_exit:
            self.finish.set()
        self.task: asyncio.Task[None] | None = None
        self.used = False

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()

        def factory(
            loop: asyncio.AbstractEventLoop,
            work: Coroutine[Any, Any, Any] | Generator[Any, None, Any],
            /,
            *,
            context: Context | None = None,
        ) -> asyncio.Future[Any]:
            if (
                not self.used
                and inspect.iscoroutine(work)
                and work.cr_code is self.code
            ):
                self.used = True
                self.task = asyncio.Task(self._run(work), loop=loop, context=context)
                return self.task
            if previous is not None:
                if context is not None:
                    return context.run(previous, loop, work)
                return previous(loop, work)
            return asyncio.Task(work, loop=loop, context=context)

        loop.set_task_factory(factory)
        try:
            yield
        finally:
            loop.set_task_factory(previous)
            self.start.set()
            self.finish.set()

    async def _run(self, work: Coroutine[Any, Any, None]) -> None:
        self.started.set()
        try:
            await self.start.wait()
            await work
        finally:
            work.close()
            self.exiting.set()
            await self.finish.wait()
