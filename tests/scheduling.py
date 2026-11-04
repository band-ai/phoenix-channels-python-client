"""Native event-loop observation and one-task wakeup gates."""

from __future__ import annotations

import asyncio
import inspect
import sys
from collections.abc import Callable, Coroutine, Generator, Iterator
from contextlib import contextmanager
from contextvars import Context, copy_context
from dataclasses import dataclass, field
from types import CodeType
from typing import Any, TypeAlias
from unittest.mock import patch

# asyncio's factory stubs dropped generator coroutines in Python 3.12.
if sys.version_info >= (3, 12):
    _TaskWork: TypeAlias = Coroutine[Any, Any, Any]
else:
    _TaskWork: TypeAlias = Coroutine[Any, Any, Any] | Generator[Any, None, Any]


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
        self.task: asyncio.Future[None] | None = None
        self.used = False

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()

        def factory(
            loop: asyncio.AbstractEventLoop,
            work: _TaskWork,
            /,
            **options: Any,
        ) -> asyncio.Future[Any]:
            original = (
                work
                if (
                    not self.used
                    and inspect.iscoroutine(work)
                    and work.cr_code is self.code
                )
                else None
            )
            wrapped = self._run(original) if original is not None else None
            if wrapped is not None:
                self.used = True

            def close_unstarted(_future: asyncio.Future[Any] | None = None) -> None:
                for coroutine in (wrapped, original):
                    if (
                        coroutine is not None
                        and inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
                    ):
                        coroutine.close()

            scheduled = wrapped if wrapped is not None else work
            try:
                task = (
                    previous(loop, scheduled, **options)
                    if previous is not None
                    else asyncio.Task(scheduled, loop=loop, **options)
                )
            except BaseException:
                close_unstarted()
                raise
            if wrapped is not None:
                self.task = task
                if task.done():
                    close_unstarted()
                else:
                    task.add_done_callback(close_unstarted)
            return task

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


@dataclass
class TimerCallback:
    deadline: float
    callback: Callable[..., object]
    args: tuple[object, ...]
    context: Context
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    released: bool = False

    def release(self) -> None:
        if self.ready.is_set() and not self.released:
            self.released = True
            self.context.run(self.callback, *self.args)


class TimerCallbackGate:
    """Delay dispatched native timers to inject stale callbacks or choose tie order."""

    def __init__(self, code: CodeType, *, count: int = 1) -> None:
        self.code = code
        self.count = count
        self.registered: asyncio.Queue[TimerCallback] = asyncio.Queue()
        self.timers: list[TimerCallback] = []
        self.holding = False

    @contextmanager
    def installed(self) -> Iterator[None]:
        loop = asyncio.get_running_loop()
        schedule = loop.call_at
        self.holding = True

        def call_at(
            when: float,
            callback: Callable[..., object],
            *args: object,
            context: Context | None = None,
        ) -> asyncio.TimerHandle:
            function = getattr(callback, "__func__", callback)
            if (
                len(self.timers) < self.count
                and getattr(function, "__code__", None) is self.code
            ):
                timer = TimerCallback(
                    when,
                    callback,
                    args,
                    context if context is not None else copy_context(),
                )
                self.timers.append(timer)
                self.registered.put_nowait(timer)
                return schedule(when, self._hold, timer)
            return schedule(when, callback, *args, context=context)

        try:
            with patch.object(loop, "call_at", call_at):
                yield
        finally:
            self.holding = False
            for timer in self.timers:
                timer.release()

    def _hold(self, timer: TimerCallback) -> None:
        timer.ready.set()
        if not self.holding:
            timer.release()
