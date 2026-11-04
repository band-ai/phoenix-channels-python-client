from __future__ import annotations

import asyncio
import inspect
import sys
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from contextvars import Context, ContextVar
from dataclasses import dataclass, field
from types import coroutine
from typing import Any

import pytest

from tests.join_support import deadline
from tests.scheduling import TaskLifecycleGate, _TaskWork

TASK_NAME = "observed task"


@dataclass
class RecordingFactory:
    eager: bool = False
    work: list[_TaskWork] = field(default_factory=list)
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)

    def __call__(
        self, loop: asyncio.AbstractEventLoop, work: _TaskWork, /, **options: Any
    ) -> asyncio.Task[Any]:
        self.work.append(work)
        if self.eager:
            options.setdefault("eager_start", True)
        task = asyncio.Task(work, loop=loop, **options)
        self.tasks.append(task)
        return task


@contextmanager
def installed_factory(
    factory: Callable[..., asyncio.Future[Any]] | None,
) -> Iterator[None]:
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)


@pytest.mark.parametrize("with_previous", [False, True])
@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("intercepted", [False, True])
async def test_gate_preserves_names_and_the_explicit_context(
    *, with_previous: bool, populated: bool, intercepted: bool
) -> None:
    value = ContextVar("value", default="default")
    context = Context()
    if populated:
        context.run(value.set, "initial")
    observed: list[str] = []

    async def operation() -> None:
        observed.append(value.get())
        value.set("changed")

    code = operation.__code__ if intercepted else installed_factory.__code__
    gate = TaskLifecycleGate(code)
    factory = RecordingFactory()
    async with deadline():
        with installed_factory(factory if with_previous else None), gate.installed():
            task = asyncio.create_task(operation(), context=context, name=TASK_NAME)
            await task
    assert task.get_name() == TASK_NAME
    assert observed == ["initial" if populated else "default"]
    assert context.get(value) == "changed"
    assert value.get() == "default"
    assert factory.tasks == ([task] if with_previous else [])


@pytest.mark.parametrize("with_previous", [False, True])
async def test_gate_holds_only_the_first_matching_task_and_restores_the_factory(
    *, with_previous: bool
) -> None:
    completed: asyncio.Queue[str] = asyncio.Queue()

    async def operation(label: str) -> None:
        completed.put_nowait(label)

    async def unrelated() -> None:
        completed.put_nowait("unrelated")

    gate = TaskLifecycleGate(operation.__code__, hold_start=True, hold_exit=True)
    factory = RecordingFactory() if with_previous else None
    async with deadline():
        with installed_factory(factory):
            with gate.installed():
                first = asyncio.create_task(operation("first"), name=TASK_NAME)
                second = asyncio.create_task(operation("second"))
                other = asyncio.create_task(unrelated())
                await gate.started.wait()
                await asyncio.gather(second, other)
                assert {completed.get_nowait(), completed.get_nowait()} == {
                    "second",
                    "unrelated",
                }
                assert completed.empty()
                assert not first.done()
                gate.start.set()
                await gate.exiting.wait()
                assert completed.get_nowait() == "first"
                assert not first.done()
            assert asyncio.get_running_loop().get_task_factory() is factory
            await first
    assert first.get_name() == TASK_NAME
    assert gate.task is first


async def test_gate_closes_work_cancelled_before_its_wrapper_starts() -> None:
    entered = asyncio.Event()

    async def operation() -> None:
        entered.set()

    original = operation()
    gate = TaskLifecycleGate(operation.__code__)
    factory = RecordingFactory()
    async with deadline():
        with installed_factory(factory), gate.installed():
            task = asyncio.create_task(original)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    wrapped = factory.work[0]
    assert inspect.iscoroutine(wrapped)
    assert inspect.getcoroutinestate(wrapped) == inspect.CORO_CLOSED
    assert inspect.getcoroutinestate(original) == inspect.CORO_CLOSED
    assert not entered.is_set()
    assert not gate.started.is_set()
    assert not gate.exiting.is_set()


@pytest.mark.parametrize("hold_start", [False, True])
async def test_gate_propagates_cancellation_after_the_wrapper_starts(
    *, hold_start: bool
) -> None:
    entered = asyncio.Event()
    unwound = asyncio.Event()
    release = asyncio.Event()

    async def operation() -> None:
        entered.set()
        try:
            await release.wait()
        finally:
            unwound.set()

    original = operation()
    gate = TaskLifecycleGate(operation.__code__, hold_start=hold_start)
    async with deadline():
        with gate.installed():
            task = asyncio.create_task(original)
            await (gate.started if hold_start else entered).wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    assert inspect.getcoroutinestate(original) == inspect.CORO_CLOSED
    assert gate.exiting.is_set()
    assert entered.is_set() is not hold_start
    assert unwound.is_set() is not hold_start


async def test_gate_preserves_work_failure_after_the_exit_hold() -> None:
    failure = RuntimeError("operation failed")

    async def operation() -> None:
        raise failure

    gate = TaskLifecycleGate(operation.__code__, hold_exit=True)
    async with deadline():
        with gate.installed():
            task = asyncio.create_task(operation())
            await gate.exiting.wait()
            assert not task.done()
            gate.finish.set()
            with pytest.raises(RuntimeError) as caught:
                await task
    assert caught.value is failure


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
async def test_factory_rejection_closes_both_coroutines_without_retry(
    failure_type: type[RuntimeError | asyncio.CancelledError],
) -> None:
    failure = failure_type("factory rejected work")
    created: list[asyncio.Task[None]] = []
    received: list[_TaskWork] = []

    def reject(
        loop: asyncio.AbstractEventLoop, work: _TaskWork, /, **options: Any
    ) -> asyncio.Future[Any]:
        del loop, options
        received.append(work)
        raise failure

    async def operation() -> None:
        await asyncio.Event().wait()

    original = operation()
    gate = TaskLifecycleGate(operation.__code__)
    with installed_factory(reject):
        with gate.installed(), pytest.raises(failure_type) as caught:
            created.append(asyncio.create_task(original))
        assert asyncio.get_running_loop().get_task_factory() is reject
    assert caught.value is failure
    assert len(received) == 1
    wrapped = received[0]
    assert inspect.iscoroutine(wrapped)
    assert inspect.getcoroutinestate(wrapped) == inspect.CORO_CLOSED
    assert inspect.getcoroutinestate(original) == inspect.CORO_CLOSED


async def test_terminal_factory_result_closes_both_coroutines_immediately() -> None:
    received: list[_TaskWork] = []

    async def operation() -> None:
        await asyncio.Event().wait()

    terminal_task = asyncio.create_task(operation())
    terminal_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await terminal_task

    def terminal(
        loop: asyncio.AbstractEventLoop, work: _TaskWork, /, **options: Any
    ) -> asyncio.Task[None]:
        del loop, options
        received.append(work)
        return terminal_task

    original = operation()
    gate = TaskLifecycleGate(operation.__code__)
    with installed_factory(terminal), gate.installed():
        task = asyncio.create_task(original)
        assert task.cancelled()
        wrapped = received[0]
        assert inspect.iscoroutine(wrapped)
        assert inspect.getcoroutinestate(wrapped) == inspect.CORO_CLOSED
        assert inspect.getcoroutinestate(original) == inspect.CORO_CLOSED
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not gate.started.is_set()
    assert not gate.exiting.is_set()


if sys.version_info < (3, 12):

    async def test_python_311_generator_work_reaches_the_previous_factory() -> None:
        @coroutine
        def operation() -> Generator[Any, None, str]:
            yield from asyncio.sleep(0).__await__()
            return "completed"

        work = operation()
        assert inspect.isgenerator(work)
        factory = RecordingFactory()
        gate = TaskLifecycleGate(installed_factory.__code__)
        async with deadline():
            with installed_factory(factory), gate.installed():
                task = asyncio.create_task(work)
                assert await task == "completed"
        assert factory.tasks == [task]


@pytest.mark.parametrize("with_previous", [False, True])
@pytest.mark.parametrize("eager_start", [False, True])
async def test_python_314_eager_options_preserve_both_gate_holds(
    *, with_previous: bool, eager_start: bool
) -> None:
    if sys.version_info < (3, 14):
        pytest.skip("Task creation forwards eager_start on Python 3.14")
    entered = asyncio.Event()

    async def operation() -> None:
        entered.set()

    factory = RecordingFactory(eager=True) if with_previous else None
    gate = TaskLifecycleGate(operation.__code__, hold_start=True, hold_exit=True)
    options: dict[str, Any] = {"eager_start": eager_start}
    async with deadline():
        with installed_factory(factory), gate.installed():
            task = asyncio.create_task(operation(), **options)
            assert gate.started.is_set() is eager_start
            assert not entered.is_set()
            await gate.started.wait()
            gate.start.set()
            await gate.exiting.wait()
            assert entered.is_set()
            assert not task.done()
        await task


async def test_python_314_preserves_the_previous_factory_eager_default() -> None:
    if sys.version_info < (3, 14):
        pytest.skip("Eager factory delegation is checked on Python 3.14")
    entered = asyncio.Event()

    async def operation() -> None:
        entered.set()

    factory = RecordingFactory(eager=True)
    gate = TaskLifecycleGate(operation.__code__, hold_start=True)
    async with deadline():
        with installed_factory(factory), gate.installed():
            task = asyncio.create_task(operation())
            assert gate.started.is_set()
            assert not entered.is_set()
        await task
    assert entered.is_set()
    assert factory.tasks == [task]
