"""Signals and consumer state for ordered join tests."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Coroutine, Generator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import Context
from typing import Any

from phoenix_channels_python_client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from tests.fake_server import FakePhoenixServer, Frame, ReplyStatus, WireEvent
from tests.support import ASYNC_TIMEOUT_S, EVENT, TOPIC


class JoinProbe:
    def __init__(self, client: PHXChannelsClient, topic: str = TOPIC) -> None:
        self.client = client
        self.topic = topic
        self.state: object = None
        self.order: list[object] = []
        self.installed: asyncio.Queue[object] = asyncio.Queue()
        self.delivered: asyncio.Queue[ChannelMessage] = asyncio.Queue()

    def install(self, response: object) -> None:
        assert not self.client.is_topic_joined(self.topic)
        self.state = response
        self.order.append(response)
        self.installed.put_nowait(response)

    async def receive(self, message: ChannelMessage) -> None:
        assert self.client.is_topic_joined(self.topic)
        self.order.append(message.payload)
        self.delivered.put_nowait(message)


@asynccontextmanager
async def pending_subscribe(
    client: PHXChannelsClient, probe: JoinProbe
) -> AsyncIterator[asyncio.Task[None]]:
    task = asyncio.create_task(
        client.subscribe_to_topic(
            probe.topic, probe.receive, on_join_response=probe.install
        )
    )
    try:
        yield task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def push(
    server: FakePhoenixServer,
    request: Frame,
    event: str = EVENT,
    payload: dict[str, object] | None = None,
    *,
    ref: str | None = None,
) -> None:
    await server.simulate_server_event(
        request.topic, event, payload or {}, join_ref=request.ref, ref=ref
    )


async def reply(
    server: FakePhoenixServer, request: Frame, *, status: ReplyStatus = ReplyStatus.OK
) -> None:
    await push(server, request, WireEvent.REPLY, {"status": status}, ref=request.ref)


@asynccontextmanager
async def deadline() -> AsyncIterator[None]:
    # A hung-test guard, never evidence of ordering or independence.
    async with asyncio.timeout(ASYNC_TIMEOUT_S):
        yield


class ProcessorGate:
    """Hold one real processor's start or finalizer at the task boundary.

    Initial joins have no application callback to hold. The task factory supplies
    that missing lifecycle boundary without replacing the router or processor.
    """

    def __init__(self, *, hold_start: bool = False, hold_exit: bool = False) -> None:
        self.started = asyncio.Event()
        self.exiting = asyncio.Event()
        self.start = asyncio.Event()
        self.finish = asyncio.Event()
        if not hold_start:
            self.start.set()
        if not hold_exit:
            self.finish.set()
        self.task: asyncio.Task[None] | None = None
        self._used = False

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
                not self._used
                and inspect.iscoroutine(work)
                and work.cr_code is TopicRuntimeMixin._process_topic_messages.__code__
            ):
                self._used = True
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
