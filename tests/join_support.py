"""Signals and consumer state for ordered join tests."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager

from websockets import ClientConnection

from phoenix_channels_python_client import PHXChannelsClient
from phoenix_channels_python_client.phx_messages import ChannelMessage
from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin
from tests.fake_server import FakePhoenixServer, Frame, ReplyStatus, WireEvent
from tests.scheduling import TaskLifecycleGate
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
    client: PHXChannelsClient, probe: JoinProbe, *, blocks_reconnect: bool = True
) -> AsyncIterator[asyncio.Task[None]]:
    task = asyncio.create_task(
        client.subscribe_to_topic(
            probe.topic,
            probe.receive,
            on_join_response=probe.install,
            blocks_reconnect=blocks_reconnect,
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


class ProcessorGate(TaskLifecycleGate):
    """Control a processor lifetime when no application callback exists to hold."""

    def __init__(self, *, hold_start: bool = False, hold_exit: bool = False) -> None:
        super().__init__(
            TopicRuntimeMixin._process_topic_messages.__code__,
            hold_start=hold_start,
            hold_exit=hold_exit,
        )


@contextmanager
def paused_writes(connection: ClientConnection) -> Iterator[None]:
    """Model transport flow control while real receive/router tasks keep running."""
    connection.pause_writing()
    try:
        yield
    finally:
        if connection.paused:
            connection.resume_writing()


class RecoveryFailures(logging.Handler):
    """Signal a reported transient failure before advancing its retry clock."""

    def __init__(self) -> None:
        super().__init__()
        self.receipts: asyncio.Queue[logging.LogRecord] = asyncio.Queue()

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith("Rejoin attempt "):
            self.receipts.put_nowait(record)

    @contextmanager
    def attached(self, logger: logging.Logger) -> Iterator[RecoveryFailures]:
        logger.addHandler(self)
        try:
            yield self
        finally:
            logger.removeHandler(self)
