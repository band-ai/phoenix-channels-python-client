from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from websockets.asyncio.server import ServerConnection

from phoenix_channels_python_client import Channel, ProtocolVersion, Reply
from tests.fake_server import FakePhoenixServer, Frame, WireEvent
from tests.support import EVENT

PushRequest = tuple[ServerConnection, Frame]


class PushPeer(FakePhoenixServer):
    """Record application pushes; replies are controlled independently by tests."""

    def __init__(self, protocol: ProtocolVersion) -> None:
        super().__init__(protocol)
        self.pushes: asyncio.Queue[PushRequest] = asyncio.Queue()

    async def handle_frame(self, websocket: ServerConnection, frame: Frame) -> None:
        if frame.event not in {*WireEvent}:
            self.pushes.put_nowait((websocket, frame))
            return
        await super().handle_frame(websocket, frame)

    async def answer_push(
        self,
        request: PushRequest,
        *,
        status: str = "ok",
        response: Mapping[str, object] | None = None,
    ) -> None:
        await self.reply_payload(
            request, {"status": status, "response": dict(response or {})}
        )

    async def reply_payload(
        self, request: PushRequest, payload: Mapping[str, object]
    ) -> None:
        websocket, frame = request
        await self._send(
            websocket,
            Frame(frame.join_ref, frame.ref, frame.topic, WireEvent.REPLY, payload),
        )


@asynccontextmanager
async def pending_push(
    channel: Channel, *, payload: dict[str, object] | None = None, timeout_s: float = 10
) -> AsyncIterator[asyncio.Task[Reply]]:
    task = asyncio.create_task(channel.push(EVENT, payload or {}, timeout_s=timeout_s))
    try:
        yield task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
