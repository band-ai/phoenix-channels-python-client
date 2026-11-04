from __future__ import annotations

import asyncio
from enum import IntEnum
from typing import NamedTuple, cast

from websockets.asyncio.server import ServerConnection

from phoenix_channels_python_client import ProtocolVersion
from tests.fake_server import Frame
from tests.push_support import PushPeer


class BinaryKind(IntEnum):
    PUSH = 0
    REPLY = 1
    BROADCAST = 2


class BinaryFrame(NamedTuple):
    join_ref: str | None
    ref: str | None
    topic: str
    event: str
    payload: bytes


BinaryRequest = tuple[ServerConnection, BinaryFrame]


def binary_frame(kind: BinaryKind, fields: tuple[str, ...], payload: bytes) -> bytes:
    headers = tuple(field.encode("utf-8") for field in fields)
    return (
        bytes([kind, *(len(header) for header in headers)])
        + b"".join(headers)
        + payload
    )


def decode_client_push(raw: bytes) -> BinaryFrame:
    if raw[0] != BinaryKind.PUSH:
        raise ValueError("Client binary frames must be pushes")
    header_size = 5
    offset = header_size
    fields: list[str] = []
    for size in raw[1:header_size]:
        fields.append(raw[offset : offset + size].decode("utf-8"))
        offset += size
    join_ref, ref, topic, event = fields
    return BinaryFrame(join_ref or None, ref or None, topic, event, raw[offset:])


class BinaryPeer(PushPeer):
    def __init__(self) -> None:
        super().__init__(ProtocolVersion.V2)
        self.binary_pushes: asyncio.Queue[BinaryRequest] = asyncio.Queue()
        self.defer_leaves = False
        self.deferred_leaves: asyncio.Queue[tuple[ServerConnection, Frame]] = (
            asyncio.Queue()
        )

    def _decode(self, raw: str | bytes) -> Frame | None:
        if isinstance(raw, bytes):
            # The parent dispatches the six fields structurally; intercept binary
            # frames in handle_frame before its JSON-only handler sees them.
            return cast(Frame, decode_client_push(raw))
        return super()._decode(raw)

    async def handle_frame(self, websocket: ServerConnection, frame: Frame) -> None:
        if isinstance(frame, BinaryFrame):
            self.binary_pushes.put_nowait((websocket, frame))
            return
        await super().handle_frame(websocket, frame)

    async def _handle_leave(self, websocket: ServerConnection, frame: Frame) -> None:
        if self.defer_leaves:
            self.left.put_nowait((websocket, frame))
            self.deferred_leaves.put_nowait((websocket, frame))
            return
        await super()._handle_leave(websocket, frame)

    async def binary_reply(
        self,
        request: BinaryRequest | tuple[ServerConnection, Frame],
        payload: bytes,
        *,
        status: str = "ok",
    ) -> None:
        connection, frame = request
        await connection.send(
            binary_frame(
                BinaryKind.REPLY,
                (frame.join_ref or "", frame.ref or "", frame.topic, status),
                payload,
            )
        )

    async def binary_push(
        self,
        connection: ServerConnection,
        topic: str,
        event: str,
        payload: bytes,
        *,
        join_ref: str | None,
    ) -> None:
        await connection.send(
            binary_frame(BinaryKind.PUSH, (join_ref or "", topic, event), payload)
        )

    async def binary_broadcast(
        self, connection: ServerConnection, topic: str, event: str, payload: bytes
    ) -> None:
        await connection.send(
            binary_frame(BinaryKind.BROADCAST, (topic, event), payload)
        )
