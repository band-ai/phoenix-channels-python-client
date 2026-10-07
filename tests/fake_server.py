from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from enum import StrEnum
from typing import NamedTuple
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.frames import CloseCode
from websockets.http11 import Request

from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)

# One IPv4 socket on a port the OS picks, so test runs never collide on a port.
LOOPBACK_HOST = "127.0.0.1"
ANY_FREE_PORT = 0

# The socket-level topic heartbeats travel on.
SYSTEM_TOPIC = "phoenix"


# Wire names are spelled out here, not taken from the client, so the server
# checks the client's protocol independently.
class WireEvent(StrEnum):
    HEARTBEAT = "heartbeat"
    JOIN = "phx_join"
    LEAVE = "phx_leave"
    REPLY = "phx_reply"
    CLOSE = "phx_close"


class ReplyStatus(StrEnum):
    OK = "ok"
    ERROR = "error"


class Frame(NamedTuple):
    join_ref: str | None
    ref: str | None
    topic: str
    event: str
    payload: Mapping[str, object]


class FakePhoenixServer:
    SOCKET_PATH = "/socket/websocket"
    TOPIC = "test-topic"
    OTHER_TOPIC = "test-topic-b"
    REJECTED_TOPIC = "invalid-topic"
    VALID_TOPICS = frozenset({TOPIC, OTHER_TOPIC})

    def __init__(
        self,
        protocol: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
    ):
        self.protocol = protocol
        self.host = LOOPBACK_HOST
        self.port = ANY_FREE_PORT

        self.server: Server | None = None
        self._clients: set[ServerConnection] = set()
        self._client_ids: dict[ServerConnection, int] = {}
        self._client_api_key: dict[ServerConnection, str] = {}
        self._next_client_id = 1
        self.close_on_join_ids: set[int] = set()
        self.close_on_join_code: int = CloseCode.SERVICE_RESTART
        self.fail_join_targets: set[tuple[int, str]] = set()
        # Joins from these clients get no reply, so the client's join times out.
        self.unanswered_join_ids: set[int] = set()
        # Clear to leave heartbeats unanswered, like an unresponsive server.
        self.answer_heartbeats = True
        # Every topic a client asked to join, in order, answered or not.
        self.join_topics: list[str] = []
        self.enforce_single_connection_per_api_key = False
        self.connection_attempts_by_path: dict[str, int] = {}
        # The close code and reason each finished connection received, in order.
        self.closes: list[tuple[int | None, str | None]] = []
        # Clear the gate to hold new opening handshakes until it is set again.
        self.handshake_gate = asyncio.Event()
        self.handshake_gate.set()
        self.handshake_pending = asyncio.Event()

    def _encode(self, frame: Frame) -> str:
        match self.protocol:
            case PhoenixChannelsProtocolVersion.V1:
                return json.dumps(
                    {
                        "topic": frame.topic,
                        "event": frame.event,
                        "ref": frame.ref,
                        "payload": dict(frame.payload),
                    }
                )
            case PhoenixChannelsProtocolVersion.V2:
                return json.dumps(
                    [
                        frame.join_ref,
                        frame.ref,
                        frame.topic,
                        frame.event,
                        dict(frame.payload),
                    ]
                )

    def _decode(self, raw: str | bytes) -> Frame | None:
        data = json.loads(raw)
        match self.protocol, data:
            case PhoenixChannelsProtocolVersion.V1, {"topic": str(topic)}:
                return Frame(
                    data.get("join_ref"),
                    data.get("ref"),
                    topic,
                    data.get("event"),
                    data.get("payload") or {},
                )
            case PhoenixChannelsProtocolVersion.V2, [
                join_ref,
                ref,
                str(topic),
                event,
                payload,
            ]:
                return Frame(join_ref, ref, topic, event, payload or {})
            case _:
                return None

    @staticmethod
    def _extract_request_path(websocket: ServerConnection) -> str:
        request = getattr(websocket, "request", None)
        if request is None:
            return ""
        return str(getattr(request, "path", ""))

    @staticmethod
    def _extract_api_key(request_path: str) -> str:
        parsed = urlparse(request_path)
        query = parse_qs(parsed.query)
        values = query.get("api_key")
        if not values:
            return ""
        return values[0]

    async def _hold_handshake(
        self, connection: ServerConnection, request: Request
    ) -> None:
        if not self.handshake_gate.is_set():
            self.handshake_pending.set()
            await self.handshake_gate.wait()
            self.handshake_pending.clear()

    async def handler(self, websocket: ServerConnection) -> None:
        client_id = self._next_client_id
        self._next_client_id += 1
        request_path = self._extract_request_path(websocket)
        path_only = urlparse(request_path).path
        api_key = self._extract_api_key(request_path)

        self._clients.add(websocket)
        self._client_ids[websocket] = client_id
        self._client_api_key[websocket] = api_key
        self.connection_attempts_by_path[path_only] = (
            self.connection_attempts_by_path.get(path_only, 0) + 1
        )

        if self.enforce_single_connection_per_api_key and api_key:
            for existing in list(self._clients):
                if existing is websocket:
                    continue
                if self._client_api_key.get(existing) != api_key:
                    continue
                await existing.close(
                    code=CloseCode.TRY_AGAIN_LATER, reason="duplicate session"
                )

        try:
            async for message in websocket:
                frame = self._decode(message)
                if frame is not None:
                    await self.handle_frame(websocket, frame)
        except Exception:
            pass
        finally:
            self._clients.discard(websocket)
            self._client_ids.pop(websocket, None)
            self._client_api_key.pop(websocket, None)
            self.closes.append((websocket.close_code, websocket.close_reason))

    async def _send(self, websocket: ServerConnection, frame: Frame) -> None:
        await websocket.send(self._encode(frame))

    async def _reply(
        self,
        websocket: ServerConnection,
        request: Frame,
        status: ReplyStatus,
        reason: str = "",
    ) -> None:
        response = {"reason": reason} if reason else {}
        payload = {"status": status, "response": response}
        reply = Frame(
            request.join_ref, request.ref, request.topic, WireEvent.REPLY, payload
        )
        await self._send(websocket, reply)

    async def handle_frame(self, websocket: ServerConnection, frame: Frame) -> None:
        match frame.event:
            case WireEvent.HEARTBEAT if (
                frame.topic == SYSTEM_TOPIC and self.answer_heartbeats
            ):
                await self._reply(websocket, frame, ReplyStatus.OK)
            case WireEvent.JOIN:
                await self._handle_join(websocket, frame)
            case WireEvent.LEAVE:
                await self._reply(websocket, frame, ReplyStatus.OK)
                await self._send(
                    websocket,
                    Frame(
                        frame.join_ref, frame.join_ref, frame.topic, WireEvent.CLOSE, {}
                    ),
                )

    async def _handle_join(self, websocket: ServerConnection, frame: Frame) -> None:
        client_id = self._client_ids.get(websocket)
        self.join_topics.append(frame.topic)
        if client_id in self.unanswered_join_ids:
            return

        if (client_id, frame.topic) in self.fail_join_targets:
            await self._reply(
                websocket, frame, ReplyStatus.ERROR, "forced join failure"
            )
            return

        if frame.topic in self.VALID_TOPICS:
            await self._reply(websocket, frame, ReplyStatus.OK)
        else:
            await self._reply(websocket, frame, ReplyStatus.ERROR, "unmatched topic")

        if client_id in self.close_on_join_ids:
            await websocket.close(
                code=self.close_on_join_code, reason="forced close on join"
            )

    async def simulate_server_event(
        self,
        topic: str,
        event: str,
        payload: Mapping[str, object],
        join_ref: str | None = None,
    ) -> None:
        """Push one event to every connected client."""
        frame = Frame(join_ref, None, topic, event, payload)
        for websocket in list(self._clients):
            await self._send(websocket, frame)

    async def send_raw(self, text: str) -> None:
        """Send ``text`` as-is to every client, valid frame or not."""
        for websocket in list(self._clients):
            await websocket.send(text)

    @contextmanager
    def unresponsive(self) -> Iterator[None]:
        """Stop reading from every client until exit, like a peer that hangs."""
        transports = [websocket.transport for websocket in self._clients]
        for transport in transports:
            transport.pause_reading()
        try:
            yield
        finally:
            for transport in transports:
                if not transport.is_closing():
                    transport.resume_reading()

    async def close_all_clients(
        self,
        *,
        code: int = CloseCode.SERVICE_RESTART,
        reason: str = "service restart",
    ) -> None:
        for websocket in list(self._clients):
            await websocket.close(code=code, reason=reason)

    @property
    def next_client_id(self) -> int:
        """The id the next connection will get, to target it before it exists."""
        return self._next_client_id

    def current_client_ids(self) -> set[int]:
        return set(self._client_ids.values())

    def get_connection_attempts(self, path: str) -> int:
        return self.connection_attempts_by_path.get(path, 0)

    def list_client_connections(self) -> list[ServerConnection]:
        return list(self._clients)

    def url_for(self, path: str) -> str:
        return f"ws://{self.host}:{self.port}{path}"

    async def start(self) -> None:
        self.server = await serve(
            self.handler, self.host, self.port, process_request=self._hold_handshake
        )
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        # Closing waits for every handshake, including ones held at the gate.
        self.handshake_gate.set()
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def __aenter__(self) -> FakePhoenixServer:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()
