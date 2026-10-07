from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
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

    def __init__(
        self,
        protocol: PhoenixChannelsProtocolVersion = PhoenixChannelsProtocolVersion.V2,
        host: str = LOOPBACK_HOST,
        port: int = ANY_FREE_PORT,
    ):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.valid_topics = {self.TOPIC, self.OTHER_TOPIC}

        self.server: Server | None = None
        self.client_websocket: ServerConnection | None = None
        self._clients: set[ServerConnection] = set()
        self._client_ids: dict[ServerConnection, int] = {}
        self._client_path: dict[ServerConnection, str] = {}
        self._client_api_key: dict[ServerConnection, str] = {}
        self._next_client_id = 1
        self.close_on_join_ids: set[int] = set()
        self.close_on_join_code: int = CloseCode.SERVICE_RESTART
        self.close_on_join_reason = "forced close on join"
        self.fail_join_ids: set[int] = set()
        self.fail_join_targets: set[tuple[int, str]] = set()
        # Joins from these clients get no reply, so the client's join times out.
        self.unanswered_join_ids: set[int] = set()
        # Clear to leave heartbeats unanswered, like an unresponsive server.
        self.answer_heartbeats = True
        # Every topic a client asked to join, in order, answered or not.
        self.join_topics: list[str] = []
        self.enforce_single_connection_per_api_key = False
        self.duplicate_close_code: int = CloseCode.TRY_AGAIN_LATER
        self.duplicate_close_reason = "duplicate session"
        self.connection_attempts_by_path: dict[str, int] = {}
        # Clear the gate to hold new opening handshakes until it is set again.
        self.handshake_gate = asyncio.Event()
        self.handshake_gate.set()
        self.handshake_pending = asyncio.Event()

    def is_valid_topic(self, topic: str) -> bool:
        return topic in self.valid_topics

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

        self.client_websocket = websocket
        self._clients.add(websocket)
        self._client_ids[websocket] = client_id
        self._client_path[websocket] = path_only
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
                    code=self.duplicate_close_code,
                    reason=self.duplicate_close_reason,
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
            self._client_path.pop(websocket, None)
            self._client_api_key.pop(websocket, None)

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
            case WireEvent.HEARTBEAT if self.answer_heartbeats:
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

        if client_id in self.fail_join_ids or (
            client_id is not None and (client_id, frame.topic) in self.fail_join_targets
        ):
            await self._reply(
                websocket, frame, ReplyStatus.ERROR, "forced join failure"
            )
            return

        if self.is_valid_topic(frame.topic):
            await self._reply(websocket, frame, ReplyStatus.OK)
        else:
            await self._reply(websocket, frame, ReplyStatus.ERROR, "unmatched topic")

        if client_id in self.close_on_join_ids:
            await websocket.close(
                code=self.close_on_join_code,
                reason=self.close_on_join_reason,
            )

    async def simulate_server_event(
        self,
        topic: str,
        event: str,
        payload: Mapping[str, object],
        join_ref: str | None = None,
        client_id: int | None = None,
    ) -> None:
        """Simulate a server event being sent to the client for testing purposes."""
        targets: list[ServerConnection] = []

        if client_id is None:
            targets = list(self._clients)
        else:
            for websocket, ws_client_id in self._client_ids.items():
                if ws_client_id == client_id:
                    targets = [websocket]
                    break

        frame = Frame(join_ref, None, topic, event, payload)
        for websocket in targets:
            await self._send(websocket, frame)

    async def send_raw(self, text: str) -> None:
        """Send ``text`` as-is to every client, valid frame or not."""
        for websocket in list(self._clients):
            await websocket.send(text)

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

    def get_client_id_for_path(self, path: str) -> int | None:
        for websocket, websocket_path in self._client_path.items():
            if websocket_path == path:
                return self._client_ids.get(websocket)
        return None

    def url_for(self, path: str) -> str:
        return f"ws://{self.host}:{self.port}{path}"

    @property
    def url(self) -> str:
        return self.url_for(self.SOCKET_PATH)

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
