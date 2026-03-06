from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Mapping
from urllib.parse import parse_qs, urlparse

import pytest_asyncio
from websockets.asyncio.server import Server, ServerConnection, serve


class FakePhoenixServer:
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.valid_topics = {
            "test-topic",
            "test-topic-b",
        }

        self.server: Server | None = None
        self.client_websocket: ServerConnection | None = None
        self._clients: set[ServerConnection] = set()
        self._client_ids: dict[ServerConnection, int] = {}
        self._client_path: dict[ServerConnection, str] = {}
        self._client_api_key: dict[ServerConnection, str] = {}
        self._next_client_id = 1
        self.close_on_join_ids: set[int] = set()
        self.close_on_join_code = 1012
        self.close_on_join_reason = "forced close on join"
        self.fail_join_ids: set[int] = set()
        self.fail_join_targets: set[tuple[int, str]] = set()
        self.enforce_single_connection_per_api_key = False
        self.duplicate_close_code = 1013
        self.duplicate_close_reason = "duplicate session"
        self.connection_attempts_by_path: dict[str, int] = {}

    def is_valid_topic(self, topic: str) -> bool:
        """Check if a topic is valid for subscription."""
        return topic in self.valid_topics

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

    async def handler(self, websocket: ServerConnection) -> None:
        """Handle WebSocket connections and messages."""
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
                data = json.loads(message)
                await self.handle_message(websocket, data)
        except Exception:
            pass
        finally:
            self._clients.discard(websocket)
            self._client_ids.pop(websocket, None)
            self._client_path.pop(websocket, None)
            self._client_api_key.pop(websocket, None)

    async def handle_message(
        self, websocket: ServerConnection, data: list[object]
    ) -> None:
        """Handle incoming Phoenix messages (array format: [join_ref, msg_ref, topic, event, payload])."""
        if not isinstance(data, list) or len(data) != 5:
            return

        join_ref, msg_ref, topic, event, payload = data

        if not isinstance(topic, str):
            return

        client_id = self._client_ids.get(websocket)

        if topic == "phoenix" and event == "heartbeat":
            reply = [
                join_ref,
                msg_ref,
                "phoenix",
                "phx_reply",
                {"status": "ok", "response": {}},
            ]
            await websocket.send(json.dumps(reply))
            return

        if event == "phx_join":
            if client_id in self.fail_join_ids or (
                client_id is not None and (client_id, topic) in self.fail_join_targets
            ):
                reply = [
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    {"status": "error", "response": {"reason": "forced join failure"}},
                ]
                await websocket.send(json.dumps(reply))
                return

            if self.is_valid_topic(topic):
                reply = [
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    {"status": "ok", "response": {}},
                ]
            else:
                reply = [
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    {"status": "error", "response": {"reason": "unmatched topic"}},
                ]

            await websocket.send(json.dumps(reply))

            if client_id in self.close_on_join_ids:
                await websocket.close(
                    code=self.close_on_join_code,
                    reason=self.close_on_join_reason,
                )

        elif event == "phx_leave":
            reply = [
                join_ref,
                msg_ref,
                topic,
                "phx_reply",
                {"status": "ok", "response": {}},
            ]
            await websocket.send(json.dumps(reply))

            close_message = [join_ref, join_ref, topic, "phx_close", {}]
            await websocket.send(json.dumps(close_message))

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

        message = [join_ref, None, topic, event, payload]
        for websocket in targets:
            await websocket.send(json.dumps(message))

    async def close_all_clients(
        self,
        *,
        code: int = 1012,
        reason: str = "service restart",
    ) -> None:
        for websocket in list(self._clients):
            await websocket.close(code=code, reason=reason)

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

    async def start(self) -> None:
        """Start the fake Phoenix server."""
        self.server = await serve(self.handler, self.host, self.port)

    async def stop(self) -> None:
        """Stop the fake Phoenix server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/socket/websocket"


@pytest_asyncio.fixture
async def phoenix_server() -> AsyncGenerator[FakePhoenixServer, None]:
    """Fixture that provides a fake Phoenix WebSocket server."""
    server = FakePhoenixServer()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
