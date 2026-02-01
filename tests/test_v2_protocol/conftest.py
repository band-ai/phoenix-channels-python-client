import json
import pytest_asyncio
from typing import AsyncGenerator
from websockets.asyncio.server import serve


class FakePhoenixServerV2:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.server = None
        self.client_websocket = None
        self.valid_topics = {
            "test-topic",
            "test-topic-b",
        }

    def is_valid_topic(self, topic):
        """Check if a topic is valid for subscription"""
        return topic in self.valid_topics

    async def handler(self, websocket):
        """Handle WebSocket connections and messages"""
        self.client_websocket = websocket
        try:
            async for message in websocket:
                data = json.loads(message)
                await self.handle_message(data)
        except Exception:
            pass
        finally:
            self.client_websocket = None

    async def handle_message(self, data):
        """Handle incoming v2 protocol messages (array format: [join_ref, msg_ref, topic, event, payload])"""
        if not isinstance(data, list) or len(data) != 5:
            return  # Invalid v2 message format

        join_ref, msg_ref, topic, event, payload = data

        if event == "phx_join":
            # Check if topic is valid before allowing join
            if self.is_valid_topic(topic):
                # Send successful join reply for valid topics
                reply = [
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    {"status": "ok", "response": {}},
                ]
            else:
                # Send error reply for invalid topics
                reply = [
                    join_ref,
                    msg_ref,
                    topic,
                    "phx_reply",
                    {"status": "error", "response": {"reason": "unmatched topic"}},
                ]
            await self.client_websocket.send(json.dumps(reply))
        elif event == "phx_leave":
            # Send successful leave reply
            reply = [
                join_ref,
                msg_ref,
                topic,
                "phx_reply",
                {"status": "ok", "response": {}},
            ]
            await self.client_websocket.send(json.dumps(reply))

            # Also send phx_close message after successful leave
            close_message = [join_ref, join_ref, topic, "phx_close", {}]
            await self.client_websocket.send(json.dumps(close_message))

    async def simulate_server_event(self, topic, event, payload, join_ref=None):
        """Simulate a server event being sent to the client for testing purposes

        Args:
            topic: The topic to send the event to
            event: The event name
            payload: The event payload
            join_ref: The join_ref to use (test controls this directly)
        """
        if self.client_websocket:
            message = [join_ref, None, topic, event, payload]
            await self.client_websocket.send(json.dumps(message))

    async def start(self):
        """Start the fake Phoenix server"""
        self.server = await serve(self.handler, self.host, self.port)

    async def stop(self):
        """Stop the fake Phoenix server"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        self.client_websocket = None

    @property
    def url(self):
        return f"ws://{self.host}:{self.port}/socket/websocket"


@pytest_asyncio.fixture
async def phoenix_server() -> AsyncGenerator[FakePhoenixServerV2, None]:
    """Fixture that provides a fake Phoenix WebSocket server (v2 protocol)"""
    server = FakePhoenixServerV2()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
