from websockets.asyncio.server import ServerConnection

from tests.fake_server import FakePhoenixServer, Frame


class UnansweredLeavesPeer(FakePhoenixServer):
    """Deliberately accept leave frames without replying or ending membership."""

    async def _handle_leave(self, websocket: ServerConnection, frame: Frame) -> None:
        self.left.put_nowait((websocket, frame))
