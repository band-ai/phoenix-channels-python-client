"""Opt-in virtual time, stationary while real sockets become ready."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable

import looptime
import pytest
import pytest_asyncio

from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import FakePhoenixServer

CLOCK_RESOLUTION = 2**-16


class ClockLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        super().__init__()
        self._clock_resolution: float = CLOCK_RESOLUTION


def controlled_loop() -> asyncio.AbstractEventLoop:
    return looptime.patch_event_loop(ClockLoop(), start=0, resolution=CLOCK_RESOLUTION)


class Clock:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self._stationary: asyncio.Handle | None = None
        self.freeze()

    def freeze(self) -> None:
        self._stationary = self.loop.call_soon(self.freeze)

    def stop(self) -> None:
        if self._stationary is not None:
            self._stationary.cancel()

    def jump(self, seconds: float) -> None:
        assert isinstance(self.loop, looptime.LoopTimeEventLoop)
        self.loop.setup_looptime(
            start=self.loop.time() + seconds, resolution=CLOCK_RESOLUTION
        )

    async def advance(self, seconds: float) -> None:
        self.jump(seconds)
        await asyncio.sleep(0)


@pytest_asyncio.fixture
async def clock() -> AsyncIterator[Clock]:
    loop = asyncio.get_running_loop()
    assert isinstance(loop, looptime.LoopTimeEventLoop)
    clock = Clock(loop)
    try:
        yield clock
    finally:
        clock.stop()


@pytest_asyncio.fixture
async def clocked_peer(
    clock: Clock, protocol: PhoenixChannelsProtocolVersion
) -> AsyncIterator[FakePhoenixServer]:
    del clock
    async with FakePhoenixServer(protocol) as server:
        yield server


def pytest_asyncio_loop_factories(
    config: pytest.Config, item: pytest.Item
) -> dict[str, Callable[[], asyncio.AbstractEventLoop]]:
    del config
    if item.get_closest_marker("clock") is not None:
        return {"clock": controlled_loop}
    # pytest-asyncio 1.4 requires a mapping even for unmarked tests.
    return {"default": asyncio.new_event_loop}
