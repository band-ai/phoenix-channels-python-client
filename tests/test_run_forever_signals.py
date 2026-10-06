from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from types import FrameType
from typing import Any

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.shutdown_signals import (
    SHUTDOWN_SIGNALS,
    _on_shutdown_signal,
    handle_shutdown_signals,
)

from tests.conftest import ASYNC_TIMEOUT_S, FakePhoenixServer, make_client

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="a raised SIGINT kills the Windows test process"
)

Handler = Callable[[int, FrameType | None], None]


@pytest.fixture
def host_handlers() -> Iterator[tuple[Handler, list[int]]]:
    """Install one recording handler for both signals, as a host would.

    Installed before the test's event loop starts, like a host that owns its
    process signals.
    """
    received: list[int] = []

    def sentinel(signum: int, frame: FrameType | None) -> None:
        received.append(signum)

    originals = {sig: signal.signal(sig, sentinel) for sig in SHUTDOWN_SIGNALS}
    try:
        yield sentinel, received
    finally:
        for sig, original in originals.items():
            signal.signal(sig, original)


def installed(handler: Any) -> bool:
    return all(signal.getsignal(sig) is handler for sig in SHUTDOWN_SIGNALS)


def send(sig: signal.Signals, expected_handler: Any) -> None:
    # A regression fails here instead of delivering a real signal to pytest.
    assert signal.getsignal(sig) is expected_handler
    signal.raise_signal(sig)  # the Python handler runs before this returns


async def start(client: PHXChannelsClient, **kwargs: bool) -> asyncio.Task[None]:
    run = asyncio.create_task(client.run_forever(**kwargs))
    await asyncio.sleep(0)  # handlers are installed before the first suspension
    return run


async def test_without_signal_handlers_the_host_keeps_its_own(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    async with make_client(phoenix_server) as client:
        run = await start(client, install_signal_handlers=False)
        assert installed(sentinel)

        send(signal.SIGTERM, sentinel)
        await asyncio.sleep(0)
        assert received == [signal.SIGTERM]
        assert not run.done()

        await client.shutdown("host stop")
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert installed(sentinel)


@pytest.mark.parametrize(
    "stop_signal",
    [signal.SIGTERM, signal.SIGINT, None],
    ids=["sigterm", "sigint", "host-shutdown"],
)
async def test_run_forever_stops_and_restores_the_hosts_handlers(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
    stop_signal: signal.Signals | None,
) -> None:
    sentinel, received = host_handlers
    async with make_client(phoenix_server) as client:
        run = await start(client)
        if stop_signal is None:
            await client.shutdown("host stop")
        else:
            send(stop_signal, _on_shutdown_signal)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert client.connection is None
        assert installed(sentinel)
        assert received == []


def test_run_forever_restores_asyncio_runs_own_sigint_handler() -> None:
    async def run_and_stop() -> tuple[Any, Any]:
        asyncio_run_handler = signal.getsignal(signal.SIGINT)
        async with FakePhoenixServer() as server, make_client(server) as client:
            run = await start(client)
            send(signal.SIGINT, _on_shutdown_signal)
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        return asyncio_run_handler, signal.getsignal(signal.SIGINT)

    asyncio_run_handler, after_run_forever = asyncio.run(run_and_stop())
    assert asyncio_run_handler is not signal.default_int_handler
    assert after_run_forever is asyncio_run_handler


async def test_run_forever_keeps_a_loop_registered_host_handler(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    loop = asyncio.get_running_loop()
    hit = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, hit.set)
    try:
        host = signal.getsignal(signal.SIGTERM)
        async with make_client(phoenix_server) as client:
            run = await start(client)
            send(signal.SIGINT, _on_shutdown_signal)
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

        send(signal.SIGTERM, host)
        await asyncio.wait_for(hit.wait(), ASYNC_TIMEOUT_S)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


async def test_run_forever_never_overwrites_a_newer_host_handler(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, _ = host_handlers

    def newer(signum: int, frame: FrameType | None) -> None:
        pass

    async with make_client(phoenix_server) as client:
        run = await start(client)
        signal.signal(signal.SIGTERM, newer)
        send(signal.SIGINT, _on_shutdown_signal)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

    assert signal.getsignal(signal.SIGTERM) is newer
    assert signal.getsignal(signal.SIGINT) is sentinel


@pytest.mark.parametrize(
    "with_signal", [False, True], ids=["cancel", "cancel-with-sigterm"]
)
async def test_cancelled_run_forever_restores_and_hands_back_the_signal(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
    with_signal: bool,
) -> None:
    sentinel, received = host_handlers
    async with make_client(phoenix_server) as client:
        run = await start(client)
        if with_signal:
            send(signal.SIGTERM, _on_shutdown_signal)
        run.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
        assert installed(sentinel)
        assert client.connection is not None
        assert received == ([signal.SIGTERM] if with_signal else [])


async def test_one_signal_stops_every_waiting_client(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    async with make_client(phoenix_server) as a, make_client(phoenix_server) as b:
        runs = [await start(a), await start(b)]
        send(signal.SIGTERM, _on_shutdown_signal)

        for run in runs:
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert installed(sentinel)
        assert received == []


async def test_the_last_waiter_out_restores_the_handlers(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    async with make_client(phoenix_server) as a, make_client(phoenix_server) as b:
        run_a, run_b = await start(a), await start(b)

        await a.shutdown("host stop")
        assert await asyncio.wait_for(run_a, ASYNC_TIMEOUT_S) is None
        assert installed(_on_shutdown_signal)

        send(signal.SIGTERM, _on_shutdown_signal)
        assert await asyncio.wait_for(run_b, ASYNC_TIMEOUT_S) is None
        assert installed(sentinel)
        assert received == []


async def test_run_forever_off_the_main_thread_leaves_handlers_alone(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, _ = host_handlers

    async def run_and_stop() -> bool:
        async with make_client(phoenix_server) as client:
            run = await start(client)
            untouched = installed(sentinel)
            await client.shutdown("host stop")
            await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
            return untouched

    assert await asyncio.to_thread(asyncio.run, run_and_stop())
    assert installed(sentinel)


def test_a_signal_after_every_waiting_loop_closed_goes_to_the_host(
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    with ExitStack() as orphaned:

        async def enter() -> None:
            orphaned.enter_context(handle_shutdown_signals(asyncio.Event()))

        loop = asyncio.new_event_loop()
        loop.run_until_complete(enter())
        loop.close()  # the body never unwinds while its loop runs

        send(signal.SIGTERM, _on_shutdown_signal)
        assert received == [signal.SIGTERM]
        assert installed(sentinel)
