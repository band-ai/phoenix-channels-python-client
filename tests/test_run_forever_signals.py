from __future__ import annotations

import asyncio
import signal
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from types import FrameType
from typing import Any

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.client_types import ClientState
from phoenix_channels_python_client.exceptions import PHXConnectionError
from phoenix_channels_python_client.shutdown_signals import (
    SHUTDOWN_SIGNALS,
    _on_shutdown_signal,
    handle_shutdown_signals,
)
from tests.fake_server import FakePhoenixServer
from tests.support import ASYNC_TIMEOUT_S, STOP_REASON, make_client

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


async def serve_until(server: FakePhoenixServer, stop_signal: signal.Signals) -> None:
    """Run a client until ``stop_signal`` stops it."""
    async with make_client(server) as client:
        run = await start(client)
        send(stop_signal, _on_shutdown_signal)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None


@contextmanager
def orphaned_waiter() -> Iterator[None]:
    """A waiter whose loop closed while it was still waiting for a signal."""
    with ExitStack() as orphaned:

        async def wait_for_signals() -> None:
            orphaned.enter_context(handle_shutdown_signals(asyncio.Event()))

        with asyncio.Runner() as runner:
            runner.run(wait_for_signals())
        yield


async def test_without_signal_handlers_the_host_keeps_its_own(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, _ = host_handlers
    async with make_client(phoenix_server) as client:
        run = await start(client, install_signal_handlers=False)
        send(signal.SIGTERM, sentinel)

        await client.shutdown(STOP_REASON)
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
            await client.shutdown(STOP_REASON)
        else:
            send(stop_signal, _on_shutdown_signal)

        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
        assert client.connection is None
        assert installed(sentinel)
        assert received == []


def test_run_forever_restores_asyncio_runs_own_sigint_handler() -> None:
    async def main() -> tuple[Any, Any]:
        asyncio_run_handler = signal.getsignal(signal.SIGINT)
        async with FakePhoenixServer() as server:
            await serve_until(server, signal.SIGINT)
        return asyncio_run_handler, signal.getsignal(signal.SIGINT)

    asyncio_run_handler, after_run_forever = asyncio.run(main())
    assert asyncio_run_handler is not signal.default_int_handler
    assert after_run_forever is asyncio_run_handler


@pytest.mark.usefixtures("host_handlers")
async def test_a_loop_registered_host_handler_fires_during_and_after_run_forever(
    phoenix_server: FakePhoenixServer,
) -> None:
    loop = asyncio.get_running_loop()
    hit = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, hit.set)
    try:
        host = signal.getsignal(signal.SIGTERM)
        async with make_client(phoenix_server) as client:
            run = await start(client)
            send(signal.SIGTERM, _on_shutdown_signal)
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None
            await asyncio.wait_for(hit.wait(), ASYNC_TIMEOUT_S)

        hit.clear()
        send(signal.SIGTERM, host)
        await asyncio.wait_for(hit.wait(), ASYNC_TIMEOUT_S)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


async def test_a_newer_host_handler_stays_and_may_chain_to_ours(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    chained: list[int] = []
    async with make_client(phoenix_server) as client:
        run = await start(client)

        def newer(signum: int, frame: FrameType | None) -> None:
            chained.append(signum)
            _on_shutdown_signal(signum, frame)  # chains to the handler it replaced

        assert signal.signal(signal.SIGTERM, newer) is _on_shutdown_signal
        send(signal.SIGINT, _on_shutdown_signal)
        assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

    assert signal.getsignal(signal.SIGINT) is sentinel
    send(signal.SIGTERM, newer)
    assert chained == [signal.SIGTERM]
    assert received == []


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


async def test_run_forever_after_the_client_stopped_raises(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(phoenix_server)
    async with client:
        pass

    with pytest.raises(PHXConnectionError, match="not connected"):
        await asyncio.wait_for(
            client.run_forever(install_signal_handlers=False), ASYNC_TIMEOUT_S
        )


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

        await a.shutdown(STOP_REASON)
        assert await asyncio.wait_for(run_a, ASYNC_TIMEOUT_S) is None
        assert installed(_on_shutdown_signal)

        send(signal.SIGTERM, _on_shutdown_signal)
        assert await asyncio.wait_for(run_b, ASYNC_TIMEOUT_S) is None
        assert installed(sentinel)
        assert received == []


async def test_a_signal_never_stops_a_client_waiting_off_the_main_thread(
    phoenix_server: FakePhoenixServer,
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, _ = host_handlers
    main_loop = asyncio.get_running_loop()
    worker_waiting = asyncio.Event()
    release_worker = threading.Event()

    async def wait_on_worker_thread() -> bool:
        async with make_client(phoenix_server) as client:
            run = await start(client)
            main_loop.call_soon_threadsafe(worker_waiting.set)
            await asyncio.to_thread(release_worker.wait, ASYNC_TIMEOUT_S)
            still_connected = client._state is ClientState.CONNECTED
            await client.shutdown(STOP_REASON)
            await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
            return still_connected

    worker = asyncio.create_task(
        asyncio.to_thread(asyncio.run, wait_on_worker_thread())
    )
    try:
        await asyncio.wait_for(worker_waiting.wait(), ASYNC_TIMEOUT_S)
        assert installed(sentinel)
        await serve_until(phoenix_server, signal.SIGTERM)
    finally:
        release_worker.set()
    assert await asyncio.wait_for(worker, ASYNC_TIMEOUT_S)
    assert installed(sentinel)


def test_a_signal_after_every_waiting_loop_closed_goes_to_the_host(
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers
    with orphaned_waiter():
        send(signal.SIGTERM, _on_shutdown_signal)
        assert received == [signal.SIGTERM]
        assert installed(sentinel)


def test_a_closed_loop_never_blocks_restoring_the_hosts_handlers(
    host_handlers: tuple[Handler, list[int]],
) -> None:
    sentinel, received = host_handlers

    async def main() -> None:
        async with FakePhoenixServer() as server:
            await serve_until(server, signal.SIGTERM)

    with orphaned_waiter():
        asyncio.run(main())
        assert installed(sentinel)
        assert received == []
