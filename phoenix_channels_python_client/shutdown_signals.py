"""Process-wide SIGTERM/SIGINT handling shared by every waiting event loop.

Handlers are installed with ``signal.signal`` and the previous handler is put
back exactly, as ``asyncio.Runner`` does. Signal handlers interrupt main-thread
code between bytecodes, so nothing here takes a lock: the OS handler table
decides ownership, the handler never iterates a live container or removes
entries, and ``_previous`` entries are only ever overwritten.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from types import FrameType
from typing import Any

logger = logging.getLogger(__name__)

SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)

# typeshed's signal._HANDLER is private.
_Handler = Callable[[int, FrameType | None], Any] | int | None

# Each waiting (loop, event) maps to the last signal it took, if any.
_waiters: dict[tuple[asyncio.AbstractEventLoop, asyncio.Event], int | None] = {}
_previous: dict[int, _Handler] = {}


def _owned(sig: int) -> bool:
    return signal.getsignal(sig) is _on_shutdown_signal


def _on_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    live = [(loop, event) for loop, event in tuple(_waiters) if not loop.is_closed()]
    for loop, event in live:
        _waiters[(loop, event)] = signum
        loop.call_soon_threadsafe(event.set)
    # Every waiting loop closed without unwinding: hand the signal back.
    if not live and signum in _previous:
        _restore()
        signal.raise_signal(signum)


def _install() -> bool:
    try:
        for sig in SHUTDOWN_SIGNALS:
            # None means a handler set outside Python, which can't be restored.
            if not _owned(sig) and signal.getsignal(sig) is not None:
                _previous[sig] = signal.signal(sig, _on_shutdown_signal)
    except ValueError:
        # The main thread can't take signals (embedded interpreter, gh-91880).
        return False
    return True


def _restore() -> None:
    for sig in SHUTDOWN_SIGNALS:
        if _owned(sig):  # never overwrite a handler installed after ours
            signal.signal(sig, _previous[sig])


@contextmanager
def handle_shutdown_signals(event: asyncio.Event) -> Generator[None, None, None]:
    """Set ``event`` on SIGTERM/SIGINT while inside.

    A signal is consumed only if the body completes with ``event`` set, and the
    caller must then act on it. Any other signal taken is handed back on exit.
    """
    if threading.current_thread() is not threading.main_thread():
        logger.debug("Signal handlers not available off the main thread")
        yield
        return
    waiter = (asyncio.get_running_loop(), event)
    consumed = False
    try:
        # Register before installing so a signal mid-install is never dropped.
        _waiters[waiter] = None
        if not _install():
            logger.debug("Signal handlers not available in this interpreter")
        yield
        consumed = event.is_set()
    finally:
        received = _waiters.pop(waiter, None)
        for orphan in [w for w in tuple(_waiters) if w[0].is_closed()]:
            _waiters.pop(orphan, None)  # a closed loop never blocks a restore
        # GC may finalize an orphaned body off the main thread, where
        # signal.signal raises.
        if not _waiters and threading.current_thread() is threading.main_thread():
            _restore()
        if received is not None and not consumed and not _owned(received):
            signal.raise_signal(received)
