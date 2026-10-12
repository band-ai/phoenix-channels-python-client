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
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT)

# typeshed's signal._HANDLER is private.
_Handler = Callable[[int, FrameType | None], Any] | int | None


class _Waiter(NamedTuple):
    loop: asyncio.AbstractEventLoop
    event: asyncio.Event

    @property
    def live(self) -> bool:
        return not self.loop.is_closed()


# Each waiter maps to the last signal it took, if any.
_waiters: dict[_Waiter, int | None] = {}
_previous: dict[int, _Handler] = {}


def _on_main_thread() -> bool:
    return threading.current_thread() is threading.main_thread()


def _owned(sig: int) -> bool:
    return signal.getsignal(sig) is _on_shutdown_signal


def _on_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    del frame
    live = [waiter for waiter in tuple(_waiters) if waiter.live]
    for waiter in live:
        _waiters[waiter] = signum
        waiter.loop.call_soon_threadsafe(waiter.event.set)
    # Every waiting loop closed without unwinding: hand the signal back. If a
    # newer handler chained to this one instead, that handler already has it.
    if not live and _owned(signum):
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


def _unregister(waiter: _Waiter) -> int | None:
    """Drop ``waiter``, restoring the handlers if it was the last one.

    Returns the last signal it took, if any.
    """
    received = _waiters.pop(waiter, None)
    for orphan in [other for other in tuple(_waiters) if not other.live]:
        _waiters.pop(orphan, None)  # a closed loop never blocks a restore
    # GC may finalize an orphaned body off the main thread, where
    # signal.signal raises.
    if not _waiters and _on_main_thread():
        _restore()
    return received


@contextmanager
def handle_shutdown_signals(event: asyncio.Event) -> Generator[None, None, None]:
    """Set ``event`` on SIGTERM/SIGINT while inside.

    A signal is consumed only if the body completes with ``event`` set, and the
    caller must then act on it. Any other signal taken is handed back on exit.
    """
    if not _on_main_thread():
        logger.debug("Signal handlers not available off the main thread")
        yield
        return
    waiter = _Waiter(asyncio.get_running_loop(), event)
    consumed = False
    try:
        # Register before installing so a signal mid-install is never dropped.
        _waiters[waiter] = None
        if not _install():
            logger.debug("Signal handlers not available in this interpreter")
        yield
        consumed = event.is_set()
    finally:
        received = _unregister(waiter)
        if received is not None and not consumed and not _owned(received):
            signal.raise_signal(received)  # nobody acted on it: hand it back
