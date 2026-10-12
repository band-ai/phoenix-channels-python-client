from __future__ import annotations

import asyncio
import logging

import pytest

from phoenix_channels_python_client.utils import cancel_and_wait
from tests.support import ASYNC_TIMEOUT_S, wait_forever


async def test_cancel_and_wait_propagates_callers_cancellation() -> None:
    release = asyncio.Event()

    async def slow_to_unwind() -> None:
        try:
            await wait_forever()
        finally:
            await release.wait()

    target = asyncio.create_task(slow_to_unwind())
    await asyncio.sleep(0)
    caller = asyncio.create_task(cancel_and_wait(target))
    await asyncio.sleep(0)

    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(caller, ASYNC_TIMEOUT_S)

    release.set()
    await asyncio.wait({target}, timeout=ASYNC_TIMEOUT_S)
    assert target.cancelled()


async def test_cancel_and_wait_logs_a_failure_raised_while_unwinding(
    caplog: pytest.LogCaptureFixture,
) -> None:
    failure = RuntimeError()

    async def fails_when_cancelled() -> None:
        try:
            await wait_forever()
        except asyncio.CancelledError:
            raise failure from None

    target = asyncio.create_task(fails_when_cancelled())
    await asyncio.sleep(0)
    with caplog.at_level(logging.ERROR):
        await asyncio.wait_for(cancel_and_wait(target), ASYNC_TIMEOUT_S)

    (record,) = caplog.records
    assert record.exc_info is not None
    assert record.exc_info[1] is failure
