from __future__ import annotations

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient

from tests.fake_server import FakePhoenixServer
from tests.support import (
    API_KEY,
    FAST_RECONNECT,
    make_client,
    reconnect,
    wait_for_condition,
)

# Short enough that a test sees several heartbeats.
HEARTBEAT_INTERVAL_S = 0.1


async def test_heartbeats_are_sent_and_acknowledged(phoenix_server: FakePhoenixServer):
    acks = 0

    def count_ack() -> None:
        nonlocal acks
        acks += 1

    async with make_client(
        phoenix_server,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        on_heartbeat_ack=count_ack,
    ):
        assert await wait_for_condition(lambda: acks >= 2)


async def test_heartbeats_can_be_disabled(phoenix_server: FakePhoenixServer):
    # The heartbeat task starts before the connection is reported ready, so
    # its absence here is final.
    async with make_client(phoenix_server, heartbeat_interval_s=None) as client:
        assert client._heartbeat_task is None


async def test_the_heartbeat_stops_on_shutdown(phoenix_server: FakePhoenixServer):
    client = make_client(phoenix_server, heartbeat_interval_s=HEARTBEAT_INTERVAL_S)

    async with client:
        assert await wait_for_condition(lambda: client._heartbeat_task is not None)

    assert client._heartbeat_task is None
    assert client._pending_heartbeat_ref is None


@pytest.mark.parametrize("interval_s", [0, -1.0])
def test_a_non_positive_heartbeat_interval_is_rejected(interval_s: float):
    with pytest.raises(ValueError, match="heartbeat_interval_s must be > 0"):
        PHXChannelsClient(
            FakePhoenixServer().url, api_key=API_KEY, heartbeat_interval_s=interval_s
        )


async def test_the_heartbeat_resumes_after_a_reconnect(
    phoenix_server: FakePhoenixServer,
):
    client = make_client(
        phoenix_server,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        reconnect_policy=FAST_RECONNECT,
    )

    async with client:
        await reconnect(phoenix_server, client)

        # It runs on the new connection and the server acknowledges it.
        assert await wait_for_condition(
            lambda: client._heartbeat_task is not None
            and not client._heartbeat_task.done()
            and client._pending_heartbeat_ref is None
        )
