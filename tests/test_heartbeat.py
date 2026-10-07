from __future__ import annotations

import pytest


from tests.fake_server import FakePhoenixServer
from tests.support import (
    FAST_RECONNECT,
    make_client,
    reconnect,
    wait_for_condition,
)

# Short enough that a test sees several heartbeats.
HEARTBEAT_INTERVAL_S = 0.05


async def test_heartbeats_are_sent_and_acknowledged(phoenix_server: FakePhoenixServer):
    acks: list[None] = []

    async with make_client(
        phoenix_server,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        on_heartbeat_ack=lambda: acks.append(None),
    ):
        assert await wait_for_condition(lambda: len(acks) >= 2)


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


async def test_the_heartbeat_resumes_after_a_reconnect(
    phoenix_server: FakePhoenixServer,
):
    acks: list[None] = []

    client = make_client(
        phoenix_server,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        reconnect_policy=FAST_RECONNECT,
        on_heartbeat_ack=lambda: acks.append(None),
    )

    async with client:
        await reconnect(phoenix_server, client)
        acks.clear()

        assert await wait_for_condition(lambda: bool(acks))


async def test_an_unanswered_heartbeat_is_reported(
    phoenix_server: FakePhoenixServer, caplog: pytest.LogCaptureFixture
):
    phoenix_server.answer_heartbeats = False

    async with make_client(phoenix_server, heartbeat_interval_s=HEARTBEAT_INTERVAL_S):
        assert await wait_for_condition(
            lambda: "server may be unresponsive" in caplog.text
        )
