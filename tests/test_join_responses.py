from __future__ import annotations

import asyncio

import pytest

from tests.fake_server import FakePhoenixServer
from tests.join_support import JoinProbe, deadline, push, reply
from tests.support import FAST_RECONNECT, TOPIC, each_protocol, make_client


@each_protocol
class TestJoinResponses:
    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"response": None},
            {"response": []},
            {"response": {"agents": [1]}},
            {"response": "application-invalid"},
        ],
    )
    async def test_decoded_snapshot_precedes_readiness_and_delta(
        self, phoenix_server: FakePhoenixServer, payload: dict[str, object]
    ) -> None:
        phoenix_server.join_payloads[TOPIC] = {"status": "ok", **payload}
        async with deadline(), make_client(phoenix_server) as client:
            probe = JoinProbe(client)
            result = await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request, payload={"delta": 1})
            await probe.delivered.get()
            assert result is None
            assert probe.order == [payload.get("response"), {"delta": 1}]
            await client.unsubscribe_from_topic(TOPIC)
            assert not client.get_current_subscriptions()

    @pytest.mark.parametrize("socket_reconnect", [False, True])
    async def test_every_recovery_installs_before_its_events(
        self, phoenix_server: FakePhoenixServer, *, socket_reconnect: bool
    ) -> None:
        reconnects: asyncio.Queue[None] = asyncio.Queue()
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                on_reconnect=lambda: reconnects.put(None),
            ) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await phoenix_server.joined.get()
            await probe.installed.get()
            phoenix_server.join_payloads[TOPIC] = {
                "status": "ok",
                "response": {"revision": 2},
            }
            if socket_reconnect:
                await phoenix_server.close_all_clients()
                await reconnects.get()
            else:
                await phoenix_server.crash_channel(TOPIC)
            _, request = await phoenix_server.joined.get()
            assert await probe.installed.get() == {"revision": 2}
            await push(phoenix_server, request, payload={"delta": 2})
            await probe.delivered.get()
            expected = [{}, {"revision": 2}, {"delta": 2}]
            assert probe.order == expected

    async def test_consumed_join_acks_are_suppressed_but_other_replies_deliver(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with deadline(), make_client(phoenix_server) as client:
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            _, request = await phoenix_server.joined.get()
            await reply(phoenix_server, request)
            await reply(phoenix_server, request, status="error")
            await push(
                phoenix_server,
                request,
                "phx_reply",
                {"application": True},
                ref="application-request",
            )
            message = await probe.delivered.get()
            assert message.ref == "application-request"
            assert probe.order == [{}, {"application": True}]
