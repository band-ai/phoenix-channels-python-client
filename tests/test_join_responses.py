"""Join snapshots precede readiness and immediately following wire deltas."""

from __future__ import annotations

import pytest

from tests.fake_server import FakePhoenixServer, ReplyStatus, WireEvent
from tests.join_support import JoinProbe, deadline, push, reply
from tests.support import EVENT, FAST_RECONNECT, TOPIC, each_protocol, make_client


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
        phoenix_server.join_payloads[TOPIC] = {"status": ReplyStatus.OK, **payload}
        phoenix_server.join_events[TOPIC] = [(EVENT, {"delta": 1})]
        async with deadline(), make_client(phoenix_server) as client:
            probe = JoinProbe(client)
            result = await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            await probe.delivered.get()
            assert result is None
            assert probe.order == [payload.get("response"), {"delta": 1}]
            await client.unsubscribe_from_topic(TOPIC)
            assert not client.get_current_subscriptions()

    async def test_socket_rejoin_installs_before_immediate_delta(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with (
            deadline(),
            make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"revision": 2},
            }
            phoenix_server.join_events[TOPIC] = [(EVENT, {"delta": 2})]
            await phoenix_server.close_all_clients()
            await probe.delivered.get()
            assert probe.order == [{}, {"revision": 2}, {"delta": 2}]

    async def test_channel_recovery_installs_before_immediate_delta(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with (
            deadline(),
            make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client,
        ):
            probe = JoinProbe(client)
            await client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            phoenix_server.join_payloads[TOPIC] = {
                "status": ReplyStatus.OK,
                "response": {"revision": 2},
            }
            phoenix_server.join_events[TOPIC] = [(EVENT, {"delta": 2})]
            await phoenix_server.crash_channel(TOPIC)
            await probe.delivered.get()
            assert probe.order == [{}, {"revision": 2}, {"delta": 2}]

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
            await reply(phoenix_server, request, status=ReplyStatus.ERROR)
            await push(
                phoenix_server,
                request,
                WireEvent.REPLY,
                {"application": True},
                ref="application-request",
            )
            message = await probe.delivered.get()
            assert message.ref == "application-request"
            assert probe.order == [{}, {"application": True}]
