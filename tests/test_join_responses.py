"""Join snapshots precede readiness and immediately following wire deltas."""

from __future__ import annotations

import asyncio

import pytest

from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from tests.fake_server import FakePhoenixServer, ReplyStatus, WireEvent
from tests.join_support import (
    JoinProbe,
    deadline,
    paused_writes,
    pending_subscribe,
    push,
    reply,
)
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

    async def test_accepted_admission_survives_a_later_send_failure(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with (
            deadline(),
            make_client(
                phoenix_server,
                reconnect_policy=FAST_RECONNECT,
                heartbeat_interval_s=None,
            ) as client,
        ):
            probe = JoinProbe(client)
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            connection = client.connection
            assert connection is not None
            with paused_writes(connection):
                async with pending_subscribe(client, probe) as admission:
                    peer, request = await phoenix_server.deferred_joins.get()
                    await phoenix_server.release_join(peer, request)
                    assert await probe.installed.get() == {}
                    assert client.is_topic_joined(TOPIC)
                    assert not admission.done()
                    phoenix_server.join_payloads[TOPIC] = {
                        "status": ReplyStatus.OK,
                        "response": {"fresh": True},
                    }
                    phoenix_server.join_events[TOPIC] = [(EVENT, {"delta": True})]
                    await phoenix_server.reset_all_clients()
                    assert await admission is None
                await probe.delivered.get()
            assert probe.order == [{}, {"fresh": True}, {"delta": True}]
            assert client.is_topic_joined(TOPIC)

    async def test_send_failure_before_installation_fails_its_caller(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        async with (
            deadline(),
            make_client(
                phoenix_server, auto_reconnect=False, heartbeat_interval_s=None
            ) as client,
        ):
            probe = JoinProbe(client)
            phoenix_server.defer_join_targets.add(
                (phoenix_server.next_client_id - 1, TOPIC)
            )
            connection = client.connection
            assert connection is not None
            with paused_writes(connection):
                async with pending_subscribe(client, probe) as admission:
                    await phoenix_server.deferred_joins.get()
                    await phoenix_server.reset_all_clients()
                    with pytest.raises(PHXConnectionError):
                        await admission
            assert probe.order == []
            assert TOPIC not in client.get_current_subscriptions()

    async def test_installation_failure_wins_over_a_later_send_failure(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        invoked = asyncio.Event()
        failure = ValueError("invalid application snapshot")

        def install(response: object) -> None:
            del response
            invoked.set()
            raise failure

        async with (
            deadline(),
            make_client(
                phoenix_server, auto_reconnect=False, heartbeat_interval_s=None
            ) as client,
        ):
            connection = client.connection
            assert connection is not None
            with paused_writes(connection):
                admission = asyncio.create_task(
                    client.subscribe_to_topic(TOPIC, on_join_response=install)
                )
                try:
                    await invoked.wait()
                    await phoenix_server.reset_all_clients()
                    with pytest.raises(PHXTopicError) as caught:
                        await admission
                    assert caught.value.__cause__ is failure
                finally:
                    admission.cancel()
                    await asyncio.gather(admission, return_exceptions=True)
            assert TOPIC not in client.get_current_subscriptions()
