from __future__ import annotations

import asyncio
import logging

import pytest

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.exceptions import ChannelError
from phoenix_channels_python_client.messages import Message, Payload, SystemEvent
from phoenix_channels_python_client.protocol import ProtocolVersion
from phoenix_channels_python_client.topic_subscription import ChannelState
from tests.fake_server import FakePhoenixServer, WireEvent
from tests.support import (
    ASYNC_TIMEOUT_S,
    EVENT,
    FAST_RECONNECT,
    HEARTBEAT_INTERVAL_S,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    REJOINED_ONCE,
    REJOINED_TWICE,
    SUBSCRIBED,
    TOPIC,
    BusyCallback,
    LostTopics,
    ReconnectCounter,
    channel_for,
    crash_with_rejoin_in_flight,
    deferred_channel_rejoin,
    deliver,
    derive_policy,
    each_protocol,
    expect_message,
    is_joined,
    join_channel,
    joins,
    make_client,
    only_client_id,
    reconnect,
    rejoin_settled,
    subscribe_with_handlers,
    wait_for_condition,
)

# Several heartbeats long, so the outage before the first rejoin is observable.
SLOW_RECOVERY = derive_policy(FAST_RECONNECT, base_delay_s=0.3, max_delay_s=0.3)

# Longer than any FAST_RECONNECT backoff plus a local join, so a retry that
# should not happen would have happened by then.
QUIET_S = 2 * FAST_RECONNECT.max_delay_s

# A leave the server never answers would wait this long; a dead channel's
# unsubscribe must finish far sooner.
SLOW_LEAVE_TIMEOUT_S = ASYNC_TIMEOUT_S
PROMPT_S = SLOW_LEAVE_TIMEOUT_S / 4


@each_protocol
async def test_a_crashed_channel_rejoins_on_the_live_socket(
    phoenix_server: FakePhoenixServer,
    received: asyncio.Queue[Message],
    caplog: pytest.LogCaptureFixture,
) -> None:
    acks: list[None] = []
    reconnects = ReconnectCounter()
    client = make_client(
        phoenix_server,
        reconnect_policy=SLOW_RECOVERY,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        on_heartbeat_ack=lambda: acks.append(None),
        on_reconnect=reconnects,
    )
    caplog.set_level(logging.INFO)
    async with client:
        await join_channel(client, TOPIC, received.put)
        socket = client.connection

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(
            lambda: not is_joined(client, TOPIC), interval_s=0
        )
        acks_when_lost = len(acks)
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))

        assert len(acks) > acks_when_lost
        assert joins(phoenix_server) == REJOINED_ONCE
        assert client.connection is socket
        await deliver(phoenix_server, client)
        # The callback's next message is this one, so it never saw the phx_error.
        assert (await expect_message(received)).event == EVENT
        assert reconnects.count == 0

    assert "Recovered channel for topic <omitted>" in caplog.text


@each_protocol
async def test_a_broadcast_during_rejoin_is_not_delivered(
    phoenix_server: FakePhoenixServer,
) -> None:
    sentinel = {"phase": "joined"}

    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        recorder = await subscribe_with_handlers(client)
        async with deferred_channel_rejoin(phoenix_server):
            assert not is_joined(client, TOPIC)
            await phoenix_server.simulate_server_event(
                TOPIC, EVENT, {"phase": "rejoining"}
            )

        await phoenix_server.simulate_server_event(TOPIC, EVENT, sentinel)

        # The sentinel follows the pending event and join reply on the same socket.
        assert (await expect_message(recorder.messages)).payload == sentinel
        assert await expect_message(recorder.event_payloads) == sentinel
        assert recorder.deliveries == [(sentinel, True)]


@each_protocol
async def test_a_repeated_channel_error_starts_one_recovery(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=SLOW_RECOVERY) as client:
        await join_channel(client, TOPIC)
        stored_join_ref = phoenix_server.members[
            (only_client_id(phoenix_server), TOPIC)
        ]

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(lambda: not is_joined(client, TOPIC))
        await phoenix_server.simulate_server_event(
            TOPIC, WireEvent.ERROR, {}, join_ref=stored_join_ref, ref=stored_join_ref
        )
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))
        await asyncio.sleep(SLOW_RECOVERY.max_delay_s)

        assert joins(phoenix_server) == REJOINED_ONCE


@pytest.mark.parametrize("protocol", [ProtocolVersion.V2])
async def test_a_stale_channel_error_is_ignored(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[Message]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await join_channel(client, TOPIC, received.put)
        old_join_ref = client.topic_subscriptions.copy()[TOPIC].join_ref
        await reconnect(phoenix_server, client)
        assert await wait_for_condition(rejoin_settled(client))

        await phoenix_server.simulate_server_event(
            TOPIC, WireEvent.ERROR, {}, join_ref=old_join_ref, ref=old_join_ref
        )
        # Frames arrive in order, so the stale error was handled before this.
        await deliver(phoenix_server, client)
        assert (await expect_message(received)).event == EVENT
        await asyncio.sleep(QUIET_S)

        assert is_joined(client, TOPIC)
        assert joins(phoenix_server) == REJOINED_ONCE


@each_protocol
async def test_a_rejected_channel_rejoin_loses_the_topic(
    phoenix_server: FakePhoenixServer, caplog: pytest.LogCaptureFixture
) -> None:
    lost = LostTopics()
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_topic_lost=lost
    )
    async with client:
        await join_channel(client, TOPIC)
        phoenix_server.fail_join_targets.add((only_client_id(phoenix_server), TOPIC))

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(lambda: bool(lost.lost))
        await asyncio.sleep(QUIET_S)

        lost.only_error()
        assert TOPIC not in client.topic_subscriptions.copy()
        assert joins(phoenix_server) == REJOINED_ONCE
    assert "Lost topic <omitted>" in caplog.text


@each_protocol
async def test_a_channel_crashing_again_right_after_its_rejoin_is_recovered(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await join_channel(client, TOPIC)
        phoenix_server.crashes_after_join = 1

        await phoenix_server.crash_channel(TOPIC)

        assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_TWICE)
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))
        await asyncio.sleep(QUIET_S)
        assert joins(phoenix_server) == REJOINED_TWICE


@each_protocol
async def test_a_timed_out_channel_rejoin_is_retried(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, join_timeout_s=JOIN_TIMEOUT_S
    )
    async with client:
        await join_channel(client, TOPIC)
        client_id = await crash_with_rejoin_in_flight(phoenix_server, client)

        phoenix_server.unanswered_join_ids.discard(client_id)

        assert await wait_for_condition(lambda: is_joined(client, TOPIC))
        assert joins(phoenix_server) == REJOINED_TWICE


async def test_unsubscribing_during_recovery_completes_promptly(
    phoenix_server: FakePhoenixServer,
) -> None:
    lost = LostTopics()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        leave_timeout_s=SLOW_LEAVE_TIMEOUT_S,
        on_topic_lost=lost,
    )
    async with client:
        channel = await join_channel(client, TOPIC)
        await crash_with_rejoin_in_flight(phoenix_server, client)

        await asyncio.wait_for(channel.leave(), PROMPT_S)
        await asyncio.sleep(QUIET_S)

        assert not channel.joined
        assert TOPIC in client.topic_subscriptions
        with pytest.raises(ChannelError):
            await client.channel(TOPIC).join()
        assert joins(phoenix_server) == REJOINED_ONCE
        assert not lost.lost


@each_protocol
async def test_a_socket_drop_during_recovery_hands_the_topic_to_the_reconnect(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await join_channel(client, TOPIC)
        await crash_with_rejoin_in_flight(phoenix_server, client)

        # The new connection's joins are answered.
        await reconnect(phoenix_server, client)
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))
        await asyncio.sleep(QUIET_S)

        assert joins(phoenix_server) == REJOINED_TWICE


async def test_a_channel_error_during_a_callback_does_not_double_rejoin(
    phoenix_server: FakePhoenixServer,
) -> None:
    held = BusyCallback()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        # Outlasts the test, so the drain waits for the release below.
        callback_drain_timeout_s=ASYNC_TIMEOUT_S,
    )
    async with client:
        await join_channel(client, TOPIC, held)
        await deliver(phoenix_server, client)
        await asyncio.wait_for(held.running.wait(), ASYNC_TIMEOUT_S)
        await phoenix_server.crash_channel(TOPIC)
        subscription = client.topic_subscriptions.copy()[TOPIC]
        assert await wait_for_condition(
            lambda: subscription.state == ChannelState.REJOINING
        )
        assert subscription.queue.empty()

        # The reader starts recovery while the application callback is held.
        await reconnect(phoenix_server, client)
        held.release.set()
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))
        await asyncio.sleep(QUIET_S)

        assert joins(phoenix_server) == REJOINED_ONCE


@each_protocol
async def test_a_server_channel_close_loses_the_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[Message]
) -> None:
    lost = LostTopics()
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_topic_lost=lost
    )
    async with client:
        await join_channel(client, TOPIC, received.put)

        await phoenix_server.close_channel(TOPIC)
        assert await wait_for_condition(lambda: bool(lost.lost))
        await asyncio.sleep(QUIET_S)

        lost.only_error()
        assert TOPIC not in client.topic_subscriptions.copy()
        assert joins(phoenix_server) == SUBSCRIBED
        assert received.empty()


async def test_our_own_leave_does_not_report_a_lost_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[Message]
) -> None:
    lost = LostTopics()
    async with make_client(phoenix_server, on_topic_lost=lost) as client:
        await join_channel(client, TOPIC)
        await join_channel(client, OTHER_TOPIC, received.put)

        await channel_for(client, TOPIC).leave()
        # Frames arrive in order, so the leave's phx_close was routed before this.
        await deliver(phoenix_server, client, OTHER_TOPIC)
        await expect_message(received)

        assert not lost.lost


async def test_is_topic_joined_is_false_while_disconnected(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await join_channel(client, TOPIC)
        phoenix_server.handshake_gate.clear()

        await phoenix_server.close_all_clients()
        assert await wait_for_condition(lambda: client.connection is None)

        assert TOPIC in client.topic_subscriptions.copy()
        assert not is_joined(client, TOPIC)
        phoenix_server.handshake_gate.set()
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))


async def test_resubscribing_from_on_topic_lost_works(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[Message]
) -> None:
    async def resubscribe(topic: str, error: Exception) -> None:
        del error
        await join_channel(client, topic, received.put)

    client = make_client(phoenix_server, on_topic_lost=resubscribe)
    async with client:
        await join_channel(client, TOPIC, received.put)

        await phoenix_server.close_channel(TOPIC)
        assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_ONCE)
        assert await wait_for_condition(lambda: is_joined(client, TOPIC))

        await deliver(phoenix_server, client)
        assert (await expect_message(received)).event == EVENT


@pytest.mark.parametrize(
    "event",
    [
        SystemEvent.error,
        SystemEvent.close,
        "phx_error",
        "phx_close",
    ],
)
async def test_lifecycle_event_handlers_are_rejected(
    client: Client, event: str | SystemEvent
) -> None:
    async def handler(payload: Payload) -> None:
        del payload

    await join_channel(client, TOPIC)

    with pytest.raises(ValueError, match=str(event)):
        channel_for(client, TOPIC).on(event, handler)
