from __future__ import annotations

import asyncio
import logging

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.exceptions import PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage, PHXEvent
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import FakePhoenixServer, WireEvent
from tests.support import (
    ASYNC_TIMEOUT_S,
    EVENT,
    FAST_RECONNECT,
    HEARTBEAT_INTERVAL_S,
    JOIN_TIMEOUT_S,
    OTHER_TOPIC,
    TOPIC,
    LostTopics,
    ReconnectCounter,
    deliver,
    derive_policy,
    each_protocol,
    expect_message,
    make_client,
    reconnect,
    rejoin_settled,
    wait_for_condition,
)

# Several heartbeats long, so the outage before the first rejoin is observable.
SLOW_RECOVERY = derive_policy(FAST_RECONNECT, base_delay_s=0.3, max_delay_s=0.3)

# Longer than any FAST_RECONNECT backoff plus a local join, so a retry that
# should not happen would have happened by then.
QUIET_S = 0.2

# A leave the server never answers would wait this long; a dead channel's
# unsubscribe must finish far sooner.
SLOW_LEAVE_TIMEOUT_S = ASYNC_TIMEOUT_S
PROMPT_S = SLOW_LEAVE_TIMEOUT_S / 4

# Join counts for one topic: the subscribe's join, then each rejoin after it.
SUBSCRIBED = 1
REJOINED_ONCE = 2
REJOINED_TWICE = 3

# Only ever seen in a delivered payload, so a log line carrying it leaked one.
PAYLOAD_MARKER = "payload-marker"


def joins(server: FakePhoenixServer, topic: str = TOPIC) -> int:
    return server.join_topics.count(topic)


def only_client_id(server: FakePhoenixServer) -> int:
    (client_id,) = server.current_client_ids()
    return client_id


class HeldCallback:
    """A topic callback that records each event and holds the first until released."""

    def __init__(self) -> None:
        self.events: list[object] = []
        self.running = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, message: ChannelMessage) -> None:
        self.events.append(message.event)
        self.running.set()
        await self.release.wait()


def hold_rejoins(server: FakePhoenixServer) -> int:
    """Leave the connected client's next joins unanswered; return its id."""
    client_id = only_client_id(server)
    server.unanswered_join_ids.add(client_id)
    return client_id


async def crash_with_rejoin_in_flight(
    server: FakePhoenixServer, client: PHXChannelsClient
) -> None:
    """Crash TOPIC's channel and wait until its rejoin is sent and unanswered."""
    hold_rejoins(server)
    await server.crash_channel(TOPIC)
    assert await wait_for_condition(lambda: joins(server) == REJOINED_ONCE)
    assert not client.is_topic_joined(TOPIC)


@each_protocol
async def test_a_crashed_channel_rejoins_on_the_live_socket(
    phoenix_server: FakePhoenixServer,
    received: asyncio.Queue[ChannelMessage],
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
        await client.subscribe_to_topic(TOPIC, received.put)
        await deliver(phoenix_server, client, payload={"marker": PAYLOAD_MARKER})
        await expect_message(received)
        socket = client.connection

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(
            lambda: not client.is_topic_joined(TOPIC), interval_s=0
        )
        acks_when_lost = len(acks)
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))

        assert len(acks) > acks_when_lost
        assert joins(phoenix_server) == REJOINED_ONCE
        assert client.connection is socket
        await deliver(phoenix_server, client)
        # The callback's next message is this one, so it never saw the phx_error.
        assert (await expect_message(received)).event == EVENT
        assert reconnects.count == 0

    assert f"Recovered channel for topic {TOPIC}" in caplog.text
    assert PAYLOAD_MARKER not in caplog.text


@each_protocol
async def test_a_repeated_channel_error_starts_one_recovery(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=SLOW_RECOVERY) as client:
        await client.subscribe_to_topic(TOPIC)
        stored_join_ref = phoenix_server.members[
            (only_client_id(phoenix_server), TOPIC)
        ]

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(lambda: not client.is_topic_joined(TOPIC))
        await phoenix_server.simulate_server_event(
            TOPIC, WireEvent.ERROR, {}, join_ref=stored_join_ref, ref=stored_join_ref
        )
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))
        await asyncio.sleep(SLOW_RECOVERY.max_delay_s)

        assert joins(phoenix_server) == REJOINED_ONCE


@pytest.mark.parametrize("protocol", [PhoenixChannelsProtocolVersion.V2])
async def test_a_stale_channel_error_is_ignored(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        old_join_ref = client.get_current_subscriptions()[TOPIC].join_ref
        await reconnect(phoenix_server, client)
        assert await wait_for_condition(rejoin_settled(client))

        await phoenix_server.simulate_server_event(
            TOPIC, WireEvent.ERROR, {}, join_ref=old_join_ref, ref=old_join_ref
        )
        # Frames arrive in order, so the stale error was handled before this.
        await deliver(phoenix_server, client)
        assert (await expect_message(received)).event == EVENT
        await asyncio.sleep(QUIET_S)

        assert client.is_topic_joined(TOPIC)
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
        await client.subscribe_to_topic(TOPIC)
        phoenix_server.fail_join_targets.add((only_client_id(phoenix_server), TOPIC))

        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(lambda: bool(lost.lost))
        await asyncio.sleep(QUIET_S)

        [(topic, error)] = lost.lost
        assert topic == TOPIC
        assert isinstance(error, PHXTopicError)
        assert TOPIC not in client.get_current_subscriptions()
        assert joins(phoenix_server) == REJOINED_ONCE
    assert f"Lost topic {TOPIC}" in caplog.text


@each_protocol
async def test_a_timed_out_channel_rejoin_retries_with_backoff(
    phoenix_server: FakePhoenixServer,
) -> None:
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, join_timeout_s=JOIN_TIMEOUT_S
    )
    async with client:
        await client.subscribe_to_topic(TOPIC)
        client_id = hold_rejoins(phoenix_server)
        await phoenix_server.crash_channel(TOPIC)
        assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_ONCE)

        phoenix_server.unanswered_join_ids.discard(client_id)

        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))
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
        await client.subscribe_to_topic(TOPIC)
        await crash_with_rejoin_in_flight(phoenix_server, client)

        await asyncio.wait_for(client.unsubscribe_from_topic(TOPIC), PROMPT_S)
        await asyncio.sleep(QUIET_S)

        assert TOPIC not in client.get_current_subscriptions()
        assert joins(phoenix_server) == REJOINED_ONCE
        assert not lost.lost


@each_protocol
async def test_a_socket_drop_during_recovery_hands_the_topic_to_the_reconnect(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC)
        await crash_with_rejoin_in_flight(phoenix_server, client)

        # The new connection's joins are answered.
        await reconnect(phoenix_server, client)
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))
        await asyncio.sleep(QUIET_S)

        assert joins(phoenix_server) == REJOINED_TWICE


async def test_a_channel_error_queued_behind_a_callback_does_not_double_rejoin(
    phoenix_server: FakePhoenixServer,
) -> None:
    held = HeldCallback()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        # Outlasts the test, so the drain waits for the release below.
        callback_drain_timeout_s=ASYNC_TIMEOUT_S,
    )
    async with client:
        await client.subscribe_to_topic(TOPIC, held)
        await deliver(phoenix_server, client)
        await asyncio.wait_for(held.running.wait(), ASYNC_TIMEOUT_S)
        await phoenix_server.crash_channel(TOPIC)
        subscription = client.get_current_subscriptions()[TOPIC]
        assert await wait_for_condition(lambda: not subscription.queue.empty())

        # Nothing yields from the reconnect until the topic's drain, so the
        # queued phx_error is handled during the socket rejoin.
        await reconnect(phoenix_server, client)
        held.release.set()
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))
        await asyncio.sleep(QUIET_S)

        assert joins(phoenix_server) == REJOINED_ONCE


@each_protocol
async def test_a_server_channel_close_loses_the_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    lost = LostTopics()
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_topic_lost=lost
    )
    async with client:
        await client.subscribe_to_topic(TOPIC, received.put)

        await phoenix_server.close_channel(TOPIC)
        assert await wait_for_condition(lambda: bool(lost.lost))
        await asyncio.sleep(QUIET_S)

        [(topic, error)] = lost.lost
        assert topic == TOPIC
        assert isinstance(error, PHXTopicError)
        assert TOPIC not in client.get_current_subscriptions()
        assert joins(phoenix_server) == SUBSCRIBED
        assert received.empty()


async def test_our_own_leave_does_not_report_a_lost_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    lost = LostTopics()
    async with make_client(phoenix_server, on_topic_lost=lost) as client:
        await client.subscribe_to_topic(TOPIC)
        await client.subscribe_to_topic(OTHER_TOPIC, received.put)

        await client.unsubscribe_from_topic(TOPIC)
        # Frames arrive in order, so the leave's phx_close was routed before this.
        await deliver(phoenix_server, client, OTHER_TOPIC)
        await expect_message(received)

        assert not lost.lost


async def test_is_topic_joined_is_false_while_disconnected(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC)
        phoenix_server.handshake_gate.clear()

        await phoenix_server.close_all_clients()
        assert await wait_for_condition(lambda: client.connection is None)

        assert TOPIC in client.get_current_subscriptions()
        assert not client.is_topic_joined(TOPIC)
        phoenix_server.handshake_gate.set()
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))


async def test_resubscribing_from_on_topic_lost_works(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async def resubscribe(topic: str, error: Exception) -> None:
        del error
        await client.subscribe_to_topic(topic, received.put)

    client = make_client(phoenix_server, on_topic_lost=resubscribe)
    async with client:
        await client.subscribe_to_topic(TOPIC, received.put)

        await phoenix_server.close_channel(TOPIC)
        assert await wait_for_condition(lambda: joins(phoenix_server) == REJOINED_ONCE)
        assert await wait_for_condition(lambda: client.is_topic_joined(TOPIC))

        await deliver(phoenix_server, client)
        assert (await expect_message(received)).event == EVENT


@pytest.mark.parametrize("event", [PHXEvent.error, PHXEvent.close])
async def test_lifecycle_event_handlers_are_rejected(
    client: PHXChannelsClient, event: PHXEvent
) -> None:
    async def handler(payload: dict[str, object]) -> None:
        del payload

    await client.subscribe_to_topic(TOPIC)

    with pytest.raises(ValueError, match=str(event)):
        client.add_event_handler(TOPIC, event, handler)
