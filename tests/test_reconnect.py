from __future__ import annotations

import asyncio

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError, PHXTopicError
from phoenix_channels_python_client.phx_messages import ChannelMessage
from tests.fake_server import FakePhoenixServer
from tests.support import (
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    JOIN_TIMEOUT_S,
    LEAVE_TIMEOUT_S,
    OTHER_TOPIC,
    STOP_REASON,
    TOPIC,
    UNPARSEABLE_FRAME,
    ReconnectCounter,
    deliver,
    each_protocol,
    expect_message,
    make_client,
    reconnect,
    reconnect_after,
    rejoin_settled,
    wait_for_condition,
    wait_forever,
)

# Short, so a callback that is never released gets cancelled promptly.
CALLBACK_DRAIN_TIMEOUT_S = 0.05

# Long enough to tell apart from FAST_RECONNECT's other delays.
TRY_AGAIN_LATER_MIN_DELAY_S = 0.2
TRY_AGAIN_LATER_MAX_DELAY_S = 0.25

SUPPRESS_AFTER_DISCONNECTS = 3

# A private-use close code (4000-4999) the client has no rule for.
UNRECOGNISED_CLOSE_CODE = 4001


class CallbackFailure(Exception):
    pass


class BusyCallback:
    """A topic callback that stays busy until released, then fails."""

    def __init__(self) -> None:
        self.running = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, message: ChannelMessage) -> None:
        self.running.set()
        await self.release.wait()
        raise CallbackFailure


async def reconnect_while_draining(
    server: FakePhoenixServer, client: PHXChannelsClient, busy: BusyCallback
) -> None:
    await deliver(server, client)
    await asyncio.wait_for(busy.running.wait(), ASYNC_TIMEOUT_S)
    # Nothing yields from the reconnect until the first topic's drain.
    await reconnect(server, client)


async def assert_delivers_after_rejoin(
    server: FakePhoenixServer,
    client: PHXChannelsClient,
    topic: str,
    received: asyncio.Queue[ChannelMessage],
) -> None:
    assert await wait_for_condition(rejoin_settled(client, topic))
    await deliver(server, client, topic)
    await expect_message(received)


@each_protocol
async def test_a_service_restart_reconnects_and_rejoins_the_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, received.put)

        await reconnect(phoenix_server, client)

        await assert_delivers_after_rejoin(phoenix_server, client, TOPIC, received)


async def test_with_auto_reconnect_disabled_a_service_restart_disconnects(
    phoenix_server: FakePhoenixServer,
) -> None:
    async with make_client(phoenix_server, auto_reconnect=False) as client:
        await client.subscribe_to_topic(TOPIC)

        await phoenix_server.close_all_clients(code=CloseCode.SERVICE_RESTART)

        assert await wait_for_condition(lambda: client.connection is None)


async def test_try_again_later_holds_the_reconnect_for_its_cooldown(
    phoenix_server: FakePhoenixServer,
) -> None:
    policy = ReconnectPolicy.model_validate(
        FAST_RECONNECT.model_dump()
        | {
            "try_again_later_min_delay_s": TRY_AGAIN_LATER_MIN_DELAY_S,
            "try_again_later_max_delay_s": TRY_AGAIN_LATER_MAX_DELAY_S,
        }
    )
    async with make_client(phoenix_server, reconnect_policy=policy) as client:
        await client.subscribe_to_topic(TOPIC)
        loop = asyncio.get_running_loop()
        closed_at = loop.time()

        await reconnect(phoenix_server, client, code=CloseCode.TRY_AGAIN_LATER)

        assert loop.time() - closed_at >= TRY_AGAIN_LATER_MIN_DELAY_S


@each_protocol
async def test_messages_queued_before_a_reconnect_are_dropped(
    phoenix_server: FakePhoenixServer,
) -> None:
    handled: list[object] = []
    first_started = asyncio.Event()

    async def stuck_on_the_first_message(message: ChannelMessage) -> None:
        handled.append(message.payload["id"])
        if not first_started.is_set():
            first_started.set()
            await wait_forever()

    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        callback_drain_timeout_s=CALLBACK_DRAIN_TIMEOUT_S,
    )
    async with client:
        await client.subscribe_to_topic(TOPIC, stuck_on_the_first_message)
        first, queued, after_reconnect = 1, 2, 3
        await deliver(phoenix_server, client, payload={"id": first})
        await asyncio.wait_for(first_started.wait(), ASYNC_TIMEOUT_S)
        await deliver(phoenix_server, client, payload={"id": queued})

        await reconnect(phoenix_server, client)
        assert await wait_for_condition(rejoin_settled(client))
        await deliver(phoenix_server, client, payload={"id": after_reconnect})

        assert await wait_for_condition(lambda: after_reconnect in handled)
        assert handled == [first, after_reconnect]


@each_protocol
async def test_a_rejected_rejoin_unregisters_only_that_topic(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, received.put)
        await client.subscribe_to_topic(OTHER_TOPIC)
        phoenix_server.fail_join_targets.add(
            (phoenix_server.next_client_id, OTHER_TOPIC)
        )

        await reconnect(phoenix_server, client)

        # The failed join settles before the rejoin unregisters the topic.
        assert await wait_for_condition(
            lambda: OTHER_TOPIC not in client.get_current_subscriptions()
        )
        await assert_delivers_after_rejoin(phoenix_server, client, TOPIC, received)


@each_protocol
async def test_a_rejoin_that_times_out_keeps_the_topic_for_the_next_reconnect(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, join_timeout_s=JOIN_TIMEOUT_S
    )
    async with client:
        await client.subscribe_to_topic(TOPIC, received.put)
        phoenix_server.unanswered_join_ids.add(phoenix_server.next_client_id)

        await reconnect(phoenix_server, client)
        assert await wait_for_condition(rejoin_settled(client))
        assert TOPIC in client.get_current_subscriptions()

        await reconnect(phoenix_server, client)
        await assert_delivers_after_rejoin(phoenix_server, client, TOPIC, received)


async def test_unsubscribing_during_a_rejoin_drain_keeps_the_client_running(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    busy = BusyCallback()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        leave_timeout_s=LEAVE_TIMEOUT_S,
        # Outlasts the leave, so the unsubscribe cancels the callback mid-drain.
        callback_drain_timeout_s=ASYNC_TIMEOUT_S,
    )

    async with client:
        await client.subscribe_to_topic(TOPIC, busy)
        await client.subscribe_to_topic(OTHER_TOPIC, received.put)
        await reconnect_while_draining(phoenix_server, client, busy)
        run = asyncio.create_task(client.run_forever(install_signal_handlers=False))

        with pytest.raises(PHXTopicError):
            await client.unsubscribe_from_topic(TOPIC)

        await assert_delivers_after_rejoin(
            phoenix_server, client, OTHER_TOPIC, received
        )
        assert TOPIC not in client.get_current_subscriptions()

    assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None


async def test_a_callback_failing_while_the_rejoin_drains_it_does_not_stop_the_rejoin(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    busy = BusyCallback()
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, busy)
        await client.subscribe_to_topic(OTHER_TOPIC, received.put)
        await reconnect_while_draining(phoenix_server, client, busy)

        busy.release.set()

        assert await wait_for_condition(rejoin_settled(client))
        assert TOPIC in client.get_current_subscriptions()
        await assert_delivers_after_rejoin(
            phoenix_server, client, OTHER_TOPIC, received
        )


async def test_a_callback_outlasting_the_drain_is_cancelled_and_the_topic_rejoins(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    busy = BusyCallback()
    client = make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        callback_drain_timeout_s=CALLBACK_DRAIN_TIMEOUT_S,
    )

    async with client:
        await client.subscribe_to_topic(TOPIC, busy)
        await client.subscribe_to_topic(OTHER_TOPIC, received.put)
        await reconnect_while_draining(phoenix_server, client, busy)

        assert await wait_for_condition(rejoin_settled(client))
        busy.running.clear()
        await deliver(phoenix_server, client)
        await asyncio.wait_for(busy.running.wait(), ASYNC_TIMEOUT_S)
        await assert_delivers_after_rejoin(
            phoenix_server, client, OTHER_TOPIC, received
        )


async def test_a_subscribe_interrupted_by_a_disconnect_fails_fast(
    phoenix_server: FakePhoenixServer,
) -> None:
    # The default join timeout outlasts ASYNC_TIMEOUT_S, so only failing fast passes.
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        phoenix_server.unanswered_join_ids.update(phoenix_server.current_client_ids())
        subscribe = asyncio.create_task(client.subscribe_to_topic(TOPIC))
        assert await wait_for_condition(
            lambda: TOPIC in client.get_current_subscriptions()
        )

        await reconnect(phoenix_server, client)

        with pytest.raises(PHXConnectionError):
            await asyncio.wait_for(subscribe, ASYNC_TIMEOUT_S)
        assert TOPIC not in client.get_current_subscriptions()
        await client.subscribe_to_topic(TOPIC)
        # The first subscribe's join and the retry's; the rejoin sent none.
        assert phoenix_server.join_topics == [TOPIC, TOPIC]


async def test_shutdown_during_a_rejoin_does_not_report_a_reconnect(
    phoenix_server: FakePhoenixServer,
) -> None:
    reconnects = ReconnectCounter()
    client = make_client(
        phoenix_server, reconnect_policy=FAST_RECONNECT, on_reconnect=reconnects
    )
    async with client:
        await client.subscribe_to_topic(TOPIC)
        subscription = client.get_current_subscriptions()[TOPIC]
        joined_on = subscription.conn_generation
        phoenix_server.unanswered_join_ids.add(phoenix_server.next_client_id)

        await phoenix_server.close_all_clients()
        # Set just before the rejoin sends its join, so it now waits on the reply.
        assert await wait_for_condition(
            lambda: subscription.conn_generation > joined_on
        )
        await client.shutdown(STOP_REASON)

    assert reconnects.count == 0


@each_protocol
@pytest.mark.parametrize("install_signal_handlers", [True, False])
async def test_rapid_disconnects_suppress_reconnecting_and_fail_run_forever(
    phoenix_server: FakePhoenixServer, install_signal_handlers: bool
) -> None:
    policy = ReconnectPolicy.model_validate(
        FAST_RECONNECT.model_dump()
        | {
            # Long enough that every drop right after a join counts as rapid.
            "rapid_disconnect_uptime_s": 1.0,
            "rapid_window_s": 2.0,
            "rapid_suppress_disconnect_count": SUPPRESS_AFTER_DISCONNECTS,
        }
    )
    # Every connection up to suppression drops right after its join.
    phoenix_server.close_on_join_ids.update(range(1, 2 * SUPPRESS_AFTER_DISCONNECTS))

    async with make_client(phoenix_server, reconnect_policy=policy) as client:
        await client.subscribe_to_topic(TOPIC)

        with pytest.raises(PHXConnectionError, match="Reconnect suppressed"):
            await asyncio.wait_for(
                client.run_forever(install_signal_handlers=install_signal_handlers),
                ASYNC_TIMEOUT_S,
            )


@each_protocol
async def test_a_policy_violation_close_stops_reconnecting_with_a_terminal_error(
    phoenix_server: FakePhoenixServer,
) -> None:
    phoenix_server.close_on_join_ids.add(phoenix_server.next_client_id)
    phoenix_server.close_on_join_code = CloseCode.POLICY_VIOLATION

    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC)

        with pytest.raises(
            PHXConnectionError,
            match=f"terminal close code {int(CloseCode.POLICY_VIOLATION)}",
        ):
            await asyncio.wait_for(client.run_forever(), ASYNC_TIMEOUT_S)


@each_protocol
async def test_a_normal_close_does_not_reconnect_by_default(
    phoenix_server: FakePhoenixServer,
) -> None:
    phoenix_server.close_on_join_ids.add(phoenix_server.next_client_id)
    phoenix_server.close_on_join_code = CloseCode.NORMAL_CLOSURE

    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC)
        await asyncio.wait_for(client.run_forever(), ASYNC_TIMEOUT_S)

    assert phoenix_server.get_connection_attempts(FakePhoenixServer.SOCKET_PATH) == 1


@each_protocol
async def test_an_unrecognised_close_code_reconnects(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, received.put)

        await reconnect(phoenix_server, client, code=UNRECOGNISED_CLOSE_CODE)

        await assert_delivers_after_rejoin(phoenix_server, client, TOPIC, received)


@each_protocol
async def test_a_frame_the_client_cannot_parse_triggers_a_reconnect(
    phoenix_server: FakePhoenixServer, received: asyncio.Queue[ChannelMessage]
) -> None:
    async with make_client(phoenix_server, reconnect_policy=FAST_RECONNECT) as client:
        await client.subscribe_to_topic(TOPIC, received.put)

        await reconnect_after(client, phoenix_server.send_raw(UNPARSEABLE_FRAME))

        await assert_delivers_after_rejoin(phoenix_server, client, TOPIC, received)
