"""A consumer-owned reconnect policy: ``reconnect_decider`` and ``on_connected``."""

from __future__ import annotations

import asyncio
import math
from http import HTTPStatus
from typing import Any

import pytest
from websockets.exceptions import InvalidStatus
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient
from phoenix_channels_python_client.client_types import (
    ConnectFailed,
    Connecting,
    Disconnected,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer
from tests.support import (
    ASYNC_TIMEOUT_S,
    FAST_RECONNECT,
    REJOINED_ONCE,
    STOP,
    STOP_REASON,
    SUBSCRIBED,
    TERMINAL_REFUSAL,
    TOPIC,
    CallbackError,
    ReconnectCounter,
    RecordingDecider,
    derive_policy,
    joins,
    make_client,
    reconnect,
)

SOCKET_PATH = FakePhoenixServer.SOCKET_PATH

CLOSE_REASON = "going away"

# A private-use close code (4000-4999) the client has no rule for.
UNRECOGNISED_CLOSE_CODE = 4001

# Longer than any backoff the default policy computes for a first retry.
LONG_DELAY_S = 10.0

SUPPRESS_AFTER_DISCONNECTS = 3

# Every decision point the decider is asked at.
EACH_EVENT_KIND = pytest.mark.parametrize(
    "kind", [Connecting, Disconnected, ConnectFailed]
)


def terminal(reason: str = "decided terminal") -> ReconnectDecision:
    return ReconnectDecision(
        should_reconnect=False, terminal_error=PHXConnectionError(reason)
    )


def attempts(server: FakePhoenixServer) -> int:
    return server.get_connection_attempts(SOCKET_PATH)


async def test_every_attempt_is_announced_before_it_connects(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    async with make_client(phoenix_server, reconnect_decider=decider) as client:
        await reconnect(phoenix_server, client)

    assert [type(event) for event in decider.seen] == [
        Connecting,
        Disconnected,
        Connecting,
    ]


@pytest.mark.parametrize("stop_with_error", [False, True])
async def test_a_stop_before_the_next_attempt_makes_no_connection(
    phoenix_server: FakePhoenixServer, *, stop_with_error: bool
) -> None:
    decider = RecordingDecider()
    stop = terminal() if stop_with_error else STOP
    async with make_client(phoenix_server, reconnect_decider=decider) as client:
        # What a consumer does when it learns during the backoff that the
        # session is over: the close itself was still decided as a reconnect.
        decider.answers[Connecting] = stop
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)

        run = client.run_forever(install_signal_handlers=False)
        if stop_with_error:
            with pytest.raises(PHXConnectionError) as raised:
                await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
            assert raised.value is stop.terminal_error
        else:
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

    assert attempts(phoenix_server) == 1


async def test_a_terminal_stop_before_the_first_attempt_raises_from_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    stop = terminal()
    decider.answers[Connecting] = stop
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(PHXConnectionError) as raised:
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

    assert raised.value is stop.terminal_error
    assert attempts(phoenix_server) == 0


async def test_a_plain_stop_before_the_first_attempt_fails_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    decider.answers[Connecting] = STOP
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(PHXConnectionError, match="stopped before connecting"):
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

    assert attempts(phoenix_server) == 0


@pytest.mark.parametrize(
    "close_code",
    [CloseCode.NORMAL_CLOSURE, CloseCode.GOING_AWAY, UNRECOGNISED_CLOSE_CODE],
)
async def test_a_close_is_decided_with_its_code_and_reason(
    phoenix_server: FakePhoenixServer, close_code: int
) -> None:
    decider = RecordingDecider()
    async with make_client(phoenix_server, reconnect_decider=decider):
        await phoenix_server.close_all_clients(code=close_code, reason=CLOSE_REASON)

        event, _ = await decider.next_of(Disconnected)

    assert event == Disconnected(close_code=close_code, close_reason=CLOSE_REASON)


@pytest.mark.clock
@pytest.mark.parametrize("delay_s", [0.0, LONG_DELAY_S])
async def test_a_close_waits_exactly_the_decided_delay(
    clock: Clock, clocked_peer: FakePhoenixServer, delay_s: float
) -> None:
    decider = RecordingDecider()
    decider.answers[Disconnected] = ReconnectDecision(
        should_reconnect=True, delay_s=delay_s
    )
    async with make_client(clocked_peer, reconnect_decider=decider):
        await decider.next_of(Connecting)
        await clocked_peer.close_all_clients(code=CloseCode.GOING_AWAY)
        _, closed_at = await decider.next_of(Disconnected)

        await clock.advance(delay_s)
        _, attempted_at = await decider.next_of(Connecting)

    assert attempted_at - closed_at == delay_s


@pytest.mark.clock
@pytest.mark.parametrize("delay_s", [0.0, LONG_DELAY_S])
async def test_a_failed_connect_waits_exactly_the_decided_delay(
    clock: Clock, clocked_peer: FakePhoenixServer, delay_s: float
) -> None:
    decider = RecordingDecider()
    decider.answers[ConnectFailed] = ReconnectDecision(
        should_reconnect=True, delay_s=delay_s
    )
    client = make_client(clocked_peer, reconnect_decider=decider)
    with clocked_peer.refusing(1):
        entering = asyncio.create_task(client.__aenter__())
        _, failed_at = await decider.next_of(ConnectFailed)

        await clock.advance(delay_s)
        _, attempted_at = await decider.next_of(Connecting)
    await entering
    await client.__aexit__(None, None, None)

    assert attempted_at - failed_at == delay_s


async def test_a_terminal_close_decision_raises_from_run_forever(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    stop = terminal()
    decider.answers[Disconnected] = stop
    async with make_client(phoenix_server, reconnect_decider=decider) as client:
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)

        with pytest.raises(PHXConnectionError) as raised:
            await asyncio.wait_for(
                client.run_forever(install_signal_handlers=False), ASYNC_TIMEOUT_S
            )

    assert raised.value is stop.terminal_error
    assert attempts(phoenix_server) == 1


async def test_a_stop_after_a_forced_close_ends_the_client(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    decider.answers[Disconnected] = STOP
    async with make_client(phoenix_server, reconnect_decider=decider) as client:
        await client.close_connection(STOP_REASON)

        assert (
            await asyncio.wait_for(
                client.run_forever(install_signal_handlers=False), ASYNC_TIMEOUT_S
            )
            is None
        )

    assert Disconnected(close_code=None, close_reason="") in decider.seen
    assert attempts(phoenix_server) == 1


@pytest.mark.parametrize("refusal", [TERMINAL_REFUSAL, HTTPStatus.SERVICE_UNAVAILABLE])
async def test_a_refused_handshake_is_decided_with_the_raw_response(
    phoenix_server: FakePhoenixServer, refusal: HTTPStatus
) -> None:
    decider = RecordingDecider()
    client = make_client(phoenix_server, reconnect_decider=decider)

    # The decider retries even the 403 the built-in policy treats as terminal.
    with phoenix_server.refusing(1, refusal):
        async with client:
            pass

    event, _ = await decider.next_of(ConnectFailed)
    assert isinstance(event.error, InvalidStatus)
    assert event.error.response.status_code == refusal
    assert phoenix_server.refused_handshakes == 1


async def run_until_stopped(
    server: FakePhoenixServer, client: PHXChannelsClient, kind: type[ReconnectEvent]
) -> None:
    """Enter, then close the socket and run, reaching each decision point once."""
    with server.refusing(1 if kind is ConnectFailed else 0):
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)
    await server.close_all_clients(code=CloseCode.GOING_AWAY)
    await asyncio.wait_for(
        client.run_forever(install_signal_handlers=False), ASYNC_TIMEOUT_S
    )


@EACH_EVENT_KIND
async def test_a_failing_decider_stops_the_client_with_its_cause(
    phoenix_server: FakePhoenixServer, kind: type[ReconnectEvent]
) -> None:
    decider = RecordingDecider()
    failure = CallbackError()
    decider.answers[kind] = failure
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(PHXConnectionError) as raised:
        await run_until_stopped(phoenix_server, client, kind)
    await client.shutdown(STOP_REASON)

    assert raised.value.__cause__ is failure
    assert sum(isinstance(event, kind) for event in decider.seen) == 1


async def test_a_decider_replaces_rapid_disconnect_suppression(
    phoenix_server: FakePhoenixServer,
) -> None:
    policy = derive_policy(
        FAST_RECONNECT, rapid_suppress_disconnect_count=SUPPRESS_AFTER_DISCONNECTS
    )
    refusals = 2 * SUPPRESS_AFTER_DISCONNECTS
    client = make_client(
        phoenix_server, reconnect_policy=policy, reconnect_decider=RecordingDecider()
    )

    with phoenix_server.refusing(refusals):
        async with client:
            for _ in range(refusals):
                await reconnect(phoenix_server, client)

    assert phoenix_server.refused_handshakes == refusals


async def test_without_auto_reconnect_the_decider_is_not_asked(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    async with make_client(
        phoenix_server, auto_reconnect=False, reconnect_decider=decider
    ) as client:
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)
        await asyncio.wait_for(
            client.run_forever(install_signal_handlers=False), ASYNC_TIMEOUT_S
        )

    assert decider.seen == []


@pytest.mark.parametrize(
    "fields",
    [
        {"delay_s": -1.0},
        {"delay_s": math.nan},
        {"delay_s": math.inf},
        {"delay_s": 1.0, "min_delay_s": 1.0},
        {"delay_s": 1.0, "max_delay_s": 2.0},
    ],
)
def test_an_invalid_decision_is_rejected(fields: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="delay_s"):
        ReconnectDecision(should_reconnect=True, **fields)


@pytest.mark.parametrize("failing", [False, True])
async def test_on_connected_reports_each_socket_before_its_rejoin(
    phoenix_server: FakePhoenixServer, *, failing: bool
) -> None:
    connected: list[tuple[int, int, int]] = []
    reconnects = ReconnectCounter()

    def on_connected(generation: int) -> None:
        connected.append((generation, joins(phoenix_server), reconnects.count))
        if failing:
            raise CallbackError

    async with make_client(
        phoenix_server,
        reconnect_policy=FAST_RECONNECT,
        on_connected=on_connected,
        on_reconnect=reconnects,
    ) as client:
        await client.subscribe_to_topic(TOPIC)
        await reconnect(phoenix_server, client)

    # Each entry: (generation, joins seen so far, on_reconnect calls so far).
    assert connected == [(1, 0, 0), (2, SUBSCRIBED, 0)]
    assert joins(phoenix_server) == REJOINED_ONCE
    assert reconnects.count == 1
