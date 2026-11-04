"""A consumer-owned reconnect policy: ``reconnect_decider``."""

from __future__ import annotations

import asyncio
import math
from http import HTTPStatus
from typing import Any

import pytest
from websockets.exceptions import InvalidStatus
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.client_types import (
    Connected,
    ConnectFailed,
    Connecting,
    Disconnected,
    ReconnectDecision,
    ReconnectEvent,
)
from phoenix_channels_python_client.exceptions import SocketError
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
    wait_for_condition,
)

SOCKET_PATH = FakePhoenixServer.SOCKET_PATH

CLOSE_REASON = "going away"

# A private-use close code (4000-4999) the client has no rule for.
UNRECOGNISED_CLOSE_CODE = 4001

# Longer than any backoff the default policy computes for a first retry.
LONG_DELAY_S = 10.0

SUPPRESS_AFTER_DISCONNECTS = 3

# A suppression threshold two entries with one rapid failure each would reach.
RAPID_FAILURES_TO_STOP = 2
RAPID_HISTORY_WINDOW_S = 60.0

# Connection attempts: the first socket, then the one reconnected after its close.
RECONNECTED_ONCE = 2

# Every decision point the decider is asked at.
EACH_EVENT_KIND = pytest.mark.parametrize(
    "kind", [Connecting, Connected, Disconnected, ConnectFailed]
)

# The decision points that only proceed or stop.
EACH_GATE = pytest.mark.parametrize("gate", [Connecting, Connected])

A_DELAY = ReconnectDecision(should_reconnect=True, delay_s=1.0)


def terminal(reason: str = "decided terminal") -> ReconnectDecision:
    return ReconnectDecision(should_reconnect=False, terminal_error=SocketError(reason))


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
        Connected,
        Disconnected,
        Connecting,
        Connected,
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
            with pytest.raises(SocketError) as raised:
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

    with pytest.raises(SocketError) as raised:
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

    assert raised.value is stop.terminal_error
    assert attempts(phoenix_server) == 0


async def test_a_plain_stop_before_the_first_attempt_fails_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    decider.answers[Connecting] = STOP
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(SocketError, match="stopped before connecting"):
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

        with pytest.raises(SocketError) as raised:
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
    server: FakePhoenixServer, client: Client, kind: type[ReconnectEvent]
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

    with pytest.raises(SocketError) as raised:
        await run_until_stopped(phoenix_server, client, kind)
    await client.shutdown(STOP_REASON)

    assert raised.value.__cause__ is failure
    assert sum(isinstance(event, kind) for event in decider.seen) == 1


@EACH_EVENT_KIND
async def test_a_decider_returning_no_decision_stops_the_client(
    phoenix_server: FakePhoenixServer, kind: type[ReconnectEvent]
) -> None:
    decider = RecordingDecider()

    def forgetful(event: ReconnectEvent) -> ReconnectDecision:
        decision = decider(event)
        # A decider whose match falls through for this event returns None.
        return None if isinstance(event, kind) else decision  # type: ignore[return-value]

    client = make_client(phoenix_server, reconnect_decider=forgetful)

    with pytest.raises(SocketError, match="no ReconnectDecision"):
        await run_until_stopped(phoenix_server, client, kind)
    await client.shutdown(STOP_REASON)


@EACH_EVENT_KIND
async def test_an_async_decider_stops_the_client(
    phoenix_server: FakePhoenixServer, kind: type[ReconnectEvent]
) -> None:
    decider = RecordingDecider()

    def mistaken(event: ReconnectEvent) -> ReconnectDecision:
        decision = decider(event)
        if not isinstance(event, kind):
            return decision

        async def deciding() -> ReconnectDecision:
            return decision

        return deciding()  # type: ignore[return-value]

    client = make_client(phoenix_server, reconnect_decider=mistaken)

    with pytest.raises(SocketError, match="must be synchronous"):
        await run_until_stopped(phoenix_server, client, kind)
    await client.shutdown(STOP_REASON)


async def test_a_close_after_shutdown_began_is_not_decided(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    stopping: list[asyncio.Task[None]] = []

    async def on_disconnect(error: Exception | None) -> None:
        del error
        stopping.append(asyncio.create_task(client.shutdown(STOP_REASON)))
        # Lets the shutdown request land before the close is decided.
        await asyncio.sleep(0)

    client = make_client(
        phoenix_server, reconnect_decider=decider, on_disconnect=on_disconnect
    )
    async with client:
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)
        assert await wait_for_condition(lambda: bool(stopping))
        await asyncio.wait_for(stopping[0], ASYNC_TIMEOUT_S)

    assert not any(isinstance(event, Disconnected) for event in decider.seen)


async def test_each_entry_starts_the_built_in_policy_afresh(
    phoenix_server: FakePhoenixServer,
) -> None:
    # A window no test run outlasts, so only a fresh decider forgets a failure.
    policy = derive_policy(
        FAST_RECONNECT,
        rapid_suppress_disconnect_count=RAPID_FAILURES_TO_STOP,
        rapid_window_s=RAPID_HISTORY_WINDOW_S,
    )
    client = make_client(phoenix_server, reconnect_policy=policy)

    # One rapid failure per entry; carried over, the second would stop the client.
    for _ in range(RAPID_FAILURES_TO_STOP):
        with phoenix_server.refusing(1):
            async with client:
                pass

    assert phoenix_server.refused_handshakes == RAPID_FAILURES_TO_STOP


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
    ("fields", "problem"),
    [
        ({"should_reconnect": True, "delay_s": -1.0}, "finite"),
        ({"should_reconnect": True, "delay_s": math.nan}, "finite"),
        ({"should_reconnect": True, "delay_s": math.inf}, "finite"),
        ({"should_reconnect": False, "delay_s": 1.0}, "no delay_s"),
        (
            {"should_reconnect": True, "terminal_error": SocketError()},
            "terminal_error",
        ),
    ],
)
def test_an_invalid_decision_is_rejected(fields: dict[str, Any], problem: str) -> None:
    with pytest.raises(ValueError, match=problem):
        ReconnectDecision(**fields)


@EACH_GATE
async def test_a_delay_for_a_gate_stops_the_client(
    phoenix_server: FakePhoenixServer, gate: type[ReconnectEvent]
) -> None:
    decider = RecordingDecider()
    decider.answers[gate] = A_DELAY
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(SocketError, match="delay for a gate"):
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)


async def test_each_socket_is_decided_before_its_rejoin(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    reconnects = ReconnectCounter()
    # Each entry: (generation, joins seen so far, on_reconnect calls so far).
    connected: list[tuple[int, int, int]] = []

    def deciding(event: ReconnectEvent) -> ReconnectDecision:
        if isinstance(event, Connected):
            connected.append(
                (event.generation, joins(phoenix_server), reconnects.count)
            )
        return decider(event)

    async with make_client(
        phoenix_server, reconnect_decider=deciding, on_reconnect=reconnects
    ) as client:
        await client.subscribe_to_topic(TOPIC)
        await reconnect(phoenix_server, client)

    assert connected == [(1, 0, 0), (2, SUBSCRIBED, 0)]
    assert joins(phoenix_server) == REJOINED_ONCE
    assert reconnects.count == 1


@pytest.mark.parametrize("stop_with_error", [False, True])
async def test_a_refused_socket_is_closed_without_a_rejoin(
    phoenix_server: FakePhoenixServer, *, stop_with_error: bool
) -> None:
    decider = RecordingDecider()
    stop = terminal() if stop_with_error else STOP
    async with make_client(phoenix_server, reconnect_decider=decider) as client:
        await client.subscribe_to_topic(TOPIC)
        # What a consumer does when the session ended during the handshake.
        decider.answers[Connected] = stop
        await phoenix_server.close_all_clients(code=CloseCode.GOING_AWAY)

        run = client.run_forever(install_signal_handlers=False)
        if stop_with_error:
            with pytest.raises(SocketError) as raised:
                await asyncio.wait_for(run, ASYNC_TIMEOUT_S)
            assert raised.value is stop.terminal_error
        else:
            assert await asyncio.wait_for(run, ASYNC_TIMEOUT_S) is None

        assert await wait_for_condition(lambda: not phoenix_server.current_client_ids())

    assert attempts(phoenix_server) == RECONNECTED_ONCE
    assert joins(phoenix_server) == SUBSCRIBED


async def test_a_refused_first_socket_fails_entry(
    phoenix_server: FakePhoenixServer,
) -> None:
    decider = RecordingDecider()
    stop = terminal()
    decider.answers[Connected] = stop
    client = make_client(phoenix_server, reconnect_decider=decider)

    with pytest.raises(SocketError) as raised:
        await asyncio.wait_for(client.__aenter__(), ASYNC_TIMEOUT_S)

    assert raised.value is stop.terminal_error
    assert await wait_for_condition(lambda: not phoenix_server.current_client_ids())
