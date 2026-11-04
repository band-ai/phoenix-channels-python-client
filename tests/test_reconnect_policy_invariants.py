from __future__ import annotations

import logging
from collections.abc import Callable
from http import HTTPStatus
from itertools import pairwise
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import Client
from phoenix_channels_python_client.client_types import (
    Connected,
    ConnectFailed,
    Disconnected,
    ReconnectDecision,
    ReconnectPolicy,
)
from phoenix_channels_python_client.default_decider import (
    DefaultReconnectDecider,
    exponential_delay,
)
from phoenix_channels_python_client.exceptions import SocketError
from tests.clock import Clock
from tests.fake_server import FakePhoenixServer
from tests.support import (
    API_KEY,
    STOP_REASON,
    STOPS_ON_NORMAL_CLOSE,
    TERMINAL_REFUSAL,
    derive_policy,
    each_normal_close_code,
    each_retried_refusal,
    make_client,
    refused,
)

# No jitter, and every pre-cooldown delay stays below the cooldown base.
COOLDOWN_POLICY = ReconnectPolicy(
    base_delay_s=0.01,
    max_delay_s=0.1,
    rapid_first_min_delay_s=0.0,
    rapid_second_min_delay_s=0.0,
    rapid_cooldown_base_s=0.5,
    rapid_cooldown_step_s=0.25,
    rapid_cooldown_max_s=1.0,
    rapid_hold_down_jitter_low_ratio=1.0,
    rapid_hold_down_jitter_high_ratio=1.0,
)

# Enough rapid disconnects for COOLDOWN_POLICY's cooldown to reach its cap.
RAPID_COUNTS = range(1, 10)

JITTER_LOW_RATIO = 0.25
JITTER_HIGH_RATIO = 0.75

# No backoff of its own, so a close's delay is drawn only from its classified range.
NO_BACKOFF = ReconnectPolicy(
    base_delay_s=0.0, rapid_first_min_delay_s=0.0, rapid_second_min_delay_s=0.0
)

SUPPRESS_AFTER_DISCONNECTS = 3

# A backoff above every close's pause range, so the range can only raise it.
SLOW_BACKOFF = ReconnectPolicy(base_delay_s=100.0, max_delay_s=100.0)

# Rapid history outlives the stable reset, so only the reset can clear it.
OUTLASTING_RAPID_HISTORY = derive_policy(
    COOLDOWN_POLICY, rapid_window_s=2 * COOLDOWN_POLICY.stable_reset_s
)

# A failed TCP connect: retried, with no status to classify.
UNREACHABLE = ConnectionRefusedError


@pytest.fixture
def random_draw(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """Fix the value the reconnect jitter draws from ``random.random``."""

    def draw(value: float) -> None:
        monkeypatch.setattr(
            "phoenix_channels_python_client.default_decider.random.random",
            lambda: value,
        )

    return draw


def _make_client(policy: ReconnectPolicy | None = None) -> Client:
    """A client that is never connected; these tests call its reconnect logic."""
    return make_client(FakePhoenixServer(), reconnect_policy=policy)


def _decider(policy: ReconnectPolicy | None = None) -> DefaultReconnectDecider:
    return DefaultReconnectDecider(policy or ReconnectPolicy(), logging.getLogger())


def _close(decider: DefaultReconnectDecider, close_code: int) -> ReconnectDecision:
    """The decision after a connection that closes with ``close_code`` at once."""
    decider(Connected(generation=1))
    return decider(Disconnected(close_code=close_code, close_reason=STOP_REASON))


def _delay_after_rapid_disconnects(policy: ReconnectPolicy, count: int) -> float:
    """The delay after ``count`` back-to-back failed connects, each a rapid one."""
    decider = _decider(policy)
    for _ in range(count - 1):
        decider(ConnectFailed(UNREACHABLE()))
    return decider(ConnectFailed(UNREACHABLE())).delay_s


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def test_the_logged_socket_url_masks_the_api_key() -> None:
    client = _make_client()
    sent = _query(client.channel_socket_url)
    logged = _query(client.channel_socket_url_redacted)

    assert API_KEY in sent["api_key"]
    assert logged.keys() == sent.keys()
    assert API_KEY not in client.channel_socket_url_redacted


@each_normal_close_code
async def test_the_default_policy_reconnects_after_a_normal_close(
    close_code: CloseCode,
) -> None:
    decision = _close(_decider(), close_code)

    assert decision.should_reconnect is True


@each_normal_close_code
async def test_a_policy_not_reconnecting_on_a_normal_close_stops_without_an_error(
    close_code: CloseCode,
) -> None:
    decision = _close(_decider(STOPS_ON_NORMAL_CLOSE), close_code)

    assert decision.should_reconnect is False
    assert decision.terminal_error is None


async def test_a_policy_violation_close_is_terminal() -> None:
    decision = _close(_decider(), CloseCode.POLICY_VIOLATION)

    assert decision.should_reconnect is False
    assert isinstance(decision.terminal_error, SocketError)


@pytest.mark.parametrize(
    ("close_code", "low", "high"),
    [
        (
            CloseCode.SERVICE_RESTART,
            NO_BACKOFF.service_restart_min_delay_s,
            NO_BACKOFF.service_restart_max_delay_s,
        ),
        (
            CloseCode.TRY_AGAIN_LATER,
            NO_BACKOFF.try_again_later_min_delay_s,
            NO_BACKOFF.try_again_later_max_delay_s,
        ),
    ],
)
async def test_a_close_asking_for_a_pause_waits_within_its_range(
    close_code: CloseCode, low: float, high: float
) -> None:
    decision = _close(_decider(NO_BACKOFF), close_code)

    assert decision.should_reconnect is True
    assert low <= decision.delay_s <= high


@pytest.mark.parametrize(
    "close_code", [CloseCode.SERVICE_RESTART, CloseCode.TRY_AGAIN_LATER]
)
async def test_a_pause_range_never_shortens_the_backoff(
    close_code: CloseCode, random_draw: Callable[[float], None]
) -> None:
    random_draw(1.0)

    decision = _close(_decider(SLOW_BACKOFF), close_code)

    assert decision.delay_s == pytest.approx(SLOW_BACKOFF.max_delay_s)


async def test_a_forbidden_handshake_is_a_terminal_connect_failure() -> None:
    decision = _decider()(ConnectFailed(refused(TERMINAL_REFUSAL)))

    assert decision.should_reconnect is False
    assert isinstance(decision.terminal_error, SocketError)


@each_retried_refusal
async def test_a_refusal_other_than_forbidden_is_retried(refusal: HTTPStatus) -> None:
    decision = _decider()(ConnectFailed(refused(refusal)))

    assert decision.should_reconnect is True


async def test_a_failed_tcp_connect_is_retried() -> None:
    decision = _decider()(ConnectFailed(UNREACHABLE()))

    assert decision.should_reconnect is True


async def test_repeated_rapid_disconnects_stop_with_a_terminal_error() -> None:
    policy = derive_policy(
        COOLDOWN_POLICY, rapid_suppress_disconnect_count=SUPPRESS_AFTER_DISCONNECTS
    )
    decider = _decider(policy)

    decisions = [
        decider(ConnectFailed(UNREACHABLE())) for _ in range(SUPPRESS_AFTER_DISCONNECTS)
    ]

    assert [decision.should_reconnect for decision in decisions] == [True, True, False]
    assert isinstance(decisions[-1].terminal_error, SocketError)


@pytest.mark.clock
async def test_a_connection_outliving_stable_reset_restarts_the_backoff(
    clock: Clock, random_draw: Callable[[float], None]
) -> None:
    policy = OUTLASTING_RAPID_HISTORY
    random_draw(1.0)
    decider = _decider(policy)
    # Enough rapid failures to reach the cooldown.
    for _ in RAPID_COUNTS:
        decider(ConnectFailed(UNREACHABLE()))

    decider(Connected(generation=1))
    await clock.advance(policy.stable_reset_s)
    decision = decider(Disconnected(close_code=None, close_reason=""))

    assert decision.delay_s == pytest.approx(policy.base_delay_s)


async def test_the_rapid_cooldown_steps_from_its_base_up_to_its_cap() -> None:
    base = COOLDOWN_POLICY.rapid_cooldown_base_s
    step = COOLDOWN_POLICY.rapid_cooldown_step_s
    cap = COOLDOWN_POLICY.rapid_cooldown_max_s

    delays = [
        _delay_after_rapid_disconnects(COOLDOWN_POLICY, count) for count in RAPID_COUNTS
    ]
    cooldowns = [delay for delay in delays if delay >= base]

    assert cooldowns[0] == pytest.approx(base)
    for earlier, later in pairwise(cooldowns):
        assert later == pytest.approx(min(earlier + step, cap))
    assert cooldowns[-1] == pytest.approx(cap)


async def test_a_reconnect_below_the_cooldown_waits_half_to_all_of_its_delay(
    random_draw: Callable[[float], None],
) -> None:
    random_draw(0.0)
    lowest = _delay_after_rapid_disconnects(COOLDOWN_POLICY, 1)
    random_draw(1.0)
    highest = _delay_after_rapid_disconnects(COOLDOWN_POLICY, 1)

    assert lowest == pytest.approx(COOLDOWN_POLICY.base_delay_s / 2)
    assert highest == pytest.approx(COOLDOWN_POLICY.base_delay_s)


async def test_a_zero_base_delay_reconnects_immediately() -> None:
    policy = derive_policy(COOLDOWN_POLICY, base_delay_s=0.0)

    assert _delay_after_rapid_disconnects(policy, 1) == 0.0


async def test_the_hold_down_jitter_spans_the_configured_ratios_of_the_cooldown(
    random_draw: Callable[[float], None],
) -> None:
    policy = derive_policy(
        COOLDOWN_POLICY,
        rapid_hold_down_jitter_low_ratio=JITTER_LOW_RATIO,
        rapid_hold_down_jitter_high_ratio=JITTER_HIGH_RATIO,
    )
    # Past the cap, so the cooldown floor is the cap.
    floor = policy.rapid_cooldown_max_s

    random_draw(0.0)
    lowest = _delay_after_rapid_disconnects(policy, RAPID_COUNTS[-1])
    random_draw(1.0)
    highest = _delay_after_rapid_disconnects(policy, RAPID_COUNTS[-1])

    assert lowest == pytest.approx(floor * JITTER_LOW_RATIO)
    assert highest == pytest.approx(floor * JITTER_HIGH_RATIO)


def test_the_exponential_delay_doubles_up_to_its_cap() -> None:
    # COOLDOWN_POLICY's base, doubled by the default factor, then held at its cap.
    expected = [0.01, 0.02, 0.04, 0.08, 0.1, 0.1]

    delays = [
        exponential_delay(COOLDOWN_POLICY, attempt) for attempt in range(len(expected))
    ]

    assert delays == pytest.approx(expected)


def test_a_channel_rejoin_waits_half_to_all_of_its_backoff(
    random_draw: Callable[[float], None],
) -> None:
    client = _make_client(COOLDOWN_POLICY)
    attempt = 1
    backoff = exponential_delay(COOLDOWN_POLICY, attempt)

    random_draw(0.0)
    lowest = client._channel_rejoin_delay(attempt)
    random_draw(1.0)
    highest = client._channel_rejoin_delay(attempt)

    assert lowest == pytest.approx(backoff / 2)
    assert highest == pytest.approx(backoff)
