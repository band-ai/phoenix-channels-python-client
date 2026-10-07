from __future__ import annotations

from collections import deque
from collections.abc import Callable
from itertools import pairwise
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError
from tests.fake_server import FakePhoenixServer
from tests.support import API_KEY, STOP_REASON, derive_policy, make_client

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


@pytest.fixture
def random_draw(monkeypatch: pytest.MonkeyPatch) -> Callable[[float], None]:
    """Fix the value the reconnect jitter draws from ``random.random``."""

    def draw(value: float) -> None:
        monkeypatch.setattr(
            "phoenix_channels_python_client.reconnect_controller.random.random",
            lambda: value,
        )

    return draw


def _make_client(policy: ReconnectPolicy | None = None) -> PHXChannelsClient:
    """A client that is never connected; these tests call its reconnect logic."""
    return make_client(FakePhoenixServer(), reconnect_policy=policy)


def _delay_after_rapid_disconnects(client: PHXChannelsClient, count: int) -> float:
    client._rapid_disconnects = deque([0.0] * count)
    return client._compute_reconnect_delay(attempt=0)


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def test_the_logged_socket_url_masks_the_api_key() -> None:
    client = _make_client()
    sent = _query(client.channel_socket_url)
    logged = _query(client.channel_socket_url_redacted)

    assert API_KEY in sent["api_key"]
    assert logged.keys() == sent.keys()
    assert API_KEY not in client.channel_socket_url_redacted


def test_close_codes_are_classified_by_the_policy() -> None:
    policy = ReconnectPolicy()
    client = _make_client(policy)

    normal = client._classify_disconnect(CloseCode.NORMAL_CLOSURE, STOP_REASON)
    assert normal.should_reconnect is False
    assert normal.terminal_error is None

    violation = client._classify_disconnect(CloseCode.POLICY_VIOLATION, STOP_REASON)
    assert violation.should_reconnect is False
    assert isinstance(violation.terminal_error, PHXConnectionError)

    restart = client._classify_disconnect(CloseCode.SERVICE_RESTART, STOP_REASON)
    assert restart.should_reconnect is True
    assert restart.min_delay_s == policy.service_restart_min_delay_s
    assert restart.max_delay_s == policy.service_restart_max_delay_s

    busy = client._classify_disconnect(CloseCode.TRY_AGAIN_LATER, STOP_REASON)
    assert busy.should_reconnect is True
    assert busy.min_delay_s == policy.try_again_later_min_delay_s
    assert busy.max_delay_s == policy.try_again_later_max_delay_s


def test_the_rapid_cooldown_steps_from_its_base_up_to_its_cap() -> None:
    client = _make_client(COOLDOWN_POLICY)
    base = COOLDOWN_POLICY.rapid_cooldown_base_s
    step = COOLDOWN_POLICY.rapid_cooldown_step_s
    cap = COOLDOWN_POLICY.rapid_cooldown_max_s

    delays = [_delay_after_rapid_disconnects(client, count) for count in RAPID_COUNTS]
    cooldowns = [delay for delay in delays if delay >= base]

    assert cooldowns[0] == pytest.approx(base)
    for earlier, later in pairwise(cooldowns):
        assert later == pytest.approx(min(earlier + step, cap))
    assert cooldowns[-1] == pytest.approx(cap)


def test_a_reconnect_without_rapid_disconnects_waits_half_to_all_of_its_delay(
    random_draw: Callable[[float], None],
) -> None:
    client = _make_client(COOLDOWN_POLICY)

    random_draw(0.0)
    lowest = _delay_after_rapid_disconnects(client, 0)
    random_draw(1.0)
    highest = _delay_after_rapid_disconnects(client, 0)

    assert lowest == pytest.approx(COOLDOWN_POLICY.base_delay_s / 2)
    assert highest == pytest.approx(COOLDOWN_POLICY.base_delay_s)


def test_the_hold_down_jitter_spans_the_configured_ratios_of_the_cooldown(
    random_draw: Callable[[float], None],
) -> None:
    policy = derive_policy(
        COOLDOWN_POLICY,
        rapid_hold_down_jitter_low_ratio=JITTER_LOW_RATIO,
        rapid_hold_down_jitter_high_ratio=JITTER_HIGH_RATIO,
    )
    client = _make_client(policy)
    # Past the cap, so the cooldown floor is the cap.
    floor = policy.rapid_cooldown_max_s

    random_draw(0.0)
    lowest = _delay_after_rapid_disconnects(client, RAPID_COUNTS[-1])
    random_draw(1.0)
    highest = _delay_after_rapid_disconnects(client, RAPID_COUNTS[-1])

    assert lowest == pytest.approx(floor * JITTER_LOW_RATIO)
    assert highest == pytest.approx(floor * JITTER_HIGH_RATIO)
