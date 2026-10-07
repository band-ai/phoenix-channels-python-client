from __future__ import annotations

from typing import Any

import pytest

from phoenix_channels_python_client.client_state_machine import transition_client_state
from phoenix_channels_python_client.client_types import (
    ClientState,
    ReconnectPolicy,
    reconnect_policy_is_invalid,
)


def test_transitioning_to_the_same_state_is_allowed() -> None:
    assert (
        transition_client_state(ClientState.CLOSED, ClientState.CLOSED)
        == ClientState.CLOSED
    )


def test_an_invalid_state_transition_raises() -> None:
    with pytest.raises(RuntimeError):
        transition_client_state(ClientState.CLOSED, ClientState.CONNECTED)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_delay_s": -1},
        {"factor": 0},
        {"max_delay_s": -1},
        {"stable_reset_s": 0},
        {"service_restart_min_delay_s": -1},
        {"service_restart_max_delay_s": 0, "service_restart_min_delay_s": 1},
        {"try_again_later_min_delay_s": -1},
        {"try_again_later_max_delay_s": 0, "try_again_later_min_delay_s": 1},
        {"rapid_disconnect_uptime_s": -1},
        {"rapid_window_s": 0},
        {"rapid_first_min_delay_s": -1},
        {"rapid_second_min_delay_s": -1},
        {"rapid_cooldown_base_s": -1},
        {"rapid_cooldown_step_s": -1},
        {"rapid_cooldown_max_s": 0, "rapid_cooldown_base_s": 1},
        {"rapid_suppress_disconnect_count": -1},
        {"rapid_hold_down_jitter_low_ratio": -1},
        {
            "rapid_hold_down_jitter_high_ratio": 0.1,
            "rapid_hold_down_jitter_low_ratio": 0.2,
        },
    ],
)
def test_an_out_of_range_reconnect_policy_is_invalid(kwargs: dict[str, Any]) -> None:
    assert reconnect_policy_is_invalid(ReconnectPolicy(**kwargs)) is True


def test_the_default_reconnect_policy_is_valid() -> None:
    assert reconnect_policy_is_invalid(ReconnectPolicy()) is False
