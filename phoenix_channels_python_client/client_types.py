from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from phoenix_channels_python_client.exceptions import PHXConnectionError


class ClientState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    SHUTTING_DOWN = "shutting_down"
    CLOSED = "closed"


@dataclass(frozen=True)
class ReconnectPolicy:
    base_delay_s: float = 0.5
    factor: float = 2.0
    max_delay_s: float = 30.0
    stable_reset_s: float = 60.0
    reconnect_on_normal_close: bool = False
    policy_violation_is_terminal: bool = True
    service_restart_min_delay_s: float = 1.0
    service_restart_max_delay_s: float = 5.0
    try_again_later_min_delay_s: float = 30.0
    try_again_later_max_delay_s: float = 60.0
    rapid_disconnect_uptime_s: float = 5.0
    rapid_window_s: float = 60.0
    rapid_first_min_delay_s: float = 2.0
    rapid_second_min_delay_s: float = 10.0
    rapid_cooldown_base_s: float = 60.0
    rapid_cooldown_step_s: float = 30.0
    rapid_cooldown_max_s: float = 300.0
    rapid_suppress_disconnect_count: int = 10
    rapid_hold_down_jitter_low_ratio: float = 0.25
    rapid_hold_down_jitter_high_ratio: float = 1.0


@dataclass(frozen=True)
class ReconnectDecision:
    should_reconnect: bool
    min_delay_s: float | None = None
    max_delay_s: float | None = None
    terminal_error: PHXConnectionError | None = None


def reconnect_policy_is_invalid(policy: ReconnectPolicy) -> bool:
    if policy.base_delay_s < 0:
        return True
    if policy.factor <= 0:
        return True
    if policy.max_delay_s < 0:
        return True
    if policy.stable_reset_s <= 0:
        return True
    if policy.service_restart_min_delay_s < 0:
        return True
    if policy.service_restart_max_delay_s < policy.service_restart_min_delay_s:
        return True
    if policy.try_again_later_min_delay_s < 0:
        return True
    if policy.try_again_later_max_delay_s < policy.try_again_later_min_delay_s:
        return True
    if policy.rapid_disconnect_uptime_s < 0:
        return True
    if policy.rapid_window_s <= 0:
        return True
    if policy.rapid_first_min_delay_s < 0:
        return True
    if policy.rapid_second_min_delay_s < 0:
        return True
    if policy.rapid_cooldown_base_s < 0:
        return True
    if policy.rapid_cooldown_step_s < 0:
        return True
    if policy.rapid_cooldown_max_s < policy.rapid_cooldown_base_s:
        return True
    if policy.rapid_suppress_disconnect_count < 0:
        return True
    if policy.rapid_hold_down_jitter_low_ratio < 0:
        return True
    return (
        policy.rapid_hold_down_jitter_high_ratio
        < policy.rapid_hold_down_jitter_low_ratio
    )
