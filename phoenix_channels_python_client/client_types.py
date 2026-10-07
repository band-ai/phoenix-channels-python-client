from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    model_validator,
)

from phoenix_channels_python_client.exceptions import PHXConnectionError


class ClientState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    SHUTTING_DOWN = "shutting_down"
    CLOSED = "closed"


# Each (low, high) pair of fields where high must not be below low.
_ORDERED_POLICY_FIELDS = (
    ("service_restart_min_delay_s", "service_restart_max_delay_s"),
    ("try_again_later_min_delay_s", "try_again_later_max_delay_s"),
    ("rapid_cooldown_base_s", "rapid_cooldown_max_s"),
    ("rapid_hold_down_jitter_low_ratio", "rapid_hold_down_jitter_high_ratio"),
)


class ReconnectPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    base_delay_s: NonNegativeFloat = 0.5
    factor: PositiveFloat = 2.0
    max_delay_s: NonNegativeFloat = 30.0
    stable_reset_s: PositiveFloat = 60.0
    reconnect_on_normal_close: bool = False
    policy_violation_is_terminal: bool = True
    service_restart_min_delay_s: NonNegativeFloat = 1.0
    service_restart_max_delay_s: float = 5.0
    try_again_later_min_delay_s: NonNegativeFloat = 30.0
    try_again_later_max_delay_s: float = 60.0
    rapid_disconnect_uptime_s: NonNegativeFloat = 5.0
    rapid_window_s: PositiveFloat = 60.0
    rapid_first_min_delay_s: NonNegativeFloat = 2.0
    rapid_second_min_delay_s: NonNegativeFloat = 10.0
    rapid_cooldown_base_s: NonNegativeFloat = 60.0
    rapid_cooldown_step_s: NonNegativeFloat = 30.0
    rapid_cooldown_max_s: float = 300.0
    rapid_suppress_disconnect_count: NonNegativeInt = 10
    rapid_hold_down_jitter_low_ratio: NonNegativeFloat = 0.25
    rapid_hold_down_jitter_high_ratio: float = 1.0

    @model_validator(mode="after")
    def _check_ordering(self) -> Self:
        for low, high in _ORDERED_POLICY_FIELDS:
            if getattr(self, high) < getattr(self, low):
                raise ValueError(f"{high} must be >= {low}")
        return self


@dataclass(frozen=True)
class ReconnectDecision:
    should_reconnect: bool
    min_delay_s: float | None = None
    max_delay_s: float | None = None
    terminal_error: PHXConnectionError | None = None
