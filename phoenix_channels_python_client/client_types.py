from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    model_validator,
)

from phoenix_channels_python_client.exceptions import PHXConnectionError

JoinResponseCallback = Callable[[Any], None]


class ClientState(StrEnum):
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
    # Phoenix >= 1.8.8 closes deliberate disconnects with 1001 ("reconnect
    # expected"), and Bandit answers a close the peer started with 1000, so
    # neither code is a stop signal.
    reconnect_on_normal_close: bool = True
    policy_violation_is_terminal: bool = True
    service_restart_min_delay_s: NonNegativeFloat = 1.0
    service_restart_max_delay_s: NonNegativeFloat = 5.0
    try_again_later_min_delay_s: NonNegativeFloat = 30.0
    try_again_later_max_delay_s: NonNegativeFloat = 60.0
    rapid_disconnect_uptime_s: NonNegativeFloat = 5.0
    rapid_window_s: PositiveFloat = 60.0
    rapid_first_min_delay_s: NonNegativeFloat = 2.0
    rapid_second_min_delay_s: NonNegativeFloat = 10.0
    rapid_cooldown_base_s: NonNegativeFloat = 60.0
    rapid_cooldown_step_s: NonNegativeFloat = 30.0
    rapid_cooldown_max_s: NonNegativeFloat = 300.0
    rapid_suppress_disconnect_count: NonNegativeInt = 10
    rapid_hold_down_jitter_low_ratio: NonNegativeFloat = 0.25
    rapid_hold_down_jitter_high_ratio: NonNegativeFloat = 1.0

    @model_validator(mode="after")
    def _check_ordering(self) -> Self:
        for low, high in _ORDERED_POLICY_FIELDS:
            if getattr(self, high) < getattr(self, low):
                raise ValueError(f"{high} must be >= {low}")
        return self


@dataclass(frozen=True)
class ReconnectDecision:
    """Whether to connect again, and after how long.

    ``delay_s`` is the exact wait and replaces the computed backoff;
    ``min_delay_s``/``max_delay_s`` only raise it.
    """

    should_reconnect: bool
    min_delay_s: float | None = None
    max_delay_s: float | None = None
    terminal_error: PHXConnectionError | None = None
    delay_s: float | None = None

    def __post_init__(self) -> None:
        if self.delay_s is None:
            return
        if not math.isfinite(self.delay_s) or self.delay_s < 0:
            raise ValueError("delay_s must be finite and >= 0")
        if self.min_delay_s is not None or self.max_delay_s is not None:
            raise ValueError("delay_s excludes min_delay_s and max_delay_s")


@dataclass(frozen=True)
class Connecting:
    """A connection attempt is about to start."""


@dataclass(frozen=True)
class Disconnected:
    """The connection closed. ``close_code`` is ``None`` for a forced close."""

    close_code: int | None
    close_reason: str


@dataclass(frozen=True)
class ConnectFailed:
    """A connection attempt failed with ``error``, e.g. a refused handshake."""

    error: Exception


ReconnectEvent = Connecting | Disconnected | ConnectFailed
