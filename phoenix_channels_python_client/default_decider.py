"""The built-in reconnect policy, as a ``reconnect_decider``."""

from __future__ import annotations

import asyncio
import logging
import random
from collections import deque
from http import HTTPStatus

from websockets.exceptions import InvalidStatus
from websockets.frames import CloseCode

from phoenix_channels_python_client.client_types import (
    PROCEED,
    Connected,
    ConnectFailed,
    Disconnected,
    ReconnectDecision,
    ReconnectEvent,
    ReconnectPolicy,
)
from phoenix_channels_python_client.exceptions import SocketError

# The Band platform answers every auth refusal with 403, and its websocket spec
# (thenvoi-platform spec/websocket-api.draft.md) makes a rejected reconnect terminal.
# A 401 could only come from an edge layer, so it and every other status retry.
TERMINAL_HANDSHAKE_STATUS = HTTPStatus.FORBIDDEN

# A (low, high) range a close's backoff is raised into.
DelayRange = tuple[float, float]


def random_between(min_delay_s: float, max_delay_s: float) -> float:
    low = max(0.0, min_delay_s)
    high = max(low, max_delay_s)
    if high == low:
        return low
    return low + (random.random() * (high - low))  # noqa: S311  # jitter


def exponential_delay(policy: ReconnectPolicy, attempt: int) -> float:
    """The policy's capped exponential backoff for ``attempt``, unjittered."""
    return min(
        policy.max_delay_s,
        policy.base_delay_s * (policy.factor ** max(attempt, 0)),
    )


def equal_jitter(delay: float) -> float:
    # Equal jitter avoids synchronization while keeping a meaningful minimum delay.
    return random_between(delay / 2, delay)


def _now() -> float:
    return asyncio.get_running_loop().time()


def _terminal_decision(cause: str) -> ReconnectDecision:
    return ReconnectDecision(
        should_reconnect=False,
        terminal_error=SocketError(f"Reconnect disabled due to terminal {cause}"),
    )


class DefaultReconnectDecider:
    """Classifies closes and refusals, backs off, and stops on rapid churn.

    Holds one run's backoff attempt and rapid-disconnect history, so the client
    builds a fresh one each time it is entered.
    """

    def __init__(self, policy: ReconnectPolicy, logger: logging.Logger) -> None:
        self._policy = policy
        self._logger = logger
        self._attempt = 0
        self._rapid_disconnects: deque[float] = deque()
        self._connected_at = 0.0

    def __call__(self, event: ReconnectEvent) -> ReconnectDecision:
        match event:
            case Connected():
                self._connected_at = _now()
                return PROCEED
            case Disconnected():
                return self._after_close(event.close_code, event.close_reason)
            case ConnectFailed():
                return self._after_connect_failure(event.error)
            case _:  # Connecting: always attempt.
                return PROCEED

    def _after_close(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision:
        stop = self._stop_for_close(close_code, close_reason)
        if stop is not None:
            return stop
        uptime_s = _now() - self._connected_at
        if self._suppressed(uptime_s):
            return self._suppression()
        if uptime_s >= self._policy.stable_reset_s:
            self._attempt = 0
            self._rapid_disconnects.clear()
        else:
            self._attempt += 1
        delay = self._backoff(self._attempt)
        delay_range = self._delay_range_for_close(close_code)
        if delay_range is not None:
            delay = max(delay, random_between(*delay_range))
        return ReconnectDecision(should_reconnect=True, delay_s=delay)

    def _after_connect_failure(self, error: Exception) -> ReconnectDecision:
        if (
            isinstance(error, InvalidStatus)
            and error.response.status_code == TERMINAL_HANDSHAKE_STATUS
        ):
            return _terminal_decision(
                f"handshake status {TERMINAL_HANDSHAKE_STATUS} "
                f"({TERMINAL_HANDSHAKE_STATUS.phrase})"
            )
        if self._suppressed(0.0):
            return self._suppression()
        delay = self._backoff(self._attempt)
        self._attempt += 1
        return ReconnectDecision(should_reconnect=True, delay_s=delay)

    def _stop_for_close(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision | None:
        if (
            close_code in {CloseCode.NORMAL_CLOSURE, CloseCode.GOING_AWAY}
            and not self._policy.reconnect_on_normal_close
        ):
            return ReconnectDecision(should_reconnect=False)
        if (
            close_code == CloseCode.POLICY_VIOLATION
            and self._policy.policy_violation_is_terminal
        ):
            reason = close_reason or "policy violation"
            return _terminal_decision(
                f"close code {CloseCode.POLICY_VIOLATION} ({reason})"
            )
        return None

    def _delay_range_for_close(self, close_code: int | None) -> DelayRange | None:
        policy = self._policy
        match close_code:
            case CloseCode.SERVICE_RESTART:
                return (
                    policy.service_restart_min_delay_s,
                    policy.service_restart_max_delay_s,
                )
            case CloseCode.TRY_AGAIN_LATER:
                return (
                    policy.try_again_later_min_delay_s,
                    policy.try_again_later_max_delay_s,
                )
            case _:
                return None

    def _suppressed(self, uptime_s: float) -> bool:
        """Record a disconnect after ``uptime_s``; whether churn now stops us."""
        now = _now()
        if uptime_s < self._policy.rapid_disconnect_uptime_s:
            self._rapid_disconnects.append(now)
        while (
            self._rapid_disconnects
            and now - self._rapid_disconnects[0] > self._policy.rapid_window_s
        ):
            self._rapid_disconnects.popleft()
        threshold = self._policy.rapid_suppress_disconnect_count
        rapid_count = len(self._rapid_disconnects)
        self._logger.debug(
            "Recorded disconnect. uptime_s=%s rapid_count=%s threshold=%s",
            uptime_s,
            rapid_count,
            threshold,
        )
        return threshold > 0 and rapid_count >= threshold

    def _suppression(self) -> ReconnectDecision:
        return ReconnectDecision(
            should_reconnect=False,
            terminal_error=SocketError(
                "Reconnect suppressed after repeated rapid disconnects. "
                "Likely duplicate connection or unstable endpoint."
            ),
        )

    def _rapid_floors(self) -> tuple[float, ...]:
        """The floor for each rapid-disconnect count before the cooldown starts."""
        return (
            0.0,
            self._policy.rapid_first_min_delay_s,
            self._policy.rapid_second_min_delay_s,
        )

    def _backoff(self, attempt: int) -> float:
        """The jittered wait for ``attempt``, raised to the rapid-disconnect floor."""
        rapid_count = len(self._rapid_disconnects)
        cooldown_steps = rapid_count - len(self._rapid_floors())
        if cooldown_steps < 0:
            floor = self._rapid_floors()[rapid_count]
        else:
            floor = min(
                self._policy.rapid_cooldown_base_s
                + (self._policy.rapid_cooldown_step_s * cooldown_steps),
                self._policy.rapid_cooldown_max_s,
            )
        delay = max(exponential_delay(self._policy, attempt), floor)
        if delay <= 0:
            return 0.0
        if cooldown_steps < 0:
            computed = equal_jitter(delay)
        else:
            computed = random_between(
                delay * self._policy.rapid_hold_down_jitter_low_ratio,
                delay * self._policy.rapid_hold_down_jitter_high_ratio,
            )
        self._logger.debug(
            "Computed reconnect delay. attempt=%s rapid_count=%s computed_delay_s=%s",
            attempt,
            rapid_count,
            computed,
        )
        return computed
