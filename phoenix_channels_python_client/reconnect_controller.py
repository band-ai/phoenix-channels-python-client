from __future__ import annotations

import asyncio
import logging
import random
from collections import deque

from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.client_types import (
    ReconnectDecision,
    ReconnectPolicy,
)
from phoenix_channels_python_client.exceptions import PHXConnectionError

logger = logging.getLogger(__name__)


class ReconnectControllerMixin:
    reconnect_policy: ReconnectPolicy
    _rapid_disconnects: deque[float]

    def _prune_rapid_disconnects(self, now: float) -> None:
        while (
            self._rapid_disconnects
            and now - self._rapid_disconnects[0] > self.reconnect_policy.rapid_window_s
        ):
            self._rapid_disconnects.popleft()

    def _record_disconnect(self, connection_uptime_s: float) -> None:
        now = asyncio.get_running_loop().time()
        if connection_uptime_s < self.reconnect_policy.rapid_disconnect_uptime_s:
            self._rapid_disconnects.append(now)
        self._prune_rapid_disconnects(now)
        logger.debug(
            "Recorded disconnect. uptime_s=%s rapid_count=%s rapid_window_s=%s",
            connection_uptime_s,
            len(self._rapid_disconnects),
            self.reconnect_policy.rapid_window_s,
            extra={
                "connection_uptime_s": connection_uptime_s,
                "rapid_count": len(self._rapid_disconnects),
                "rapid_window_s": self.reconnect_policy.rapid_window_s,
            },
        )

    def _should_suppress_reconnect(self) -> bool:
        threshold = self.reconnect_policy.rapid_suppress_disconnect_count
        should_suppress = threshold > 0 and len(self._rapid_disconnects) >= threshold
        if should_suppress:
            logger.debug(
                "Reconnect suppression threshold met. rapid_count=%s threshold=%s",
                len(self._rapid_disconnects),
                threshold,
                extra={
                    "rapid_count": len(self._rapid_disconnects),
                    "threshold": threshold,
                },
            )
        return should_suppress

    def _compute_reconnect_delay(self, attempt: int) -> float:
        base_delay = min(
            self.reconnect_policy.max_delay_s,
            self.reconnect_policy.base_delay_s
            * (self.reconnect_policy.factor ** max(attempt, 0)),
        )

        rapid_count = len(self._rapid_disconnects)
        min_delay = 0.0
        if rapid_count == 1:
            min_delay = self.reconnect_policy.rapid_first_min_delay_s
        elif rapid_count == 2:
            min_delay = self.reconnect_policy.rapid_second_min_delay_s
        elif rapid_count >= 3:
            cooldown_delay = min(
                self.reconnect_policy.rapid_cooldown_base_s
                + (self.reconnect_policy.rapid_cooldown_step_s * (rapid_count - 3)),
                self.reconnect_policy.rapid_cooldown_max_s,
            )
            min_delay = cooldown_delay

        delay = max(base_delay, min_delay)
        if delay <= 0:
            return 0.0

        if rapid_count >= 3:
            low_ratio = self.reconnect_policy.rapid_hold_down_jitter_low_ratio
            high_ratio = self.reconnect_policy.rapid_hold_down_jitter_high_ratio
            min_jittered = delay * low_ratio
            max_jittered = delay * high_ratio
            computed = self._random_between(min_jittered, max_jittered)
            logger.debug(
                "Computed reconnect delay with hold-down jitter. attempt=%s rapid_count=%s base_delay_s=%s min_delay_s=%s computed_delay_s=%s",
                attempt,
                rapid_count,
                base_delay,
                min_delay,
                computed,
                extra={
                    "attempt": attempt,
                    "rapid_count": rapid_count,
                    "base_delay_s": base_delay,
                    "min_delay_s": min_delay,
                    "computed_delay_s": computed,
                },
            )
            return computed

        # Equal jitter avoids synchronization while keeping a meaningful minimum delay.
        computed = (delay / 2) + (random.random() * (delay / 2))
        logger.debug(
            "Computed reconnect delay with equal jitter. attempt=%s rapid_count=%s base_delay_s=%s min_delay_s=%s computed_delay_s=%s",
            attempt,
            rapid_count,
            base_delay,
            min_delay,
            computed,
            extra={
                "attempt": attempt,
                "rapid_count": rapid_count,
                "base_delay_s": base_delay,
                "min_delay_s": min_delay,
                "computed_delay_s": computed,
            },
        )
        return computed

    def _extract_close_details(
        self,
        *,
        connection: ClientConnection,
        routing_error: Exception | None,
    ) -> tuple[int | None, str]:
        if isinstance(routing_error, ConnectionClosed):
            if routing_error.rcvd is not None:
                return routing_error.rcvd.code, routing_error.rcvd.reason
            if routing_error.sent is not None:
                return routing_error.sent.code, routing_error.sent.reason
        return connection.close_code, connection.close_reason or ""

    def _classify_disconnect(
        self, close_code: int | None, close_reason: str
    ) -> ReconnectDecision:
        if close_code is None:
            decision = ReconnectDecision(should_reconnect=True)
            logger.debug(
                "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s",
                close_code,
                close_reason,
                decision.should_reconnect,
                extra={
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "should_reconnect": decision.should_reconnect,
                },
            )
            return decision

        if (
            close_code in {1000, 1001}
            and not self.reconnect_policy.reconnect_on_normal_close
        ):
            decision = ReconnectDecision(should_reconnect=False)
            logger.debug(
                "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s",
                close_code,
                close_reason,
                decision.should_reconnect,
                extra={
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "should_reconnect": decision.should_reconnect,
                },
            )
            return decision

        if close_code == 1008 and self.reconnect_policy.policy_violation_is_terminal:
            reason = close_reason or "policy violation"
            decision = ReconnectDecision(
                should_reconnect=False,
                terminal_error=PHXConnectionError(
                    f"Reconnect disabled due to terminal close code 1008 ({reason})"
                ),
            )
            logger.debug(
                "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s terminal=%s",
                close_code,
                close_reason,
                decision.should_reconnect,
                decision.terminal_error,
                extra={
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "should_reconnect": decision.should_reconnect,
                    "terminal": True,
                },
            )
            return decision

        if close_code == 1012:
            decision = ReconnectDecision(
                should_reconnect=True,
                min_delay_s=self.reconnect_policy.service_restart_min_delay_s,
                max_delay_s=self.reconnect_policy.service_restart_max_delay_s,
            )
            logger.debug(
                "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s min_delay_s=%s max_delay_s=%s",
                close_code,
                close_reason,
                decision.should_reconnect,
                decision.min_delay_s,
                decision.max_delay_s,
                extra={
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "should_reconnect": decision.should_reconnect,
                    "min_delay_s": decision.min_delay_s,
                    "max_delay_s": decision.max_delay_s,
                },
            )
            return decision

        if close_code == 1013:
            decision = ReconnectDecision(
                should_reconnect=True,
                min_delay_s=self.reconnect_policy.try_again_later_min_delay_s,
                max_delay_s=self.reconnect_policy.try_again_later_max_delay_s,
            )
            logger.debug(
                "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s min_delay_s=%s max_delay_s=%s",
                close_code,
                close_reason,
                decision.should_reconnect,
                decision.min_delay_s,
                decision.max_delay_s,
                extra={
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "should_reconnect": decision.should_reconnect,
                    "min_delay_s": decision.min_delay_s,
                    "max_delay_s": decision.max_delay_s,
                },
            )
            return decision

        decision = ReconnectDecision(should_reconnect=True)
        logger.debug(
            "Disconnect classification result. close_code=%s close_reason=%s should_reconnect=%s",
            close_code,
            close_reason,
            decision.should_reconnect,
            extra={
                "close_code": close_code,
                "close_reason": close_reason,
                "should_reconnect": decision.should_reconnect,
            },
        )
        return decision

    def _apply_disconnect_delay_override(
        self,
        computed_delay_s: float,
        decision: ReconnectDecision,
    ) -> float:
        if decision.min_delay_s is None:
            return computed_delay_s

        if decision.max_delay_s is None:
            return max(computed_delay_s, decision.min_delay_s)

        override = self._random_between(decision.min_delay_s, decision.max_delay_s)
        return max(computed_delay_s, override)

    def _random_between(self, min_delay_s: float, max_delay_s: float) -> float:
        low = max(0.0, min_delay_s)
        high = max(low, max_delay_s)
        if high == low:
            return low
        return low + (random.random() * (high - low))
