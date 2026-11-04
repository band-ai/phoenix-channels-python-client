from __future__ import annotations

import inspect
import logging

from websockets import ClientConnection
from websockets.exceptions import ConnectionClosed

from phoenix_channels_python_client.client_types import (
    GATE_EVENTS,
    ReconnectDecider,
    ReconnectDecision,
    ReconnectEvent,
    ReconnectPolicy,
)
from phoenix_channels_python_client.default_decider import (
    DefaultReconnectDecider,
    equal_jitter,
    exponential_delay,
)
from phoenix_channels_python_client.exceptions import SocketError


def _decider_failure(message: str, cause: Exception | None = None) -> ReconnectDecision:
    """A stop for a broken decider; never the built-in policy deciding instead."""
    error = SocketError(message)
    error.__cause__ = cause
    return ReconnectDecision(should_reconnect=False, terminal_error=error)


class ReconnectControllerMixin:
    logger: logging.Logger

    def redact_diagnostic(self, text: str) -> str:
        raise NotImplementedError

    reconnect_policy: ReconnectPolicy
    reconnect_decider: ReconnectDecider | None
    decider: ReconnectDecider

    def _new_decider(self) -> ReconnectDecider:
        """The consumer's decider, or a fresh built-in one for this run."""
        if self.reconnect_decider is not None:
            return self.reconnect_decider
        return DefaultReconnectDecider(self.reconnect_policy, self.logger)

    def _decide(self, event: ReconnectEvent) -> ReconnectDecision:
        """The decider's answer to ``event``; a broken answer stops the client."""
        try:
            decision = self.decider(event)
        except Exception as exc:
            self.logger.exception("reconnect_decider failed; stopping the client")
            return _decider_failure("reconnect_decider failed", exc)
        if inspect.iscoroutine(decision):
            decision.close()  # Never awaited; closed so it raises no warning.
            return _decider_failure("reconnect_decider must be synchronous")
        if not isinstance(decision, ReconnectDecision):
            return _decider_failure("reconnect_decider returned no ReconnectDecision")
        if decision.delay_s and isinstance(event, GATE_EVENTS):
            # No attempt follows a gate, so its delay would be silently dropped.
            return _decider_failure("reconnect_decider returned a delay for a gate")
        return decision

    def _channel_rejoin_delay(self, attempt: int) -> float:
        # No rapid-disconnect floors: those describe socket churn, not one channel.
        return equal_jitter(exponential_delay(self.reconnect_policy, attempt))

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
