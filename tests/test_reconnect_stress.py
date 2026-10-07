from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.protocol_handler import (
    PhoenixChannelsProtocolVersion,
)
from tests.fake_server import FakePhoenixServer
from tests.support import ASYNC_TIMEOUT_S, STOP_REASON, TOPIC, make_client

# Clients sharing one API key, so the server lets only one stay connected.
SHARED_API_KEY = "shared-agent"

# Bounds on reconnect attempts per second: one client's, then all clients' together.
MAX_CLIENT_RATE_PER_S = 4.0
MAX_TWO_CLIENT_RATE_PER_S = 7.0
MAX_FOUR_CLIENT_RATE_PER_S = 12.0

# Jain's fairness index over the clients' rates; 1.0 is perfectly even. Repeated
# two-client trials even out; more clients or one short window vary more.
MIN_TWO_CLIENT_FAIRNESS = 0.7
MIN_FAIRNESS = 0.6

# Long enough for a join or leave while clients keep taking over the session.
CONTENDED_TOPIC_TIMEOUT_S = 1.0


@dataclass(frozen=True)
class ContentionMetrics:
    attempts: list[int]
    rates_per_s: list[float]
    total_rate_per_s: float
    max_rate_per_s: float
    fairness: float


def _jain_index(values: list[float]) -> float:
    if not values:
        return 0.0
    numerator = sum(values) ** 2
    denominator = len(values) * sum(value * value for value in values)
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _stress_policy() -> ReconnectPolicy:
    return ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.2,
        stable_reset_s=1.0,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.03,
        try_again_later_min_delay_s=0.2,
        try_again_later_max_delay_s=0.5,
        rapid_disconnect_uptime_s=0.2,
        rapid_window_s=3.0,
        rapid_first_min_delay_s=0.05,
        rapid_second_min_delay_s=0.1,
        rapid_cooldown_base_s=0.2,
        rapid_cooldown_step_s=0.2,
        rapid_cooldown_max_s=1.5,
        rapid_suppress_disconnect_count=0,
        rapid_hold_down_jitter_low_ratio=0.2,
        rapid_hold_down_jitter_high_ratio=1.0,
    )


def _client_path(idx: int) -> str:
    return f"/socket/stress-{idx}"


@asynccontextmanager
async def contending_clients(
    phoenix_server: FakePhoenixServer, clients_n: int
) -> AsyncIterator[None]:
    """Run ``clients_n`` clients that share one session, then shut them all down."""
    policy = _stress_policy()
    async with AsyncExitStack() as stack:
        clients: list[PHXChannelsClient] = []
        # Each joins before the next connects and takes over the shared session.
        for idx in range(clients_n):
            client = await stack.enter_async_context(
                make_client(
                    phoenix_server,
                    _client_path(idx),
                    api_key=SHARED_API_KEY,
                    reconnect_policy=policy,
                    join_timeout_s=CONTENDED_TOPIC_TIMEOUT_S,
                    leave_timeout_s=CONTENDED_TOPIC_TIMEOUT_S,
                )
            )
            await client.subscribe_to_topic(TOPIC)
            clients.append(client)
        runs = [asyncio.create_task(client.run_forever()) for client in clients]
        try:
            yield
        finally:
            async with asyncio.timeout(ASYNC_TIMEOUT_S):
                await asyncio.gather(
                    *(client.shutdown(STOP_REASON) for client in clients),
                    return_exceptions=True,
                )
                await asyncio.gather(*runs, return_exceptions=True)


async def _run_contention_trial(
    phoenix_server: FakePhoenixServer,
    *,
    clients_n: int,
    duration_s: float,
) -> ContentionMetrics:
    phoenix_server.enforce_single_connection_per_api_key = True
    phoenix_server.connection_attempts_by_path.clear()

    async with contending_clients(phoenix_server, clients_n):
        await asyncio.sleep(duration_s)

    attempts = [
        phoenix_server.get_connection_attempts(_client_path(idx))
        for idx in range(clients_n)
    ]
    rates = [attempt / duration_s for attempt in attempts]

    return ContentionMetrics(
        attempts=attempts,
        rates_per_s=rates,
        total_rate_per_s=sum(rates),
        max_rate_per_s=max(rates) if rates else 0.0,
        fairness=_jain_index(rates),
    )


async def test_duplicate_session_two_clients_stress_is_bounded(
    phoenix_server: FakePhoenixServer,
) -> None:
    trials = [
        await _run_contention_trial(phoenix_server, clients_n=2, duration_s=3.0)
        for _ in range(4)
    ]

    assert max(metric.max_rate_per_s for metric in trials) <= MAX_CLIENT_RATE_PER_S
    assert (
        max(metric.total_rate_per_s for metric in trials) <= MAX_TWO_CLIENT_RATE_PER_S
    )
    assert min(metric.fairness for metric in trials) >= MIN_TWO_CLIENT_FAIRNESS


async def test_duplicate_session_four_clients_stress_scales_without_cascade(
    phoenix_server: FakePhoenixServer,
) -> None:
    trials = [
        await _run_contention_trial(phoenix_server, clients_n=4, duration_s=3.0)
        for _ in range(3)
    ]

    assert max(metric.max_rate_per_s for metric in trials) <= MAX_CLIENT_RATE_PER_S
    assert (
        max(metric.total_rate_per_s for metric in trials) <= MAX_FOUR_CLIENT_RATE_PER_S
    )
    assert min(metric.fairness for metric in trials) >= MIN_FAIRNESS


# How long the two contending clients run before their attempts are counted.
CONTENTION_WINDOW_S = 1.2


# V2 runs in the duplicate-session stress tests above.
@pytest.mark.parametrize("protocol", [PhoenixChannelsProtocolVersion.V1])
async def test_two_clients_contending_for_one_session_reconnect_at_a_bounded_fair_rate(
    phoenix_server: FakePhoenixServer,
) -> None:
    metrics = await _run_contention_trial(
        phoenix_server, clients_n=2, duration_s=CONTENTION_WINDOW_S
    )

    assert metrics.max_rate_per_s <= MAX_CLIENT_RATE_PER_S
    assert metrics.fairness >= MIN_FAIRNESS
