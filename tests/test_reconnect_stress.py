from __future__ import annotations

import asyncio
import math
from contextlib import AsyncExitStack
from dataclasses import dataclass

from websockets.frames import CloseCode

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy

from tests.fake_server import FakePhoenixServer
from tests.support import TOPIC, each_protocol, make_client

# Clients sharing one API key, so the server lets only one stay connected.
SHARED_API_KEY = "shared-agent"


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


async def _run_contention_trial(
    phoenix_server: FakePhoenixServer,
    *,
    clients_n: int,
    duration_s: float,
) -> ContentionMetrics:
    phoenix_server.enforce_single_connection_per_api_key = True
    phoenix_server.connection_attempts_by_path.clear()

    policy = _stress_policy()
    clients: list[PHXChannelsClient] = []
    run_tasks: list[asyncio.Task[None]] = []

    try:
        for idx in range(clients_n):
            client = make_client(
                phoenix_server,
                f"/socket/stress-{idx}",
                api_key=SHARED_API_KEY,
                reconnect_policy=policy,
                join_timeout_s=1.0,
                leave_timeout_s=1.0,
            )
            await client.__aenter__()
            await client.subscribe_to_topic(TOPIC)
            clients.append(client)

        run_tasks = [asyncio.create_task(client.run_forever()) for client in clients]
        await asyncio.sleep(duration_s)
    finally:
        await asyncio.gather(
            *(client.shutdown("stress trial finished") for client in clients),
            return_exceptions=True,
        )
        await asyncio.gather(*run_tasks, return_exceptions=True)

    attempts = [
        phoenix_server.get_connection_attempts(f"/socket/stress-{idx}")
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

    assert max(metric.max_rate_per_s for metric in trials) <= 4.0
    assert max(metric.total_rate_per_s for metric in trials) <= 7.0
    assert min(metric.fairness for metric in trials) >= 0.7


async def test_duplicate_session_four_clients_stress_scales_without_cascade(
    phoenix_server: FakePhoenixServer,
) -> None:
    trials = [
        await _run_contention_trial(phoenix_server, clients_n=4, duration_s=3.0)
        for _ in range(3)
    ]

    assert max(metric.max_rate_per_s for metric in trials) <= 4.0
    assert max(metric.total_rate_per_s for metric in trials) <= 12.0
    assert min(metric.fairness for metric in trials) >= 0.6


# How long the two contending clients run before their attempts are counted.
CONTENTION_WINDOW_S = 1.2


@each_protocol
async def test_two_clients_contending_for_one_session_reconnect_at_a_bounded_fair_rate(
    phoenix_server: FakePhoenixServer,
):
    phoenix_server.enforce_single_connection_per_api_key = True
    phoenix_server.duplicate_close_code = CloseCode.TRY_AGAIN_LATER
    phoenix_server.duplicate_close_reason = "try again later"
    paths = ["/socket/websocket-a", "/socket/websocket-b"]
    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.1,
        stable_reset_s=1.0,
        service_restart_min_delay_s=0.01,
        service_restart_max_delay_s=0.03,
        try_again_later_min_delay_s=0.04,
        try_again_later_max_delay_s=0.08,
        rapid_disconnect_uptime_s=0.2,
        rapid_window_s=2.0,
        rapid_first_min_delay_s=0.02,
        rapid_second_min_delay_s=0.04,
        rapid_cooldown_base_s=0.06,
        rapid_cooldown_step_s=0.02,
        rapid_cooldown_max_s=0.1,
        rapid_suppress_disconnect_count=20,
        rapid_hold_down_jitter_low_ratio=0.2,
        rapid_hold_down_jitter_high_ratio=1.0,
    )
    clients = [
        make_client(
            phoenix_server,
            path,
            api_key=SHARED_API_KEY,
            reconnect_policy=policy,
            join_timeout_s=1.0,
            leave_timeout_s=1.0,
        )
        for path in paths
    ]

    async with AsyncExitStack() as stack:
        # Each joins before the next connects and evicts it.
        for client in clients:
            await stack.enter_async_context(client)
            await client.subscribe_to_topic(TOPIC)
        runs = [asyncio.create_task(client.run_forever()) for client in clients]
        # Counting attempts needs real elapsed time, not a condition.
        await asyncio.sleep(CONTENTION_WINDOW_S)

        await asyncio.gather(
            *(client.shutdown("test contention done") for client in clients),
            return_exceptions=True,
        )
        await asyncio.gather(*runs, return_exceptions=True)

    rates = [
        phoenix_server.get_connection_attempts(path) / CONTENTION_WINDOW_S
        for path in paths
    ]
    assert all(math.isfinite(rate) for rate in rates)
    assert max(rates) <= 30.0
    assert _jain_index(rates) >= 0.6
