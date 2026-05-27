from __future__ import annotations

from collections import deque

import pytest

from phoenix_channels_python_client.client import PHXChannelsClient, ReconnectPolicy
from phoenix_channels_python_client.exceptions import PHXConnectionError


def _make_client(policy: ReconnectPolicy | None = None) -> PHXChannelsClient:
    return PHXChannelsClient(
        "ws://example.invalid/socket/websocket",
        api_key="test-key",
        reconnect_policy=policy or ReconnectPolicy(),
    )


def test_invalid_reconnect_policy_is_rejected() -> None:
    bad_policy = ReconnectPolicy(
        service_restart_min_delay_s=2.0,
        service_restart_max_delay_s=1.0,
    )
    with pytest.raises(ValueError, match="Invalid reconnect policy"):
        _ = _make_client(bad_policy)


def test_client_keeps_api_key_out_of_socket_urls() -> None:
    client = _make_client()
    assert "vsn=2.0.0" in client.channel_socket_url
    assert "api_key" not in client.channel_socket_url
    assert "test-key" not in client.channel_socket_url
    assert "api_key" not in client.channel_socket_url_redacted
    assert "test-key" not in client.channel_socket_url_redacted
    assert client.channel_socket_headers == {"x-api-key": "test-key"}


def test_client_strips_stale_api_key_from_configured_socket_url() -> None:
    client = PHXChannelsClient(
        "ws://example.invalid/socket/websocket?api_key=stale-key&debug=true",
        api_key="test-key",
    )
    assert "debug=true" in client.channel_socket_url
    assert "vsn=2.0.0" in client.channel_socket_url
    assert "api_key" not in client.channel_socket_url
    assert "stale-key" not in client.channel_socket_url
    assert "test-key" not in client.channel_socket_url


def test_close_code_classification_uses_expected_semantics() -> None:
    policy = ReconnectPolicy(
        reconnect_on_normal_close=False,
        policy_violation_is_terminal=True,
        service_restart_min_delay_s=0.5,
        service_restart_max_delay_s=1.0,
        try_again_later_min_delay_s=2.0,
        try_again_later_max_delay_s=4.0,
    )
    client = _make_client(policy)

    normal = client._classify_disconnect(1000, "normal")
    assert normal.should_reconnect is False
    assert normal.terminal_error is None

    policy_violation = client._classify_disconnect(1008, "policy")
    assert policy_violation.should_reconnect is False
    assert isinstance(policy_violation.terminal_error, PHXConnectionError)

    service_restart = client._classify_disconnect(1012, "restart")
    assert service_restart.should_reconnect is True
    assert service_restart.min_delay_s == 0.5
    assert service_restart.max_delay_s == 1.0

    try_again_later = client._classify_disconnect(1013, "busy")
    assert try_again_later.should_reconnect is True
    assert try_again_later.min_delay_s == 2.0
    assert try_again_later.max_delay_s == 4.0


def test_linear_rapid_cooldown_floor_is_monotonic_until_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.1,
        rapid_cooldown_base_s=0.5,
        rapid_cooldown_step_s=0.25,
        rapid_cooldown_max_s=1.0,
        rapid_hold_down_jitter_low_ratio=1.0,
        rapid_hold_down_jitter_high_ratio=1.0,
    )
    client = _make_client(policy)
    monkeypatch.setattr(
        "phoenix_channels_python_client.reconnect_controller.random.random", lambda: 1.0
    )

    client._rapid_disconnects = deque([1.0, 2.0, 3.0])
    delay_3 = client._compute_reconnect_delay(attempt=0)
    client._rapid_disconnects = deque([1.0, 2.0, 3.0, 4.0])
    delay_4 = client._compute_reconnect_delay(attempt=0)
    client._rapid_disconnects = deque([1.0, 2.0, 3.0, 4.0, 5.0])
    delay_5 = client._compute_reconnect_delay(attempt=0)

    assert delay_3 == 0.5
    assert delay_4 == 0.75
    assert delay_5 == 1.0
    assert delay_3 <= delay_4 <= delay_5


def test_hold_down_jitter_respects_configured_band(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = ReconnectPolicy(
        base_delay_s=0.01,
        factor=2.0,
        max_delay_s=0.1,
        rapid_cooldown_base_s=0.6,
        rapid_cooldown_step_s=0.2,
        rapid_cooldown_max_s=2.0,
        rapid_hold_down_jitter_low_ratio=0.25,
        rapid_hold_down_jitter_high_ratio=1.0,
    )
    client = _make_client(policy)
    client._rapid_disconnects = deque([1.0, 2.0, 3.0, 4.0])

    monkeypatch.setattr(
        "phoenix_channels_python_client.reconnect_controller.random.random", lambda: 0.0
    )
    delay_low = client._compute_reconnect_delay(attempt=0)

    monkeypatch.setattr(
        "phoenix_channels_python_client.reconnect_controller.random.random", lambda: 1.0
    )
    delay_high = client._compute_reconnect_delay(attempt=0)

    # rapid_count=4 => floor = 0.8
    assert delay_low == 0.2
    assert delay_high == 0.8
