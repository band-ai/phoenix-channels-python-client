from __future__ import annotations

from phoenix_channels_python_client.client_types import ClientState


def transition_client_state(current: ClientState, new_state: ClientState) -> ClientState:
    if current == new_state:
        return current

    allowed_transitions: dict[ClientState, set[ClientState]] = {
        ClientState.CLOSED: {ClientState.CONNECTING},
        ClientState.CONNECTING: {
            ClientState.CONNECTED,
            ClientState.RECONNECTING,
            ClientState.SHUTTING_DOWN,
            ClientState.CLOSED,
        },
        ClientState.CONNECTED: {
            ClientState.RECONNECTING,
            ClientState.SHUTTING_DOWN,
            ClientState.CLOSED,
        },
        ClientState.RECONNECTING: {
            ClientState.CONNECTED,
            ClientState.SHUTTING_DOWN,
            ClientState.CLOSED,
        },
        ClientState.SHUTTING_DOWN: {ClientState.CLOSED},
    }

    if new_state not in allowed_transitions[current]:
        raise RuntimeError(f"Invalid state transition {current.value} -> {new_state.value}")

    return new_state
