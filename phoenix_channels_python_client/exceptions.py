class ClientError(Exception):
    pass


class ChannelError(ClientError):
    pass


class SocketError(ClientError):
    """Raised when there's an error connecting to the Phoenix WebSocket server."""


class PushTimeoutError(ChannelError):
    """Raised when a channel push receives no reply before its deadline."""
