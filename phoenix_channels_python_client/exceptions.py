class PHXClientError(Exception):
    pass


class PHXTopicError(PHXClientError):
    pass


class PHXConnectionError(PHXClientError):
    """Raised when there's an error connecting to the Phoenix WebSocket server."""

    pass
