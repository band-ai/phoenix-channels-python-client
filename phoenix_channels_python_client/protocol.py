from enum import StrEnum


class ProtocolVersion(StrEnum):
    V1 = "1.0"
    V2 = "2.0"

    @property
    def vsn(self) -> str:
        return _PROTOCOL_VSN[self]


_PROTOCOL_VSN = {
    ProtocolVersion.V1: "1.0.0",
    ProtocolVersion.V2: "2.0.0",
}

DEFAULT_PROTOCOL_VERSION = ProtocolVersion.V2
