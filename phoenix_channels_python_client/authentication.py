from __future__ import annotations

from enum import StrEnum

API_KEY_PARAMETER = "api_key"
TOKEN_PARAMETER = "token"  # noqa: S105 -- protocol parameter name


class AuthenticationHeader(StrEnum):
    API_KEY = "x-api-key"
    TOKEN = "x-auth-token"  # noqa: S105 -- protocol header name


def normalize_headers(headers: dict[str, str] | None) -> dict[str, str]:
    normalized = dict(headers or {})
    for header in AuthenticationHeader:
        credentials = [
            value for name, value in normalized.items() if name.casefold() == header
        ]
        if not credentials:
            continue
        if (
            any(not value.strip() for value in credentials)
            or len(set(credentials)) != 1
        ):
            raise ValueError(f"{header} headers must contain one nonempty credential")
        normalized = {
            name: value
            for name, value in normalized.items()
            if name.casefold() != header
        }
        normalized[header] = credentials[0]
    return normalized


def select_authentication_header(
    headers: dict[str, str],
) -> AuthenticationHeader | None:
    # The platform gives a supplied JWT precedence over an API key.
    for header in (AuthenticationHeader.TOKEN, AuthenticationHeader.API_KEY):
        if header in headers:
            return header
    return None
