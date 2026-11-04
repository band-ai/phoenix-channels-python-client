from __future__ import annotations

import json
import logging
import re
from enum import Enum
from urllib.parse import parse_qsl, quote, quote_plus, urlsplit

from websockets.protocol import State

from phoenix_channels_python_client.authentication import (
    API_KEY_PARAMETER,
    TOKEN_PARAMETER,
    AuthenticationHeader,
)

REDACTED = "[REDACTED]"
OMITTED = "<omitted>"
PERCENT_ESCAPE = re.compile(r"%[0-9a-fA-F]{2}")
# A printf-style conversion, as logging applies to a record's msg and args.
PLACEHOLDER = re.compile(
    r"%(?:\([^)]*\))?[#0 +-]*(?:\*|\d+)?(?:\.(?:\*|\d+))?[hlL]?[diouxXeEfFgGcrsa%]"
)
WEBSOCKET_STATE_MESSAGE = "= connection is %s"


def render_value(value: object) -> str:
    # Enum members and booleans are code-defined; any other value, numbers
    # included, may carry personal data.
    return str(value) if isinstance(value, Enum | bool) else OMITTED


def render_message(record: logging.LogRecord) -> str:
    """Fill a record's placeholders without exposing its arguments."""
    message = str(record.msg)
    if not record.args:
        return message
    args = record.args if isinstance(record.args, tuple) else ()
    if message == WEBSOCKET_STATE_MESSAGE and args and args[0] in State.__members__:
        return message % args[0]
    values = iter(args)
    return PLACEHOLDER.sub(
        lambda match: "%" if match[0] == "%%" else render_value(next(values, None)),
        message,
    )


AUTHENTICATION_NAMES = frozenset(
    {
        API_KEY_PARAMETER,
        TOKEN_PARAMETER,
        *AuthenticationHeader,
        "x-token",
        "authorization",
        "cookie",
    }
)
AUTHENTICATION_LABEL = re.compile(
    r"(?i)((?:"
    + "|".join(
        re.escape(name) for name in sorted(AUTHENTICATION_NAMES, key=len, reverse=True)
    )
    + r")[\"\']?\s*[:=]\s*[\"\']?(?:(?:Bearer|Basic)\s+)?)([^\s&\"\']+)"
)


class ConnectionLogger(logging.Logger):
    """Forward only sanitized records; raw transport objects never reach handlers."""

    def __init__(self, target: logging.Logger, secrets: set[str]) -> None:
        super().__init__(target.name)
        self.target = target
        self.parent = target
        variants = sorted(
            {
                encoded
                for secret in secrets
                if secret
                for encoded in (
                    secret,
                    quote(secret, safe=""),
                    quote(secret),
                    quote(secret, safe=";/?:@&=+$,#-_.!~*'()"),
                    quote_plus(secret, safe=""),
                    json.dumps(secret)[1:-1],
                )
            },
            key=len,
            reverse=True,
        )

        self.patterns = [
            re.compile(
                PERCENT_ESCAPE.sub(lambda match: f"(?i:{match[0]})", re.escape(secret))
            )
            for secret in variants
        ]

    def isEnabledFor(self, level: int) -> bool:  # noqa: N802 -- logging API
        # Connection loggers are intentionally absent from the global registry,
        # so its cache invalidation cannot track their effective level.
        return (
            not self.disabled
            and not self.target.disabled
            and self.manager.disable < level
            and level >= self.getEffectiveLevel()
        )

    def redact(self, text: str) -> str:
        for pattern in self.patterns:
            text = pattern.sub(REDACTED, text)
        return AUTHENTICATION_LABEL.sub(lambda match: match[1] + REDACTED, text)

    def handle(self, record: logging.LogRecord) -> None:
        if self.target.disabled:
            return
        message = render_message(record)
        if record.exc_info and record.exc_info[0]:
            message += " [" + record.exc_info[0].__name__ + "]"
        safe_record = logging.LogRecord(
            record.name,
            record.levelno,
            record.pathname,
            record.lineno,
            self.redact(message),
            (),
            None,
            record.funcName,
        )
        filtered = self.target.filter(safe_record)
        if filtered:
            if isinstance(filtered, logging.LogRecord):
                safe_record = filtered
            super().handle(safe_record)


def diagnostic_secrets(
    url: str, api_key: str | None, headers: dict[str, str]
) -> set[str]:
    """Collect connection credentials without changing authentication bytes."""
    return {
        *([api_key] if api_key is not None else []),
        *(
            value
            for name, value in parse_qsl(urlsplit(url).query)
            if name.casefold() in AUTHENTICATION_NAMES
        ),
        *(
            value
            for name, value in headers.items()
            if name.casefold() in AUTHENTICATION_NAMES
        ),
    }
