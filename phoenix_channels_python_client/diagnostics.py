from __future__ import annotations

import json
import logging
import re
import traceback
from urllib.parse import parse_qsl, quote, quote_plus, urlsplit

from phoenix_channels_python_client.authentication import (
    API_KEY_PARAMETER,
    TOKEN_PARAMETER,
    AuthenticationHeader,
)

REDACTED = "[REDACTED]"
PERCENT_ESCAPE = re.compile(r"%[0-9a-fA-F]{2}")


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
            and self.manager.disable < level
            and level >= self.getEffectiveLevel()
        )

    def redact(self, text: str) -> str:
        for pattern in self.patterns:
            text = pattern.sub(REDACTED, text)
        return AUTHENTICATION_LABEL.sub(lambda match: match[1] + REDACTED, text)

    def handle(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if record.exc_info:
            message += "\n" + "".join(traceback.format_exception(*record.exc_info))
        if record.stack_info:
            message += "\n" + record.stack_info
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
        super().handle(safe_record)


def diagnostic_secrets(url: str, api_key: str, headers: dict[str, str]) -> set[str]:
    """Collect connection credentials without changing authentication bytes."""
    return {
        api_key,
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
