from __future__ import annotations

import asyncio
import logging
from logging.handlers import BufferingHandler
from urllib.parse import parse_qsl, quote, urlsplit

import pytest
from websockets.asyncio.server import ServerConnection, serve

from phoenix_channels_python_client import PHXChannelsClient
from phoenix_channels_python_client.client_types import ReconnectPolicy
from phoenix_channels_python_client.diagnostics import ConnectionLogger


class TestConnectionSecurity:
    @pytest.mark.parametrize(
        "headers",
        [
            {"X-API-Key": ""},
            {"x-api-key": "  "},
            {"x-api-key": "one", "X-Api-Key": "two"},
        ],
    )
    def test_invalid_headers_are_rejected_before_network(
        self, headers: dict[str, str]
    ) -> None:
        with pytest.raises(ValueError, match="nonempty credential"):
            PHXChannelsClient(
                "ws://127.0.0.1:1/socket", "query-secret", additional_headers=headers
            )

    def test_mixed_authentication_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="token query authentication"):
            PHXChannelsClient(
                "ws://127.0.0.1:1/socket?token=jwt",
                "query-secret",
                additional_headers={"x-api-key": "header-secret"},
            )

    def test_diagnostic_levels_follow_live_configuration(self) -> None:
        target = logging.Logger("synthetic-target", logging.INFO)  # noqa: LOG001 -- isolated target avoids global state
        logger = ConnectionLogger(target, {"synthetic-credential"})
        collector = BufferingHandler(100)
        logger.addHandler(collector)
        logger.debug("initially suppressed")
        target.setLevel(logging.DEBUG)
        logger.debug("inherited debug")
        target.setLevel(logging.WARNING)
        logger.debug("inherited suppression")
        logger.setLevel(logging.DEBUG)
        logger.debug("local debug")
        logger.setLevel(logging.WARNING)
        logger.debug("local suppression")
        logger.setLevel(logging.DEBUG)
        previous_disable = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            logger.debug("globally suppressed")
            logging.disable(logging.NOTSET)
            logger.debug("globally restored")
        finally:
            logging.disable(previous_disable)
        assert [record.getMessage() for record in collector.buffer] == [
            "inherited debug",
            "local debug",
            "globally restored",
        ]

    def test_connections_keep_independent_diagnostic_policies(self) -> None:
        first = PHXChannelsClient("ws://127.0.0.1:1/socket", "first-credential")
        second = PHXChannelsClient("ws://127.0.0.1:1/socket", "second-credential")
        assert first.redact_diagnostic("first-credential") == "[REDACTED]"
        assert second.redact_diagnostic("first-credential") == "first-credential"
        assert second.redact_diagnostic("second-credential") == "[REDACTED]"
        assert first.redact_diagnostic("correlation%2fother") == "correlation%2fother"

    @pytest.mark.parametrize(
        "header_names", [("X-Api-Key",), ("x-api-key", "X-API-KEY")]
    )
    async def test_real_reconnect_uses_only_header_and_safe_raw_records(
        self, caplog: pytest.LogCaptureFixture, header_names: tuple[str, ...]
    ) -> None:
        secret = "synthetic/private+ credential?x=1&z=#[]"  # noqa: S105 -- synthetic test credential
        positional = "different-positional-secret"
        requests = []
        reconnected = asyncio.Event()

        async def peer(connection: ServerConnection) -> None:
            requests.append(connection.request)
            if len(requests) == 1:
                await connection.close(code=1012, reason=secret)
            else:
                reconnected.set()
                await connection.wait_closed()

        caplog.set_level(logging.DEBUG, logger="phoenix_channels_python_client.client")
        async with serve(peer, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}/socket?API%5FKEY=embedded-secret&api_key=embedded-secret&aPi_KeY=embedded-secret&token=&keep=one&keep=&vsn=old"
            async with PHXChannelsClient(
                url,
                positional,
                additional_headers={
                    **dict.fromkeys(header_names, secret),
                    "x-token": "synthetic-additional-token",
                    "x-auth-token": "synthetic-other-token",
                },
                heartbeat_interval_s=None,
                reconnect_policy=ReconnectPolicy(
                    base_delay_s=0.01,
                    service_restart_min_delay_s=0.01,
                    service_restart_max_delay_s=0.01,
                    rapid_first_min_delay_s=0.01,
                ),
            ) as client:
                collector = BufferingHandler(1000)
                client.logger.addHandler(collector)
                await asyncio.wait_for(reconnected.wait(), 5)
                client.logger.error(
                    "authorization: Bearer %s", "unconfigured-bearer-value"
                )
                client.logger.error(
                    '"authorization": "Basic %s"', "unconfigured-basic-value"
                )
                client.logger.error(
                    "untrusted %s", secret, extra={"websocket": {"secret": secret}}
                )
                assert client.redact_diagnostic(quote(secret, safe="")) == "[REDACTED]"
                assert client.redact_diagnostic(quote(secret)) == "[REDACTED]"
                assert (
                    client.redact_diagnostic(
                        quote(secret, safe="").replace("%2F", "%2f")
                    )
                    == "[REDACTED]"
                )
                try:
                    raise RuntimeError(secret)  # noqa: TRY301 -- exercise exception records
                except RuntimeError:
                    client.logger.exception(
                        "refused credential", extra={"websocket": secret}
                    )
        for request in requests:
            assert parse_qsl(urlsplit(request.path).query, keep_blank_values=True) == [
                ("keep", "one"),
                ("keep", ""),
                ("vsn", "2.0.0"),
            ]
            assert request.headers["x-api-key"] == secret
            assert request.headers["x-token"] == "synthetic-additional-token"
            assert request.headers["x-auth-token"] == "synthetic-other-token"
        records = [
            record
            for record in caplog.records
            if record.name == "phoenix_channels_python_client.client"
        ]
        assert records
        assert collector.buffer
        for record in [*records, *collector.buffer]:
            assert record.args == ()
            assert record.exc_info is None
            assert not hasattr(record, "websocket")
            for credential in (
                secret,
                positional,
                "embedded-secret",
                "synthetic-additional-token",
                "synthetic-other-token",
                "unconfigured-bearer-value",
                "unconfigured-basic-value",
                quote(secret, safe=""),
            ):
                assert credential not in repr(record.__dict__)
        assert any("OPEN" in record.getMessage() for record in records)
