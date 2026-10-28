"""Physical cleanup remains owned across terminal close and client re-entry."""

from __future__ import annotations

import asyncio

from tests.cleanup_support import terminal_cleanup
from tests.fake_server import FakePhoenixServer
from tests.join_support import JoinProbe, deadline, push
from tests.support import STOP_REASON, TOPIC, each_protocol, make_client


@each_protocol
class TestCleanupOwnership:
    async def test_shutdown_after_terminal_close_awaits_removed_callback_finalizer(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        started = asyncio.Event()
        async with deadline(), terminal_cleanup(phoenix_server) as held:

            async def shutdown() -> None:
                started.set()
                await held.client.shutdown(STOP_REASON)

            async with asyncio.TaskGroup() as tasks:
                stopping = tasks.create_task(shutdown())
                await started.wait()
                async with make_client(phoenix_server) as independent:
                    probe = JoinProbe(independent)
                    await independent.subscribe_to_topic(
                        TOPIC, probe.receive, on_join_response=probe.install
                    )
                    _, request = await phoenix_server.joined.get()
                    await push(phoenix_server, request)
                    await probe.delivered.get()
                assert not stopping.done()
                assert not held.finished.is_set()
                held.release.set()
                await stopping
                assert held.finished.is_set()

    async def test_context_exit_after_terminal_close_awaits_removed_finalizer(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        started = asyncio.Event()
        async with deadline(), terminal_cleanup(phoenix_server) as held:

            async def exit_context() -> None:
                started.set()
                await held.client.__aexit__(None, None, None)

            async with asyncio.TaskGroup() as tasks:
                exiting = tasks.create_task(exit_context())
                await started.wait()
                async with make_client(phoenix_server) as independent:
                    probe = JoinProbe(independent)
                    await independent.subscribe_to_topic(
                        TOPIC, probe.receive, on_join_response=probe.install
                    )
                    _, request = await phoenix_server.joined.get()
                    await push(phoenix_server, request)
                    await probe.delivered.get()
                assert not exiting.done()
                assert not held.finished.is_set()
                held.release.set()
                await exiting
                assert held.finished.is_set()

    async def test_reentry_installs_fresh_state_without_releasing_old_cleanup(
        self, phoenix_server: FakePhoenixServer
    ) -> None:
        started = asyncio.Event()
        async with deadline(), terminal_cleanup(phoenix_server) as held:
            await held.client.__aenter__()
            probe = JoinProbe(held.client)
            await held.client.subscribe_to_topic(
                TOPIC, probe.receive, on_join_response=probe.install
            )
            _, request = await phoenix_server.joined.get()
            await push(phoenix_server, request, payload={"fresh": True})
            await probe.delivered.get()
            assert probe.order == [{}, {"fresh": True}]
            assert not held.finished.is_set()

            async def shutdown() -> None:
                started.set()
                await held.client.shutdown(STOP_REASON)

            async with asyncio.TaskGroup() as tasks:
                stopping = tasks.create_task(shutdown())
                await started.wait()
                assert not stopping.done()
                held.release.set()
                await stopping
                assert held.finished.is_set()
