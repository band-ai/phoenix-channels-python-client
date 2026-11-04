from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, TypeVar, overload

from phoenix_channels_python_client.messages import ChannelEvent, Message
from phoenix_channels_python_client.topic_subscription import TopicSubscription

if TYPE_CHECKING:
    from phoenix_channels_python_client.topic_runtime import TopicRuntimeMixin

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]
MessageHandler = Callable[[Message], Awaitable[None]]
H = TypeVar("H", bound=EventHandler)
M = TypeVar("M", bound=MessageHandler)


class Channel:
    def __init__(
        self, client: TopicRuntimeMixin, subscription: TopicSubscription
    ) -> None:
        self.client = client
        self.subscription = subscription

    @property
    def topic(self) -> str:
        return self.subscription.name

    @property
    def joined(self) -> bool:
        return self.client._channel_joined(self.subscription)

    @overload
    def on(self, event: ChannelEvent, handler: H) -> H: ...

    @overload
    def on(self, event: ChannelEvent, handler: None = None) -> Callable[[H], H]: ...

    def on(
        self, event: ChannelEvent, handler: EventHandler | None = None
    ) -> EventHandler | Callable[[H], H]:
        def register(callback: H) -> H:
            self.subscription.add_event_handler(event, callback)
            return callback

        if handler is not None:
            self.subscription.add_event_handler(event, handler)
            return handler
        return register

    def off(self, event: ChannelEvent) -> None:
        self.subscription.remove_event_handler(event)

    def on_any(self, handler: M) -> M:
        self.subscription.async_callback = handler
        return handler

    def off_any(self) -> None:
        self.subscription.async_callback = None

    async def join(self) -> None:
        await self.client._join_channel(self.subscription)

    async def leave(self) -> None:
        await self.client._leave_channel(self.subscription)

    async def __aenter__(self) -> Self:
        await self.join()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.leave()
        except Exception:
            if exc_type is None or not issubclass(exc_type, asyncio.CancelledError):
                raise
            self.client.logger.exception("Channel cleanup failed during cancellation")
