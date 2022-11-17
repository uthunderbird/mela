import asyncio
from typing import Dict
from typing import Optional
from typing import Union

from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractMessage
from aiormq.abc import ConfirmationFrameType
from pydantic import BaseModel

from ..abc import AbstractPublisher
from ..components.base import Component
from ..processor import Processor


class Publisher(Component, AbstractPublisher):

    def __init__(
            self,
            name: str,
            default_routing_key: str = '',
            default_timeout: int = None,
            log_level: str = 'info',
            *,
            exchange: Optional[AbstractExchange] = None,
            channel: Optional[AbstractChannel] = None,
    ):
        super().__init__(name, log_level)
        self._default_routing_key = default_routing_key
        self._default_timeout = default_timeout
        self._exchange: Optional[AbstractExchange] = None
        if exchange:
            self.set_exchange(exchange)
        self._channel = channel

    def set_exchange(self, exchange: AbstractExchange):
        assert self._exchange is None, "Exchange already is set"
        self._exchange = exchange

    async def publish_message(
        self,
        message: AbstractMessage,
        routing_key: str = None,
        timeout: int = None,
    ) -> Optional[ConfirmationFrameType]:
        assert self._exchange
        if routing_key is None:
            routing_key = self._default_routing_key
        if timeout is None:
            timeout = self._default_timeout
        while self._channel.is_closed:
            # Hacky way to avoid ChannelInvalidStateError
            # See https://github.com/mosquito/aio-pika/issues/508
            await asyncio.sleep(0.001)
        return await self._exchange.publish(message, routing_key, timeout=timeout)

    async def publish(
            self,
            message: Union[Dict, BaseModel, AbstractMessage],
            routing_key: Optional[str] = None,
    ):
        message, routing_key = Processor.wrap_response(message, routing_key)
        return await self.publish_message(message, routing_key)
