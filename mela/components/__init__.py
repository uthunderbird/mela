from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Optional

from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import AbstractMessage
from aio_pika.abc import AbstractQueue
from aiormq.abc import ConfirmationFrameType

from ..processor import Processor


class Component:
    pass


class Publisher(Component):

    def __init__(
            self,
            name: str,
            default_routing_key: str = '',
            default_timeout: int = None,
            *,
            exchange: Optional[AbstractExchange] = None,
    ):
        self.name: str = name
        self._default_routing_key = default_routing_key
        self._default_timeout = default_timeout
        self._exchange: Optional[AbstractExchange] = None
        if exchange:
            self.set_exchange(exchange)

    def set_exchange(self, exchange: AbstractExchange):
        assert self._exchange is None, "Exchange already is set"
        self._exchange = exchange

    async def publish(
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
        return await self._exchange.publish(message, routing_key, timeout=timeout)


class Consumer(Component):

    def __init__(
            self,
            name: str,
            prefetch_count: int = 1,
            timeout: Optional[int] = None,
            no_ack: bool = False,
            exclusive: bool = False,
            consumer_tag: Optional[str] = None,
            *,
            queue: Optional[AbstractQueue] = None,
    ):
        self.name: str = name
        self._prefetch_count: int = prefetch_count
        self._timeout: Optional[int] = timeout
        self._no_ack: bool = no_ack
        self._exclusive: bool = exclusive
        self._consumer_tag: Optional[str] = consumer_tag
        self._queue: Optional[AbstractQueue] = None
        if queue:
            self.set_queue(queue)

        self._callback: Optional[
            Callable[
                [
                    AbstractIncomingMessage,
                ],
                Coroutine[
                    Any,
                    Any,
                    None,
                ],
            ],
        ] = None

    def set_queue(self, queue: AbstractQueue):
        self._queue = queue

    def set_processor(self, processor: Processor):

        async def wrapper(message: AbstractIncomingMessage):
            await processor.process(message)
            await message.ack()

        self.set_callback(wrapper)

    def set_callback(self, func: Callable[[AbstractIncomingMessage], Coroutine[Any, Any, None]]):
        self._callback = func

    async def consume(self, **kwargs) -> str:
        assert self._callback is not None, "We can't start without a processor, dude"
        assert self._queue is not None, "Queue is not set"
        consumer_tag = await self._queue.consume(
            callback=self._callback,
            no_ack=self._no_ack,
            exclusive=self._exclusive,
            arguments=kwargs,
            consumer_tag=self._consumer_tag,
            timeout=self._timeout,
        )
        self._consumer_tag = consumer_tag
        return consumer_tag

    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        assert self._consumer_tag
        assert self._queue
        return await self._queue.cancel(self._consumer_tag, timeout, nowait)


class Service(Component):

    def __init__(
            self,
            name: str,
            *,
            publisher: Optional[Publisher] = None,
            consumer: Optional[Consumer] = None,
    ):
        self.name: str = name
        self._consumer: Optional[Consumer] = None
        self._publisher: Optional[Publisher] = None
        if consumer:
            self.consumer = consumer
        if publisher:
            self.publisher = publisher

    def set_processor(self, processor: Processor):

        async def on_message(message: AbstractIncomingMessage) -> None:
            outgoing_message, routing_key = await processor.process(message)
            await self.publisher.publish(outgoing_message, routing_key=routing_key)
            await message.ack()
        self.consumer.set_callback(on_message)

    @property
    def consumer(self) -> Consumer:
        if self._consumer is None:
            raise RuntimeError("Consumer is not set")
        return self._consumer

    @consumer.setter
    def consumer(self, value: Consumer):
        if self._consumer:
            raise RuntimeError("Consumer already set")
        self._consumer = value

    @property
    def publisher(self) -> Publisher:
        if self._publisher is None:
            raise RuntimeError("Publisher is not set")
        return self._publisher

    @publisher.setter
    def publisher(self, value: Publisher):
        if self._publisher:
            raise RuntimeError("Publisher already set")
        self._publisher = value

    async def consume(self, **kwargs) -> str:
        return await self.consumer.consume(**kwargs)

    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        return await self.consumer.cancel(timeout, nowait)
