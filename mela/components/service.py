from json import JSONDecodeError
from typing import Optional

from aio_pika.abc import AbstractIncomingMessage

from mela.components import Consumer
from mela.components import Publisher
from mela.components.base import ConsumingComponent
from mela.components.exceptions import NackMessageError
from mela.processor import Processor


class Service(ConsumingComponent):

    def __init__(
            self,
            name: str,
            log_level: str = 'info',
            *,
            publisher: Optional[Publisher] = None,
            consumer: Optional[Consumer] = None,
    ):
        super().__init__(name, log_level)
        self._consumer: Optional[Consumer] = None
        self._publisher: Optional[Publisher] = None
        if consumer:
            self.consumer = consumer
        if publisher:
            self.publisher = publisher

    def set_processor(self, processor: Processor):
        self._processor = processor

        async def on_message(message: AbstractIncomingMessage) -> None:
            try:
                outgoing_message, routing_key = await processor.process(message)
                await self.publisher.publish_message(outgoing_message, routing_key=routing_key)
            except NackMessageError as e:
                await message.nack(requeue=e.requeue)
                self.log.exception("Message is Nacked:")
            except JSONDecodeError:
                self.log.exception("Message cannot be serialized, so we "
                                   "Nack it with requeue=False")
                await message.nack(requeue=False)
            except Exception:
                await message.nack(requeue=self.consumer.requeue_broken_messages)
                self.log.exception("Message is broken:")
            else:
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
