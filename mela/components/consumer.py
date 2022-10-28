from json import JSONDecodeError
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Optional

from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import AbstractQueue

from mela.components.base import ConsumingComponent
from mela.components.exceptions import NackMessageError
from mela.processor import Processor


class Consumer(ConsumingComponent):

    def __init__(
            self,
            name: str,
            prefetch_count: int = 1,
            timeout: Optional[int] = None,
            no_ack: bool = False,
            exclusive: bool = False,
            consumer_tag: Optional[str] = None,
            requeue_broken_messages: bool = True,
            log_level: str = 'info',
            *,
            queue: Optional[AbstractQueue] = None,
    ):
        super().__init__(name, log_level)
        self._prefetch_count: int = prefetch_count
        self._timeout: Optional[int] = timeout
        self._no_ack: bool = no_ack
        self._exclusive: bool = exclusive
        self._consumer_tag: Optional[str] = consumer_tag
        self._queue: Optional[AbstractQueue] = None
        self.requeue_broken_messages = requeue_broken_messages
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

    def get_queue_name(self) -> str:
        assert self._queue
        return self._queue.name

    def set_processor(self, processor: Processor):
        self._processor = processor

        async def wrapper(message: AbstractIncomingMessage):
            try:
                await processor.process(message)
            except NackMessageError as e:
                await message.nack(requeue=e.requeue)
                self.log.exception("Message is Nacked:")
            except JSONDecodeError:
                self.log.exception("Message cannot be serialized, so we "
                                   "Nack it with requeue=False")
                await message.nack(requeue=False)
            except Exception:
                await message.nack(requeue=self.requeue_broken_messages)
                self.log.exception("Message is broken:")
            else:
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
