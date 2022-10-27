from asyncio import AbstractEventLoop
from asyncio import Future
from asyncio import Lock

from json import JSONDecodeError
from json import loads
from typing import Optional
from typing import Type
from typing import Union
from uuid import uuid4

from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import AbstractMessage
from pydantic import BaseModel

from . import Consumer
from . import Publisher
from .base import ConsumingComponent
from .exceptions import NackMessageError
from ..abc import AbstractRPCClient
from ..processor import Processor


class RPC(ConsumingComponent):

    def __init__(
            self,
            name: str,
            log_level: str = 'info',
            *,
            worker: Optional[Consumer] = None,
            response_publisher: Optional[Publisher] = None,
            request_publisher: Optional[Publisher] = None,
    ):
        super().__init__(name, log_level)
        self._worker: Optional[Consumer] = None
        self._response_publisher: Optional[Publisher] = None
        self._request_publisher: Optional[Publisher] = None
        if worker:
            self._worker = worker
        if response_publisher:
            self._response_publisher = response_publisher
        if request_publisher:
            self._request_publisher = request_publisher
        self._client: Optional[RPCClient] = None

    def set_processor(self, processor: Processor):

        async def on_message(message: AbstractIncomingMessage) -> None:
            try:
                outgoing_message, _ = await processor.process(message)
                outgoing_message.correlation_id = message.correlation_id
                await self._response_publisher.publish_message(outgoing_message, routing_key=message.reply_to)
            except NackMessageError as e:
                await message.nack(requeue=e.requeue)
                self.log.exception("Message is Nacked:")
            except JSONDecodeError:
                self.log.exception("Message cannot be serialized, so we "
                                   "Nack it with requeue=False")
                await message.nack(requeue=False)
            except Exception:
                await message.nack(requeue=self._worker.requeue_broken_messages)
                self.log.exception("Message is broken:")
            else:
                await message.ack()
        self._worker.set_callback(on_message)

    @property
    def client(self):
        assert self._client is not None
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    async def consume(self, **kwargs) -> str:
        return await self._worker.consume(**kwargs)

    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        return await self._worker.cancel(timeout, nowait)


class RPCClient(ConsumingComponent, AbstractRPCClient):

    def __init__(
            self,
            name: str,
            log_level: str = 'info',
            loop: AbstractEventLoop = None,
            *,
            request_publisher: Optional[Publisher] = None,
            response_consumer: Optional[Consumer] = None,
            response_model: Optional[Type[BaseModel]] = None,
    ):
        super().__init__(
            name=name,
            loop=loop,
            log_level=log_level,
        )
        self._request_publisher: Optional[Publisher] = None
        if request_publisher:
            self._request_publisher = request_publisher
        if response_consumer:
            self._response_consumer = response_consumer
        self._response_model = response_model
        self._futures = {}
        self._consuming = Lock()

    @staticmethod
    def _generate_correlation_id():
        return str(uuid4())

    async def call(self, body: Union[AbstractMessage, BaseModel, dict]):
        assert self._consuming.locked(), "Consumer is not active"
        message, _ = Processor.wrap_response(body)
        message.correlation_id = self._generate_correlation_id()
        message.reply_to = self._response_consumer.get_queue_name()

        future = self.loop.create_future()
        self._futures[message.correlation_id] = future

        await self._request_publisher.publish_message(body)

        return await future

    def _prepare_callback(self):

        async def on_message(message: AbstractIncomingMessage) -> None:
            if message.correlation_id is None:
                raise KeyError("Message without correlation id")

            if self._response_model:
                parsed_response = self._response_model.parse_raw(message.body)
            else:
                parsed_response = loads(message.body)
            future: Future = self._futures.pop(message.correlation_id, None)
            if future is not None:
                future.set_result((parsed_response, message))

        self._response_consumer.set_callback(on_message)

    async def consume(self, **kwargs) -> str:
        await self._prepare_callback()
        await self._consuming.acquire()
        return await self._response_consumer.consume(**kwargs)

    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        self._consuming.release()
        return await self._response_consumer.cancel(timeout, nowait)

    def set_processor(self, processor):
        pass
