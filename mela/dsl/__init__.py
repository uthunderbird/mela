import typing

from pydantic import AmqpDsn

from . import Processor
from .abc import AbstractDefinition
from .abc import AbstractNamedDefinition
from .abc import AbstractRef
from .processor import Processor


class ConnectionRef(AbstractRef):
    pass


class ConnectionDefinition(AbstractNamedDefinition):
    url: AmqpDsn

    def define(self, url=None, ref=None) -> typing.Union[typing.Self, ConnectionRef]:
        if ref is not None:
            assert url is None
            return ConnectionRef(ref=ref)
        assert url is not None
        return super().define(url=self.url)


class ExchangeDefinition(AbstractNamedDefinition):
    type: str
    durable: bool = True
    auto_delete: bool = False
    internal: bool = False
    arguments: dict = None


class QueueDefinition(AbstractNamedDefinition):
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    arguments: dict = None


class BindingDefinition(AbstractDefinition):
    exchange: ExchangeDefinition
    queue: QueueDefinition
    routing_key: str
    arguments: dict = None


class WithProcessor(AbstractDefinition):
    _processor: Processor | None = None

    def set_processor(self, processor: Processor):
        self._processor = processor

    def make_processor(self, callback, ):
        processor = Processor(callback)
        self.set_processor(processor)
        return processor

    class Config:
        arbitrary_types_allowed = True


class ConsumerDefinition(AbstractNamedDefinition):
    queue: QueueDefinition
    exchange: ExchangeDefinition
    bindings: typing.List[BindingDefinition]
    prefetch_count: int = 1


class ProducerDefinition(AbstractNamedDefinition):
    exchange: ExchangeDefinition
    default_routing_key: str = None


class ServiceDefinition(AbstractNamedDefinition, WithProcessor):
    consumer: ConsumerDefinition
    producer: ProducerDefinition
    processor: Processor | None = None
