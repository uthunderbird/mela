import typing

from .abc import AbstractRegistry

from .. import ConnectionDefinition
from .. import ConnectionRef
from .. import ExchangeDefinition
from .. import QueueDefinition
from .. import BindingDefinition
from .. import ConsumerDefinition
from .. import ProducerDefinition
from .. import ServiceDefinition


class ConnectionRegistry(AbstractRegistry):
    _definition_class = ConnectionDefinition

    def define(self, **kwargs) -> typing.Union[ConnectionDefinition, ConnectionRef]:
        if 'ref' in kwargs:
            return ConnectionRef(**kwargs)
        return super().define(**kwargs)


class ExchangeRegistry(AbstractRegistry):
    _definition_class = ExchangeDefinition


class QueueRegistry(AbstractRegistry):
    _definition_class = QueueDefinition


class BindingRegistry(AbstractRegistry):
    _definition_class = BindingDefinition


class ConsumerRegistry(AbstractRegistry):
    _definition_class = ConsumerDefinition


class PublisherRegistry(AbstractRegistry):
    _definition_class = ProducerDefinition


class ServiceRegistry(AbstractRegistry):
    _definition_class = ServiceDefinition
