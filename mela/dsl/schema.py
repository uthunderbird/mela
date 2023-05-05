from mela.dsl import AbstractNamedDefinition
from mela.dsl.registries import BindingRegistry
from mela.dsl.registries import ConnectionRegistry
from mela.dsl.registries import ConsumerRegistry
from mela.dsl.registries import ExchangeRegistry
from mela.dsl.registries import PublisherRegistry
from mela.dsl.registries import QueueRegistry
from mela.dsl.registries import ServiceRegistry


class Schema(AbstractNamedDefinition):

    connections: ConnectionRegistry = ConnectionRegistry()
    exchanges: ExchangeRegistry = ExchangeRegistry()
    queues: QueueRegistry = QueueRegistry()
    bindings: BindingRegistry = BindingRegistry()
    consumers: ConsumerRegistry = ConsumerRegistry()
    producers: PublisherRegistry = PublisherRegistry()
    services: ServiceRegistry = ServiceRegistry()

    def __init__(self, name):
        super().__init__(name=name)

    class Config:
        arbitrary_types_allowed = True
