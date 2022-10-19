import abc
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import envyaml
from aio_pika import ExchangeType
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractQueue
from pydantic import AmqpDsn
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import Extra
from pydantic import Field
from pydantic import PrivateAttr
from pydantic.env_settings import SettingsSourceCallable


def yaml_config_settings_source(settings: 'BaseSettings') -> Dict[str, Any]:
    """
    A simple settings source that loads variables from a YAML file
    at the project's root.
    """
    yaml_config = envyaml.EnvYAML(
        settings.Config.yaml_file_path,  # type: ignore
        include_environment=False,
    )
    clear_config = {}
    for key, value in dict(yaml_config).items():
        if '.' not in key:
            clear_config[key] = value
    return clear_config


class AbstractConnectionParams(BaseModel, abc.ABC):
    name: Optional[str] = None

    @abc.abstractmethod
    def get_params_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

    class Config:
        extra = Extra.forbid


class ConnectionParams(AbstractConnectionParams):
    host: str
    port: int
    login: str = Field(alias='username')
    password: str
    virtualhost: str = '/'
    ssl: bool = False
    ssl_options = dict
    timeout: Optional[Union[float, int]] = Field(default=None, alias='connTimeout')
    client_properties: Optional[Dict] = None
    heartbeat: Optional[int] = None

    def get_params_dict(self):
        if self.name:
            self.client_properties = {'connection_name': self.name}
        res = self.dict(exclude={'name'})
        return res


class URLConnectionParams(AbstractConnectionParams):

    url: AmqpDsn

    def get_params_dict(self):
        return self.dict(exclude={'name'})


class ExchangeParams(BaseModel):
    _instance: Optional[AbstractExchange] = PrivateAttr(default=None)

    name: str
    type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    internal: bool = False
    passive: bool = False
    arguments: Optional[Dict] = None
    timeout: Optional[Union[float, int]] = None

    def get_params_dict(self):
        return self.dict()


class QueueParams(BaseModel):
    _instance: Optional[AbstractQueue] = PrivateAttr(default=None)

    name: str
    durable: bool = True
    exclusive: bool = False
    passive: bool = False
    auto_delete: bool = False
    dead_letter_exchange: Optional[Union[str, ExchangeParams]] = None
    dead_letter_exchange_type: ExchangeType = ExchangeType.DIRECT
    dead_letter_routing_key: Optional[str] = None

    def solve_dead_letter_exchange(self, exchanges: Dict['str', 'ExchangeParams']):
        if isinstance(self.dead_letter_exchange, str):
            exchange = exchanges.get(self.dead_letter_exchange)
            if not exchange:
                exchange = ExchangeParams(
                    name=self.dead_letter_exchange,
                    type=self.dead_letter_exchange_type,
                )
            self.dead_letter_exchange = exchange

    def get_params_dict(self):
        arguments = {}
        if self.dead_letter_exchange:
            arguments['x-dead-letter-exchange'] = self.dead_letter_exchange
            arguments['x-dead-letter-routing-key'] = self.dead_letter_routing_key
        return {
            'name': self.name,
            'durable': self.durable,
            'exclusive': self.exclusive,
            'passive': self.passive,
            'auto_delete': self.auto_delete,
            'arguments': arguments,
        }


class ComponentParamsBaseModel(BaseModel, abc.ABC):
    name: Optional[str] = None

    @abc.abstractmethod
    def solve(self, settings: 'Settings'):
        raise NotImplementedError

    @abc.abstractmethod
    def get_params_dict(self) -> Dict:
        raise NotImplementedError


class PublisherParams(ComponentParamsBaseModel):
    connection: Union[str, ConnectionParams, URLConnectionParams] = 'default'
    exchange: Union[str, ExchangeParams]
    exchange_type: str = 'direct'  # DEPRECATED will be deleted in v1.2.0
    routing_key: str
    skip_unroutables: bool = False
    queue: Optional[Union[str, QueueParams]] = None
    timeout: Optional[Union[int, float]] = None

    def solve_connection(
            self,
            connections: Dict[str, Union[ConnectionParams, URLConnectionParams]],
    ) -> None:
        if isinstance(self.connection, str):
            if self.connection not in connections:
                raise KeyError(f"Connection `{self.connection}` is not described in config")
            self.connection = connections[self.connection]

    def solve_exchange(self, exchanges: Dict[str, ExchangeParams]) -> None:
        if isinstance(self.exchange, str):
            exchange = exchanges.get(self.exchange)
            if not exchange:
                exchange = ExchangeParams(name=self.exchange, type=self.exchange_type)
                exchanges[self.exchange] = exchange
            else:
                if self.exchange_type != exchange.type:
                    raise AssertionError("Exchange is configured with two different types")
            self.exchange = exchange

    def solve_queue(self, queues: Dict[str, QueueParams]):
        if isinstance(self.queue, str):
            queue = queues.get(self.queue)
            if queue is None:
                queue = QueueParams(name=self.queue)
                queues[self.queue] = queue
            self.queue = queue

    def solve(self, settings: 'Settings', parent_name: Optional[str] = None):
        self.solve_connection(settings.connections)
        self.solve_exchange(settings.exchanges)
        if self.queue:
            self.solve_queue(settings.queues)
        if parent_name and self.name is None:
            self.name = parent_name + '_publisher'

    def get_params_dict(self):
        return {
            'name': self.name,
            'default_timeout': self.timeout,
            'default_routing_key': self.routing_key,
        }


class ConsumerParams(ComponentParamsBaseModel):
    connection: Union[str, ConnectionParams, URLConnectionParams] = 'default'
    exchange: Union[str, ExchangeParams]
    exchange_type: str = 'direct'  # DEPRECATED will be deleted in v1.2.0
    routing_key: str
    queue: Union[str, QueueParams]
    prefetch_count: int = 1
    dead_letter_exchange: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None
    requeue_broken_messages: bool = False

    def solve_connection(
        self,
        connections: Dict[str, Union[ConnectionParams, URLConnectionParams]],
    ) -> None:
        if isinstance(self.connection, str):
            if self.connection not in connections:
                raise KeyError(f"Connection `{self.connection}` is not described in config")
            self.connection = connections[self.connection]

    def solve_exchange(self, exchanges: Dict[str, ExchangeParams]) -> None:
        if isinstance(self.exchange, str):
            exchange = exchanges.get(self.exchange)
            if self.exchange_type != 'direct':
                # TODO deprecation warning
                pass
            if not exchange:
                exchange = ExchangeParams(name=self.exchange, type=self.exchange_type)
            else:
                if self.exchange_type != exchange.type:
                    raise AssertionError("Exchange is configured with two different types")
            self.exchange = exchange

    def solve_queue(self, queues: Dict[str, QueueParams]):
        if isinstance(self.queue, str):
            queue = queues.get(self.queue)
            if queue is None:
                queue = QueueParams(
                    name=self.queue,
                    dead_letter_exchange=self.dead_letter_exchange,
                    dead_letter_routing_key=self.dead_letter_routing_key,
                )
                queues[self.queue] = queue
            self.queue = queue

    def solve(self, settings: 'Settings', parent_name: Optional[str] = None):
        self.solve_connection(settings.connections)
        self.solve_exchange(settings.exchanges)
        self.solve_queue(settings.queues)
        if parent_name and self.name is None:
            self.name = parent_name + '_consumer'

    def get_params_dict(self):
        return {
            'name': self.name,
            'prefetch_count': self.prefetch_count,
        }


class ServiceParams(ComponentParamsBaseModel):

    consumer: Union[str, ConsumerParams]
    publisher: Union[str, PublisherParams]

    name: Optional[str] = None

    def solve_consumer(self, consumers: Dict[str, ConsumerParams]):
        if isinstance(self.consumer, str):
            if self.consumer not in consumers:
                raise KeyError(f"Consumer `{self.consumer}` is not declared")
            self.consumer = consumers[self.consumer]

    def solve_publisher(self, publishers: Dict[str, PublisherParams]):
        if isinstance(self.publisher, str):
            if self.publisher not in publishers:
                raise KeyError(f"Publisher `{self.publisher}` is not declared")
            self.publisher = publishers[self.publisher]

    def solve(self, settings: 'Settings', deep=True):
        self.solve_consumer(settings.consumers)
        self.solve_publisher(settings.publishers)
        assert isinstance(self.consumer, ConsumerParams)
        assert isinstance(self.publisher, PublisherParams)
        if deep:
            self.consumer.solve(settings, parent_name=self.name)
            self.publisher.solve(settings, parent_name=self.name)

    def get_params_dict(self) -> Dict:
        return {}


class Settings(BaseSettings):

    connections: Dict[str, Union[ConnectionParams, URLConnectionParams]] = {}
    services: Dict[str, ServiceParams] = {}
    consumers: Dict[str, ConsumerParams] = {}
    publishers: Dict[str, PublisherParams] = Field(default={}, alias="producers")
    exchanges: Dict[str, ExchangeParams] = {}
    queues: Dict[str, QueueParams] = {}

    def __init__(self, **values: Any):
        super().__init__(**values)
        for connection_name, connection in self.connections.items():
            connection.name = connection_name
        for service_name, service_config in self.services.items():
            service_config.name = service_name
            service_config.solve(self, deep=True)
        for consumer_name, consumer_config in self.consumers.items():
            consumer_config.name = consumer_name
            consumer_config.solve(self)
        for publisher_name, publisher_config in self.publishers.items():
            publisher_config.name = publisher_name
            publisher_config.solve(self)

    class Config:
        yaml_file_path = 'application.yml'

        extra = Extra.forbid

        @classmethod
        def customise_sources(
                cls,
                init_settings: SettingsSourceCallable,
                env_settings: SettingsSourceCallable,
                file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            return (
                init_settings,
                env_settings,
                yaml_config_settings_source,
                file_secret_settings,
            )
