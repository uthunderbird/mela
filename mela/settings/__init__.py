import asyncio
from typing import (
    Tuple,
    Dict,
    Any,
    Union,
    Optional,
    Callable,
    Awaitable,
)
import envyaml

from aio_pika.abc import (
    AbstractChannel,
    AbstractMessage,
    AbstractExchange,
    AbstractQueue
)
from aiormq.abc import ConfirmationFrameType

from pydantic import (
    BaseModel,
    BaseSettings,
    AmqpDsn,
    Field,
    Extra,
)


from pydantic.env_settings import SettingsSourceCallable


def yaml_config_settings_source(settings: 'BaseSettings') -> Dict[str, Any]:
    """
    A simple settings source that loads variables from a YAML file
    at the project's root.
    """
    yaml_config = envyaml.EnvYAML(settings.Config.yaml_file_path, include_environment=False)
    clear_config = {}
    for key, value in dict(yaml_config).items():
        if '.' not in key:
            clear_config[key] = value
    print(clear_config)
    return clear_config


class ConnectionSettings(BaseModel):
    host: str
    port: int
    login: str = Field(alias='username')
    password: str
    virtualhost: str = '/'
    ssl: bool = False
    ssl_options = dict
    timeout: Union[float, int] = Field(120, alias='connTimeout')  # TODO Remove DEPRECATED alias
    client_properties: Optional[Dict] = None
    heartbeat: int = None

    class Config:
        extra = Extra.forbid


class ExchangeSettings(BaseModel):
    name: str
    type: str = 'direct'
    durable: bool = False
    auto_delete: bool = False
    internal: bool = False
    passive: bool = False
    arguments: Optional[Dict] = None
    timeout: Union[float, int] = None


class URLConnectionSettings(BaseModel):
    url: AmqpDsn

    class Config:
        extra = Extra.forbid


class PublisherSettings(BaseModel):
    connection: Union[str, ConnectionSettings] = 'default'
    exchange: Union[str, ExchangeSettings]
    routing_key: str
    skip_unroutables: bool = False


class ConsumerSettings(BaseModel):
    connection: Union[str, ConnectionSettings] = 'default'
    exchange: Union[str, ExchangeSettings]
    routing_key: str
    queue: str
    prefetch_count: int = 1
    dead_letter_exchange: str = None
    dead_letter_routing_key: str = None
    requeue_broken_messages: bool = False


class ServiceSettings(BaseModel):
    consumer: ConsumerSettings
    publisher: PublisherSettings


class Settings(BaseSettings):

    connections: Dict[str, Union[ConnectionSettings, URLConnectionSettings]]
    services: Dict[str, ServiceSettings] = None
    consumers: Dict[str, ConsumerSettings] = None
    publishers: Dict[str, PublisherSettings] = Field(default=None, alias="producers")  # TODO DEPRECATE alias

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


class MelaPublisher:

    def __init__(
            self,
            name: str,
            channel: Optional[AbstractChannel] = None,
            settings: Optional[PublisherSettings] = None,
            *,
            exchange: Optional[AbstractExchange] = None,
    ):
        self.name: str = name
        self.settings: Optional[PublisherSettings] = None
        if settings:
            self.set_settings(settings)
        else:
            pass

    def set_settings(self, settings: Optional[PublisherSettings]):
        self.settings = settings

    # def gather_binds(self) -> BindMap:
    #     pass
    #
    # def gather_network(self) -> NetworkMap:
    #     pass

    def set_processor(self, func: Callable):
        pass

    async def publish(self, message: AbstractMessage, routing_key: str = '') -> ConfirmationFrameType:
        pass


class MelaConsumer:

    def __init__(
            self,
            name: str,
            settings: Optional[PublisherSettings] = None,
            *,
            queue: Optional[AbstractQueue] = None,
    ):
        self.name: str = name
        self.settings: Optional[PublisherSettings] = None
        if settings:
            self.set_settings(settings)
        else:
            pass


class MelaService:

    def __init__(
            self,
            name: str,
            settings: Optional[ServiceSettings] = None,
            *,
            publisher: Optional[MelaPublisher] = None,
            consumer: Optional[MelaConsumer] = None,
    ):
        self.name: str = name
        self.settings: Optional[ServiceSettings] = None
        if settings:
            self.set_settings(settings)
        else:
            pass

    def set_processor(self, func: Callable[[BaseModel], BaseModel]):
        pass

    def set_settings(self, settings: Optional[ServiceSettings]):
        self.settings = settings

    # def gather_binds(self) -> BindMap:
    #     pass
    #
    # def gather_network(self) -> NetworkMap:
    #     pass
    #
    # def gather_components(self) -> ComponentMap:
    #     pass

    @property
    def consumer(self) -> MelaConsumer:
        pass

    @property
    def publisher(self) -> MelaPublisher:
        pass

    def run(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        pass

    def get_runner(self) -> Callable:
        """
        Maybe it's better to use this way?
        :return:
        """


class MelaScheme:

    """
    Scheme is not runnable. It just declare relations between app components.
    """

    def __init__(
            self,
            name: str = None,
            config: Optional[Settings] = None
    ):
        self.name: str = name
        self._config: Optional[Settings] = None
        if config:
            self.set_config(config)

    def set_config(self, config: Settings) -> None:
        """
        Set config of Mela Scheme.
        """
        pass

    def service(self, name: str, settings: ServiceSettings) -> MelaService:
        pass

    # def rpc(self, name: str, settings: RPCSettings) -> MelaRPC:
    #     pass

    def publisher(self, name: str, settings: PublisherSettings) -> MelaPublisher:
        pass

    def consumer(self, name: str, settings: ConsumerSettings) -> MelaConsumer:
        pass

    # def register_component(self, component: MelaComponent) -> None:
    #     pass

    # def gather_network(self) -> NetworkMap:
    #     pass

    # def gather_binds(self) -> BindMap:
    #     pass

    # def gather_components(self) -> ComponentMap:
    #     pass

    def merge(self, other: 'MelaScheme') -> 'MelaScheme':
        pass


class Mela:

    def __init__(
            self,
            name: str,
            config: Optional[Settings] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.name: str = name
        self._config: Optional[Settings] = None
        if config:
            self.set_config(config)
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop: Optional[asyncio.AbstractEventLoop] = loop

    def set_config(self, config: Settings):
        """
        Set config of entire Mela app. It's possible only if app is not
        running yet. In other case it will raise `RuntimeError`
        """
        pass


if __name__ == '__main__':
    print(Settings())
