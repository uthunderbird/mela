import asyncio
from typing import Tuple, Dict, Any, Union, Optional
import envyaml

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
    publishers: Dict[str, PublisherSettings] = Field(default=None, alias="producers")  ## TODO DEPRECATE alias

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


class MelaScheme:
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
