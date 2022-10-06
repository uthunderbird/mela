import typing
from abc import ABC
from abc import abstractmethod
from asyncio import Event
from enum import Enum
from logging import Logger
from logging import getLogger
from typing import Optional
from typing import Union
from typing import Dict
from aio_pika.connection import URL

from aio_pika.abc import AbstractRobustConnection
from aio_pika import connect_robust
from pydantic import BaseModel
from pydantic import Field
from pydantic import Extra


class Component(ABC):

    def __init__(self, name: str, parent: Optional['Component'] = None):
        self._name: str = name
        self._parent: Optional['Component'] = None
        if parent:
            self.set_parent(parent)
        self.setup_completed: Event = Event()
        self.shutdown_completed: Event = Event()

    @property
    def parent(self) -> Optional['Component']:
        return self.parent

    @property
    def name(self) -> str:
        full_name = self._name
        ancestor = self.parent
        while ancestor:
            full_name = f"{ancestor.name}.{full_name}"
            ancestor = ancestor.parent
        return full_name

    @abstractmethod
    def setup(self):
        self.setup_completed.set()

    def set_parent(self, parent: 'Component'):
        assert self._parent is None
        self._parent = parent

    def shutdown(self):
        self.shutdown_completed.set()

    def run(self):
        assert self.setup_completed.is_set()
        assert not self.shutdown_completed.is_set()


class ConfigurableComponent(Component, ABC):

    CONFIG_CLASS = BaseModel

    def __init__(self, name: str, parent: Optional['Component'] = None, config: Optional[CONFIG_CLASS] = None):
        super(ConfigurableComponent, self).__init__(name, parent)
        self._config = None
        if config:
            self.configure(self._config)

    def configure(self, config: Optional[CONFIG_CLASS] = None):
        assert config is None
        assert not self.setup_completed.is_set()
        self._config = config


class LogLevelEnum(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class BaseModelWithLogLevel(BaseModel):
    log_level: Optional[LogLevelEnum] = None


class LoggableComponent(ConfigurableComponent, ABC):
    CONFIG_CLASS = BaseModelWithLogLevel
    DEFAULT_LOG_LEVEL = LogLevelEnum.INFO

    def __init__(self, name: str, parent: Optional['Component'] = None, config: Optional[CONFIG_CLASS] = None):
        super().__init__(name, parent, config)
        self.log: Optional[Logger] = None

    @abstractmethod
    def setup(self):
        self.log = getLogger(self.name)
        log_level = self._config.log_level or self.DEFAULT_LOG_LEVEL
        self.log.setLevel(log_level)
        super().setup()
        self.log.info(f"Component `{self.name}` is set up")
        self.log.debug(f"Component `{self.name}` configuration is:\n{self._config.dict()}")

    @abstractmethod
    def shutdown(self):
        super().shutdown()
        self.log.info(f"Component `{self.name}` is shut down")


class ResourceWrapper(LoggableComponent, ABC):
    DEFAULT_LOG_LEVEL = LogLevelEnum.DEBUG
    RESOURCE_CLASS: typing.Type = None

    @abstractmethod
    def acquire(self) -> RESOURCE_CLASS:
        pass


class URLConnectionConfiguration(BaseModel):
    url: URL
    timeout: Union[float, int] = Field(120, alias='connTimeout')  # DEPRECATED alias
    client_properties: Optional[Dict] = None
    heartbeat: int = None

    class Config:
        extra = Extra.forbid


class HostConnectionConfiguration(BaseModel):
    host: str
    port: int
    login: str = Field(alias='username')
    password: str
    virtualhost: str = '/'
    ssl: bool = False
    ssl_options = dict
    timeout: Union[float, int] = Field(120, alias='connTimeout')  # DEPRECATED alias
    client_properties: Dict = dict
    heartbeat: int = None

    class Config:
        extra = Extra.forbid


class MelaConnection(ResourceWrapper):

    CONFIG_CLASS = Union[URLConnectionConfiguration, HostConnectionConfiguration]
    RESOURCE_CLASS = AbstractRobustConnection

    async def acquire(self) -> RESOURCE_CLASS:
        return await connect_robust(**self._config.dict())

    def setup(self):


    def shutdown(self):
        pass