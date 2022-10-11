import typing
from abc import ABC
from abc import abstractmethod
from enum import Enum
from logging import Logger
from logging import getLogger
from typing import Optional
from typing import Union

from aio_pika.abc import AbstractRobustConnection
from aio_pika import connect_robust, ExchangeType, Exchange, RobustChannel
from pydantic import BaseModel, Field

from mela.abc import AbstractComponent, AbstractConfigurableComponent
from mela.connection import URLConnectionConfiguration, HostConnectionConfiguration


class LogLevelEnum(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class BaseModelWithLogLevel(BaseModel):
    log_level: Optional[LogLevelEnum] = None


class LoggableComponent(AbstractConfigurableComponent, ABC):
    CONFIG_CLASS = BaseModelWithLogLevel
    DEFAULT_LOG_LEVEL = LogLevelEnum.INFO

    def __init__(self, name: str, parent: Optional['AbstractComponent'] = None, config: Optional[CONFIG_CLASS] = None):
        super().__init__(name, parent, config)
        self.log: Optional[Logger] = None

    @abstractmethod
    def setup(self):
        self.log = getLogger(self.name)
        log_level = self._config.log_level or self.DEFAULT_LOG_LEVEL
        self.log.setLevel(log_level.value)
        super().setup()
        self.log.info(f"Component `{self.name}` is set up")
        self.log.debug(f"Component `{self.name}` configuration is:\n{self._config.dict()}")

    def shutdown(self):
        super().shutdown()
        self.log.info(f"Component `{self.name}` is shut down")


class SingletonResourceWrapper(LoggableComponent, ABC):
    # DEFAULT_LOG_LEVEL = LogLevelEnum.DEBUG
    RESOURCE_CLASS: typing.Type = None

    def __init__(
        self,
        name: str,
        parent: Optional[AbstractComponent] = None,
        config: Optional[BaseModelWithLogLevel] = None,
    ):
        super().__init__(name, parent, config)
        self._instance = None

    def setup(self):
        super(SingletonResourceWrapper, self).setup()
        self._instance = self._instantiate()

    def shutdown(self):
        del self._instance
        self._instance = None
        super(SingletonResourceWrapper, self).shutdown()

    def acquire(self) -> RESOURCE_CLASS:
        assert self.setup_completed.is_set()
        assert not self.shutdown_completed.is_set()
        assert self._instance is not None
        return self._instance

    @abstractmethod
    def _instantiate(self) -> RESOURCE_CLASS:
        return self.RESOURCE_CLASS()


class MelaConnectionWrapper(SingletonResourceWrapper):

    CONFIG_CLASS = Union[URLConnectionConfiguration, HostConnectionConfiguration]
    RESOURCE_CLASS = typing.Coroutine[typing.Any, typing.Any, AbstractRobustConnection]

    def _instantiate(self) -> RESOURCE_CLASS:
        return connect_robust(**self._config.dict())


class MelaBiDirectConnection(AbstractConfigurableComponent):

    def __init__(
        self,
        name: str,
        parent: Optional[AbstractComponent],
        config: Optional[BaseModelWithLogLevel],
    ):
        super().__init__(name, parent, config)
        self._read: Optional[MelaConnectionWrapper] = None
        self._write: Optional[MelaConnectionWrapper] = None

    @property
    def read(self):
        if not self._read:
            self._read = MelaConnectionWrapper('read', self, self._config)
            self._read.setup()
        return self._read.acquire()

    @property
    def write(self):
        if not self._write:
            self._write = MelaConnectionWrapper('write', self, self._config)
            self._write.setup()
        return self._write.acquire()

    def setup(self):
        super(MelaBiDirectConnection, self).setup()

    def shutdown(self):
        if self._read:
            self._read.shutdown()
        if self._write:
            self._write.shutdown()
        super(MelaBiDirectConnection, self).shutdown()


class ExchangeSettings(BaseModelWithLogLevel):
    name: str
    connection_name: str = Field(alias="connection")
    type: Union[ExchangeType, str] = ExchangeType.DIRECT


class MelaExchange(SingletonResourceWrapper):
    RESOURCE_CLASS: typing.Type = Exchange

    def __init__(
            self,
            name: str,
            parent: Optional[AbstractComponent],
            config: Optional[BaseModelWithLogLevel],
            channel: RobustChannel = None,
    ):
        super().__init__(name, parent, config)
        self._channel = None
        if channel:
            self.set_channel(channel)

    def _instantiate(self) -> RESOURCE_CLASS:
        return self.RESOURCE_CLASS()

    def set_channel(self, channel):
        self._channel = channel

    @property
    def channel(self):
        if not self._channel:
            self._channel = self._get_connection().channel()
        return self._channel