import abc
import logging
from typing import Optional


class Component(abc.ABC):

    def __init__(self, name, log_level=None):
        self.name = name
        self.log = None
        if log_level is not None:
            self.config_logger(log_level)

    def config_logger(self, level: str):
        self.log = logging.getLogger(self.name)
        self.log.setLevel(level.upper())


class ConsumingComponent(Component, abc.ABC):

    @abc.abstractmethod
    async def consume(self, **kwargs) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        raise NotImplementedError()
