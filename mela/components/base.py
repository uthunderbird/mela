import abc
import asyncio
import logging
from typing import Optional

from ..log import handler


class Component(abc.ABC):

    def __init__(self, name, log_level=None, loop=None):
        self.name = name
        if loop is None:
            loop = asyncio.get_running_loop()
        self.loop = loop
        self.log = None
        if log_level is not None:
            self.config_logger(log_level)

    def config_logger(self, level: str):
        self.log = logging.getLogger(self.name)
        self.log.addHandler(handler)
        self.log.setLevel(level.upper())


class ConsumingComponent(Component, abc.ABC):

    def __init__(self, name, log_level: str = None, loop: asyncio.AbstractEventLoop = None):
        super().__init__(name, log_level, loop)
        self._processor = None

    @abc.abstractmethod
    def set_processor(self, processor):
        raise NotImplementedError()

    @abc.abstractmethod
    async def consume(self, **kwargs) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def cancel(self, timeout: Optional[int] = None, nowait: bool = False):
        raise NotImplementedError()

    async def prepare_processor(self, scheme, settings):
        if self._processor:
            self._processor.cache_static_params(self, scheme)
            await self._processor.solve_requirements(settings)
