import abc
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Union

from aio_pika import Message
from aio_pika.abc import AbstractMessage
from aiormq.abc import ConfirmationFrameType
from pydantic import BaseModel

from .components.base import Component
from .settings import Settings


class AbstractPublisher(abc.ABC):

    @abc.abstractmethod
    async def publish_message(
        self,
        message: AbstractMessage,
        routing_key: str = None,
        timeout: int = None,
    ) -> Optional[ConfirmationFrameType]:
        raise NotImplementedError

    async def publish(
            self,
            message: Union[Dict, BaseModel, Message],
            routing_key: Optional[str] = None,
    ) -> Optional[ConfirmationFrameType]:
        raise NotImplementedError


class AbstractRPCClient(abc.ABC):

    @abc.abstractmethod
    async def call(self, body: Union[AbstractMessage, BaseModel, dict]) -> Union[BaseModel, dict]:
        raise NotImplementedError


class AbstractSchemeRequirement(abc.ABC):

    @abc.abstractmethod
    async def resolve(self, settings: Settings) -> Component:
        raise NotImplementedError

    @abc.abstractmethod
    def set_processor(self, processor: Optional[Callable]):
        raise NotImplementedError
