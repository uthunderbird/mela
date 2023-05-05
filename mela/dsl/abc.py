import abc
from typing import Self

from pydantic import BaseModel


class AbstractDefinition(BaseModel, abc.ABC):
    class Config:
        allow_mutation = False


class AbstractNamedDefinition(AbstractDefinition, abc.ABC):
    name: str = None

    def define(self, **kwargs) -> Self:
        return self.copy(update=kwargs)


class AbstractRef(AbstractDefinition, abc.ABC):
    ref: str


class AbstractSchema(abc.ABC):
    pass
