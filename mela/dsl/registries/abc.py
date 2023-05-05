import abc
import typing

from ..abc import AbstractDefinition


class AbstractRegistry(abc.ABC):
    _definition_class: typing.Type[AbstractDefinition]

    def __init__(self):
        self._registry = {}

    def __getitem__(self, item: str) -> '_definition_class':
        return self._registry[item]

    def __setitem__(self, key, value):
        self._registry[key] = value

    def define(self, **kwargs) -> '_definition_class':
        return self._definition_class(**kwargs)
