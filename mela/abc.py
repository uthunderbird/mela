from abc import ABC, abstractmethod
from asyncio import Event
from typing import Optional

from pydantic import BaseModel


class AbstractComponent(ABC):

    def __init__(self, name: str, parent: Optional['AbstractComponent'] = None):
        self._name: str = name
        self._parent: Optional['AbstractComponent'] = None
        if parent:
            self.set_parent(parent)
        self.setup_completed: Event = Event()
        self.shutdown_completed: Event = Event()

    @property
    def parent(self) -> Optional['AbstractComponent']:
        return self._parent

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

    def set_parent(self, parent: 'AbstractComponent'):
        assert self._parent is None
        self._parent = parent

    def shutdown(self):
        assert self.setup_completed.is_set()
        self.shutdown_completed.set()

    def run(self):
        assert self.setup_completed.is_set()
        assert not self.shutdown_completed.is_set()


class AbstractConfigurableComponent(AbstractComponent, ABC):

    CONFIG_CLASS = BaseModel

    def __init__(self, name: str, parent: Optional['AbstractComponent'] = None, config: Optional[CONFIG_CLASS] = None):
        super(AbstractConfigurableComponent, self).__init__(name, parent)
        self._config = None
        if config:
            self.configure(config)

    def configure(self, config: Optional[CONFIG_CLASS] = None):
        assert self._config is None, f"Component `{self.name}` already configured"
        assert not self.setup_completed.is_set()
        self._config = config

    @abstractmethod
    def setup(self):
        assert self._config is not None, f"Component `{self.name}` is not configured"
        super(AbstractConfigurableComponent, self).setup()
