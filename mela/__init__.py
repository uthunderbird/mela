import asyncio
from typing import Optional

from aio_pika import IncomingMessage
from aio_pika import Message

from .components.base import Component
from .components.base import ConsumingComponent
from .factories.core.connection import close_all_connections
from .factories.publisher import publisher
from .factories.rpc import client as rpc_client
from .scheme import MelaScheme
from .settings import Settings


__all__ = ['IncomingMessage', 'Message', 'Mela']


class Mela(MelaScheme):

    def __init__(
            self,
            name: str,
            settings_: Optional[Settings] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        super().__init__(name)
        self._settings: Optional[Settings] = None
        if settings_:
            self.settings = settings_
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop: Optional[asyncio.AbstractEventLoop] = loop

    def publisher_sync(self, name):
        return self._loop.run_until_complete(self.publisher_instance(name))

    async def publisher_instance(self, name):
        return await publisher(self.settings.publishers[name])

    async def rpc_client_instance(self, name):
        return await rpc_client(self.settings.rpc_services[name])

    @property
    def settings(self):
        assert self._settings, "Mela is not configured"
        return self._settings

    @settings.setter
    def settings(self, value: Settings):
        """
        Set config of entire Mela app. It's possible only if app is not
        running yet. In other case it will raise `RuntimeError`
        """
        self._settings = value

    def run(self, coro=None, loop: Optional[asyncio.AbstractEventLoop] = None):
        if self._settings is None:
            self.settings = Settings()
        if loop is None:
            loop = self._loop
        assert loop
        self._run_in_loop(coro, loop)

    @staticmethod
    async def waiter():
        try:
            # Wait until terminate
            await asyncio.Future()
        finally:
            await close_all_connections()

    def _run_in_loop(self, coro, loop: asyncio.AbstractEventLoop):
        assert self._settings
        for requirement_name, requirement in list(self.requirements.items()):
            instance: Component = loop.run_until_complete(
                requirement.resolve(self._settings),
            )
            if isinstance(instance, ConsumingComponent):
                loop.run_until_complete(instance.prepare_processor(self, self.settings))
                loop.run_until_complete(instance.consume())
        if coro:
            loop.run_until_complete(coro)
        else:
            loop.run_until_complete(self.waiter())

    def register_scheme(self, scheme_: MelaScheme):
        self.merge(scheme_)
