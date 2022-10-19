import asyncio
from typing import Optional
from typing import Union

from .components import Consumer
from .components import Service
from .factories.core.connection import close_all_connections
from .scheme import MelaScheme
from .settings import Settings


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

    def run(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if self._settings is None:
            self.settings = Settings()
        if loop is None:
            loop = self._loop
        assert loop
        self._run_in_loop(loop)

    @staticmethod
    async def waiter():
        try:
            # Wait until terminate
            await asyncio.Future()
        finally:
            await close_all_connections()

    def _run_in_loop(self, loop: asyncio.AbstractEventLoop):
        assert self._settings
        for requirement_name, requirement in self.requirements.items():
            type_ = requirement.type_
            instance: Union[Consumer, Service] = loop.run_until_complete(
                requirement.resolve(self._settings),
            )
            if type_ != 'publisher':
                loop.run_until_complete(instance.consume())
        loop.run_until_complete(self.waiter())

    def register_scheme(self, scheme_: MelaScheme):
        self.merge(scheme_)
