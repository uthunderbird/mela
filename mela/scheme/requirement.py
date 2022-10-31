from typing import Callable
from typing import Literal
from typing import Mapping
from typing import Optional

from ..abc import AbstractSchemeRequirement
from ..factories import factory_dict
from ..settings import ComponentParamsBaseModel


class SchemeRequirement(AbstractSchemeRequirement):

    def __init__(
        self,
        name: str,
        type_: Literal['publisher', 'consumer', 'service', 'rpc_service', 'rpc_client'],
        params: Optional[ComponentParamsBaseModel] = None,
        processor: Optional[Callable] = None,
    ):
        self.name = name
        self.type_ = type_
        self.params = params
        self.factory = factory_dict[type_]
        self.processor = processor

    async def _resolve(self, settings):
        if self.params:
            return await self.factory(self.params)

        registry: Mapping[str, ComponentParamsBaseModel]

        if self.type_ == 'publisher':
            registry = settings.publishers
        elif self.type_ == 'consumer':
            registry = settings.consumers
        elif self.type_ == 'service':
            registry = settings.services
        elif self.type_ in ('rpc_service', 'rpc_client'):
            registry = settings.rpc_services
        else:
            raise NotImplementedError(f"Unknown component type: `{self.type_}`")

        if self.name not in registry:
            raise KeyError(
                f"{self.type_.title()} `{self.name}` is not described in app settings, "
                f"also params are not provided",
            )
        return await self.factory(registry[self.name])

    async def resolve(self, settings):
        resolved = await self._resolve(settings)
        if self.processor:
            resolved.set_processor(self.processor)
        return resolved

    def set_processor(self, processor: Optional[Callable]):
        assert self.processor is None, (f"Processor for component "
                                        f"requirement `{self.name}` already set")
        self.processor = processor
