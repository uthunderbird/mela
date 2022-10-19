from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Literal
from typing import Mapping
from typing import Optional
from typing import Type

from pydantic import BaseModel

from .components import Component
from .factories import factory_dict
from .processor import Processor
from .settings import ComponentParamsBaseModel
from .settings import ConsumerParams
from .settings import PublisherParams
from .settings import ServiceParams
from .settings import Settings


class SchemeRequirement:

    def __init__(
        self,
        name: str,
        type_: Literal['publisher', 'consumer', 'service'],
        params: Optional[ComponentParamsBaseModel] = None,
        processor: Optional[Callable] = None,
    ):
        self.name = name
        self.type_ = type_
        self.params = params
        self.factory: Callable[
            [ComponentParamsBaseModel],
            Awaitable[Component],
        ] = factory_dict[type_]
        self.processor = processor

    async def _resolve(self, settings: Settings):
        if self.params:
            return await self.factory(self.params)

        registry: Mapping[str, ComponentParamsBaseModel]

        if self.type_ == 'publisher':
            registry = settings.publishers
        elif self.type_ == 'consumer':
            registry = settings.consumers
        elif self.type_ == 'service':
            registry = settings.services
        else:
            raise NotImplementedError(f"Unknown component type: `{self.type_}`")

        if self.name not in registry:
            raise KeyError(
                f"{self.type_.title()} `{self.name}` is not described in app settings, "
                f"also params are not provided",
            )
        return await self.factory(registry[self.name])

    async def resolve(self, settings: Settings):
        resolved = await self._resolve(settings)
        if self.processor:
            resolved.set_callback(self.processor)
        return resolved

    def set_processor(self, processor: Optional[Callable]):
        assert self.processor is None, (f"Processor for component "
                                        f"requirement `{self.name}` already set")
        self.processor = processor


class MelaScheme:

    """
    Scheme is not runnable. It just declare relations between app components.
    """

    def __init__(
            self,
            name: str,
    ):
        self.name: str = name
        self.requirements: Dict['str', SchemeRequirement] = {}

    def register_component_requirement(self, requirement: SchemeRequirement):
        if requirement.name in self.requirements:
            raise KeyError(f"Looks like requirement with name `{requirement.name}` already exists")
        self.requirements[requirement.name] = requirement

    def service(
        self,
        name: str,
        params: Optional[ServiceParams] = None,
        validate_args: bool = False,
        input_class: Type[BaseModel] = None,
        output_class: Type[BaseModel] = None,
    ) -> Callable[[Callable], Callable]:
        requirement = SchemeRequirement(name, 'service', params)

        def decorator(func: Callable) -> Callable:
            processor = Processor(
                func,
                input_class=input_class,
                validate_args=validate_args,
                output_model=output_class,
            )
            requirement.set_processor(processor)
            return processor

        self.register_component_requirement(requirement)
        return decorator

    def publisher(
        self,
        name: str,
        params: Optional[PublisherParams] = None,
    ) -> None:
        requirement = SchemeRequirement(name, 'publisher', params)
        self.register_component_requirement(requirement)

    def consumer(
        self,
        name: str,
        params: ConsumerParams = None,
        validate_args: bool = False,
        input_class: Type[BaseModel] = None,
        output_class: Type[BaseModel] = None,
    ) -> Callable[[Callable], Callable]:
        requirement = SchemeRequirement(name, 'consumer', params)
        self.register_component_requirement(requirement)

        def decorator(func: Callable[..., Any]) -> Callable:
            processor = Processor(
                func,
                input_class=input_class,
                validate_args=validate_args,
                output_model=output_class,
            )
            requirement.set_processor(processor)
            return processor
        return decorator

    def merge(self, other: 'MelaScheme') -> 'MelaScheme':
        for requirement in other.requirements.values():
            self.register_component_requirement(requirement)
        return self
