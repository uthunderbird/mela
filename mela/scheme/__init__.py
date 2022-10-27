import warnings
from typing import Dict, Optional, Type, Callable, Any

from pydantic import BaseModel

from mela.processor import Processor
from .requirement import SchemeRequirement
from mela.settings import ServiceParams, PublisherParams, ConsumerParams, RPCParams


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
    ) -> Callable[[Callable], Callable]:
        requirement = SchemeRequirement(name, 'service', params)

        def decorator(func: Callable) -> Callable:
            processor = Processor(
                func,
                input_class=input_class,
                validate_args=validate_args,
            )
            requirement.set_processor(processor)
            return processor

        self.register_component_requirement(requirement)
        return decorator

    def publisher(
        self,
        name: str,
        params: Optional[PublisherParams] = None,
    ) -> SchemeRequirement:
        requirement = SchemeRequirement(name, 'publisher', params)
        self.register_component_requirement(requirement)
        return requirement

    def consumer(
        self,
        name: str,
        params: ConsumerParams = None,
        validate_args: bool = False,
        input_class: Type[BaseModel] = None,
    ) -> Callable[[Callable], Callable]:
        requirement = SchemeRequirement(name, 'consumer', params)
        self.register_component_requirement(requirement)

        def decorator(func: Callable[..., Any]) -> Callable:
            processor = Processor(
                func,
                input_class=input_class,
                validate_args=validate_args,
            )
            requirement.set_processor(processor)
            return processor
        return decorator

    def rpc_service(
        self,
        name: str,
        params: Optional[RPCParams] = None,
        validate_args: bool = False,
        request_model: Type[BaseModel] = None,
    ):
        requirement = SchemeRequirement(name, 'rpc_service', params)
        self.register_component_requirement(requirement)

        def decorator(func: Callable[..., Any]) -> Callable:
            processor = Processor(
                func,
                input_class=request_model,
                validate_args=validate_args,
            )
            requirement.set_processor(processor)
            return processor
        return decorator

    def rpc_server(self, *args, **kwargs):
        warnings.warn("`rpc_server` component decorator will be "
                      "deprecated soon. Use `rpc_service instead`", DeprecationWarning)
        return self.rpc_service(*args, **kwargs)

    def rpc_client(
        self,
        name: str,
        params: Optional[RPCParams] = None,
    ) -> SchemeRequirement:
        requirement = SchemeRequirement(name, 'rpc_client', params)
        self.register_component_requirement(requirement)
        return requirement

    def merge(self, other: 'MelaScheme') -> 'MelaScheme':
        for requirement in other.requirements.values():
            self.register_component_requirement(requirement)
        return self