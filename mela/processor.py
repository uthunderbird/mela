import asyncio
import inspect
import json
from functools import partial
from logging import Logger
from typing import Any
from typing import Callable
from typing import Dict
from typing import ForwardRef
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union
from warnings import warn

from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.abc import AbstractMessage
from anyio.to_thread import run_sync
from pydantic import BaseModel
from pydantic import validate_arguments
from pydantic.typing import evaluate_forwardref

from .abc import AbstractPublisher
from .abc import AbstractRPCClient
from .abc import AbstractSchemeRequirement


class Processor:

    static_param_classes = [Logger, AbstractPublisher, AbstractRPCClient]

    async def __call__(self, *args, **kwargs):
        return await self.__process(*args, **kwargs)

    def __init__(
            self,
            call: Callable,
            input_class: Optional[Type[BaseModel]] = None,
            validate_args: bool = False,
    ):
        self._call = call
        if validate_args:
            self._call = validate_arguments(
                config={
                    'arbitrary_types_allowed': True,
                },
            )(self._call)
        if not self._is_coroutine():
            self.__process = self.__process_sync  # type: ignore
        self._input_class = input_class
        self._signature = self._get_typed_signature()
        self._params = self._get_typed_parameters()
        self._static_params = []
        self._dynamic_params = []
        self._split_static_and_dynamic_params()
        self._cached_static_params = {}
        if self._input_class is None:
            self._get_data_class()
        self._select_solver()
        self._have_static_params = False

    async def __process(self, *args, **kwargs):
        return await self._call(*args, **kwargs)

    async def __process_sync(self, *args, **kwargs):
        func = partial(self._call, **kwargs)
        return await run_sync(func, *args)

    def cache_static_params(self, component, scheme):
        for param in self._static_params:  # type: inspect.Parameter
            if param.annotation is Logger:
                self._cached_static_params[param.name] = component.log
            elif issubclass(param.annotation, AbstractPublisher):
                self._cached_static_params[param.name] = scheme.publisher(param.default)
            elif issubclass(param.annotation, AbstractRPCClient):
                self._cached_static_params[param.name] = scheme.rpc_client(param.default)
            else:
                raise TypeError("Static param cannot be solved")

    async def solve_requirements(self, settings):
        updated_values = {}
        for param_name, requirement in self._cached_static_params.items():
            if isinstance(requirement, AbstractSchemeRequirement):
                updated_values[param_name] = await requirement.resolve(settings)
        self._cached_static_params.update(updated_values)
        if self._cached_static_params:
            self._call = partial(self._call, **self._cached_static_params)

    def call_sync(self, *args, **kwargs):
        assert not self._is_coroutine()
        return self._call(*args, **kwargs)

    def _split_static_and_dynamic_params(self):
        for param in self._params:  # type: inspect.Parameter
            if param.annotation in self.static_param_classes:
                self._static_params.append(param)
                self._have_static_params = True
            else:
                self._dynamic_params.append(param)

    @staticmethod
    def wrap_response(
            result: Union[Dict, BaseModel, Message],
            routing_key: Optional[str] = None,
    ) -> Tuple[Message, Optional[str]]:
        if isinstance(result, Message):
            return result, routing_key
        elif isinstance(result, dict):
            return Message(json.dumps(result).encode()), routing_key
        elif isinstance(result, BaseModel):
            json_encoded = result.json(by_alias=True).encode()
            return Message(json_encoded), routing_key

    async def process(self, message: AbstractIncomingMessage) -> Tuple[Message, Optional[str]]:
        solved_params = self._solve_dependencies(message)
        result = await self(**solved_params)
        wrapped_result = self.wrap_response(result)
        return wrapped_result

    @staticmethod
    def _get_typed_annotation(param: inspect.Parameter, globalns: Dict[str, Any]) -> Any:
        annotation = param.annotation
        if isinstance(annotation, str):
            annotation = ForwardRef(annotation)
            annotation = evaluate_forwardref(annotation, globalns, globalns)
        return annotation

    def _get_typed_signature(self) -> inspect.Signature:
        signature = inspect.signature(self._call)
        globalns = getattr(self._call, "__globals__", {})
        typed_params = [
            inspect.Parameter(
                name=param.name,
                kind=param.kind,
                default=param.default,
                annotation=self._get_typed_annotation(param, globalns),
            )
            for param in signature.parameters.values()
        ]
        typed_signature = inspect.Signature(typed_params)
        return typed_signature

    def _get_typed_parameters(self) -> Iterable[inspect.Parameter]:
        return self._get_typed_signature().parameters.values()

    def _is_coroutine(self) -> bool:
        return asyncio.iscoroutinefunction(self._call)

    def _have_no_annotations(self):
        for param in self._params:
            if param.annotation is not inspect.Parameter.empty:
                return False
        return True

    def _oldstyle_annotations(self):
        params = list(self._params)
        if (
                params[0].annotation is inspect.Parameter.empty
                or
                params[0].annotation is dict
        ) and issubclass(params[1].annotation, AbstractMessage):
            return True
        return False

    def _get_data_class(self):
        message_class_candidate = None
        if self._input_class is None:
            for param in self._params:
                if issubclass(param.annotation, BaseModel):
                    if message_class_candidate is None:
                        message_class_candidate = param.annotation
                    else:
                        # Two different base classes found. We are not
                        # sure which should be used to parse message.
                        message_class_candidate = None
                        raise AssertionError("Two different data classes are found")
        self._input_class = message_class_candidate
        return self._input_class

    def _solve_dependencies_for_data_class(
        self,
        message: IncomingMessage,
    ) -> Dict[str, Any]:
        solved = {}
        assert self._input_class
        parsed_message = self._input_class.parse_raw(message.body)
        parsed_message_dict = parsed_message.dict(exclude_unset=True)
        for param in self._dynamic_params:  # type: inspect.Parameter
            if param.annotation is IncomingMessage:
                solved[param.name] = message
            elif issubclass(param.annotation, BaseModel):
                solved[param.name] = parsed_message  # type: ignore
            elif param.name in parsed_message_dict:
                solved[param.name] = parsed_message_dict[param.name]
            else:
                raise KeyError(f"Key `{param.name}` cannot be solved by dataclass solver")

        return solved

    def _solve_dependencies(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        raise NotImplementedError("Dependency solver is not set")

    def _solve_dependencies_for_raw_json(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        solved = {}
        parsed_message = json.loads(message.body)
        for param in self._dynamic_params:  # type: inspect.Parameter
            if param.annotation is IncomingMessage:
                solved[param.name] = message
            elif param.name in parsed_message:
                solved[param.name] = parsed_message[param.name]
            else:
                raise KeyError(f"Key `{param.name}` cannot be solved by JSON solver")
        return solved

    def _solve_dependencies_oldstyle(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        solved = {}
        parsed_message = json.loads(message.body)
        for i, param in enumerate(self._dynamic_params):
            if i == 0:
                solved[param.name] = parsed_message
            elif i == 1:
                solved[param.name] = message
            else:
                raise KeyError("Too much arguments for oldstyle solver")
        return solved

    def _select_solver(self):
        if len(list(self._params)) == 2 and (
                self._have_no_annotations() or self._oldstyle_annotations()
        ):
            warn("Oldstyle injections are deprecated. Update your processor definition", DeprecationWarning)
            self._solve_dependencies = self._solve_dependencies_oldstyle
        elif self._input_class:
            self._solve_dependencies = self._solve_dependencies_for_data_class
        else:
            self._solve_dependencies = self._solve_dependencies_for_raw_json
