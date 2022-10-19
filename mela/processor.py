import asyncio
import inspect
import json
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import ForwardRef
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.abc import AbstractIncomingMessage
from anyio.to_thread import run_sync
from pydantic import BaseModel
from pydantic import validate_arguments
from pydantic.typing import evaluate_forwardref


class Processor:

    async def __call__(self, *args, **kwargs):
        return await self.__process(*args, **kwargs)

    def __init__(
            self,
            call: Callable,
            input_class: Optional[Type[BaseModel]] = None,
            validate_args: bool = False,
            output_model=None,
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
        self._select_solver()

    async def __process(self, *args, **kwargs):
        return await self._call(*args, **kwargs)

    async def __process_sync(self, *args, **kwargs):
        func = partial(self._call, **kwargs)
        return await run_sync(func, *args)

    def call_sync(self, *args, **kwargs):
        return self._call(*args, **kwargs)

    @staticmethod
    def _wrap_response(
            result: Union[Dict, BaseModel, Message],
            routing_key: str = None,
    ) -> Tuple[Message, Optional[str]]:
        if isinstance(result, Message):
            return result, routing_key
        if isinstance(result, dict):
            return Message(json.dumps(result).encode()), routing_key
        if isinstance(result, BaseModel):
            return Message(result.json().encode()), routing_key

    async def process(self, message: AbstractIncomingMessage) -> Tuple[Message, Optional[str]]:
        solved_params = self._solve_dependencies(message)
        result = await self(**solved_params)
        return self._wrap_response(*result)

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
            if param.annotation is not None:
                return False
        return True

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
                        break
        self._input_class = message_class_candidate
        return self._input_class

    def _solve_dependencies_for_data_class(
        self,
        message: IncomingMessage,
    ) -> Dict[str, Any]:
        solved = {}
        assert self._input_class
        parsed_message = self._input_class.parse_raw(message.body)
        parsed_message_dict = parsed_message.dict()
        for param in self._params:  # type: inspect.Parameter
            if param.annotation is IncomingMessage:
                solved[param.name] = message
            elif issubclass(param.annotation, BaseModel):
                solved[param.name] = parsed_message  # type: ignore
            elif param.name in parsed_message_dict:
                solved[param.name] = parsed_message_dict[param.name]
            else:
                raise KeyError(f"Key `{param.name}` cannot be solved")

        return solved

    def _solve_dependencies(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        raise NotImplementedError("Dependency solver is not set")

    def _solve_dependencies_for_raw_json(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        solved = {}
        parsed_message = json.loads(message.body)
        for param in self._params:  # type: inspect.Parameter
            if param.annotation is IncomingMessage:
                solved[param.name] = message
            elif param.name in parsed_message:
                solved[param.name] = parsed_message[param.name]
            else:
                raise KeyError(f"Key `{param.name}` cannot be solved")
        return solved

    def _solve_dependencies_oldstyle(self, message: AbstractIncomingMessage) -> Dict[str, Any]:
        solved = {}
        parsed_message = json.loads(message.body)
        for i, param in enumerate(self._params):
            if i == 0:
                solved[param.name] = parsed_message
            elif i == 1:
                solved[param.name] = message
            else:
                raise KeyError("Too much arguments for oldstyle solver")
        return solved

    def _select_solver(self):
        if len(list(self._params)) == 2 and self._have_no_annotations():
            self._solve_dependencies = self._solve_dependencies_oldstyle
        elif self._input_class:
            self._solve_dependencies = self._solve_dependencies_for_data_class
        else:
            self._solve_dependencies = self._solve_dependencies_for_raw_json


if __name__ == '__main__':

    def func_(a: str, b: str, message: IncomingMessage = None) -> bool:
        return bool(a or b)

    p = Processor(func_)

    resp = asyncio.run(p(1, ""))
