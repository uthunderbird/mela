import json
import functools
import inspect
from typing import Callable, Any, ForwardRef, Dict, Iterable, Optional, Type, Union

from pydantic import BaseModel, validate_arguments, Field
from pydantic.typing import evaluate_forwardref
from datetime import datetime
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractMessage

from mela import Mela

app = Mela(__name__)
app.read_config_yaml('application.yml')


class Document(BaseModel):

    text: str
    url: str
    likesCount: int = 0
    date: datetime
    nonexistant: Dict


def get_typed_annotation(param: inspect.Parameter, globalns: Dict[str, Any]) -> Any:
    annotation = param.annotation
    if isinstance(annotation, str):
        annotation = ForwardRef(annotation)
        annotation = evaluate_forwardref(annotation, globalns, globalns)
    return annotation


def get_typed_signature(call: Callable[..., Any]) -> inspect.Signature:
    signature = inspect.signature(call)
    globalns = getattr(call, "__globals__", {})
    typed_params = [
        inspect.Parameter(
            name=param.name,
            kind=param.kind,
            default=param.default,
            annotation=get_typed_annotation(param, globalns),
        )
        for param in signature.parameters.values()
    ]
    typed_signature = inspect.Signature(typed_params)
    return typed_signature


async def run_endpoint_function(
    *, call, values: Dict[str, Any], is_coroutine: bool
) -> Any:
    # Only called by get_request_handler. Has been split into its own function to
    # facilitate profiling endpoints, since inner functions are harder to profile.
    assert call is not None, "dependant.call must be a function"

    if is_coroutine:
        return await call(**values)
    else:
        # TODO run in thread like in FastAPI
        return call(**values)


def solve_dependencies(
        dependencies: Iterable[inspect.Parameter],
        message: IncomingMessage,
        model: Optional[Type[BaseModel]] = None
) -> Dict[str, Any]:
    solved = {}
    json_message_body = json.loads(message.body)
    parsed_message = None
    if model is not None:
        parsed_message = model(**json_message_body)
    for param in dependencies:  # type: inspect.Parameter
        if param.annotation is IncomingMessage:
            solved[param.name] = message
        elif issubclass(param.annotation, BaseModel):
            if parsed_message and param.annotation is model:
                solved[param.name] = parsed_message
            elif parsed_message and param.annotation is not model:
                raise AssertionError(f"Two different models found for the same endpoint: `{model}` and `{param.annotation}`")
            else:
                model = param.annotation
                parsed_message = model(**json_message_body)
        elif param.name in json_message_body.keys():
            solved[param.name] = json_message_body[param.name]
        else:
            raise KeyError(f"Key `{param.name}` cannot be solved")

    return solved


def dependency_solver(validate_return_with_model=False):
    def decorator(
        func: Callable[..., Any]
    ) -> Callable[[IncomingMessage], Any]:
        typed_signature = get_typed_signature(func)
        params = typed_signature.parameters
        return_annotation = typed_signature.return_annotation

        func = validate_arguments(config=dict(arbitrary_types_allowed=True))(func)

        @functools.wraps(func)
        def wrapper(message: IncomingMessage):
            solved_params = solve_dependencies(params.values(), message)
            result = func(**solved_params)
            if return_annotation and validate_return_with_model:
                if not issubclass(return_annotation, BaseModel):
                    raise AssertionError("Return annotation should be a subclass if you want to validate it")
                model = return_annotation(**result)
                return model
            return result
        return wrapper
    return decorator


@dependency_solver()
def printer(body: Document, text: str, date: str, message: IncomingMessage, likesCount: str = 0, *, url: str, nonexistant: dict):
    print(body, text, date, message, likesCount, url, nonexistant)
    return body


class DependantCallable:

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)

    def __init__(self, call: Callable[..., Any]):
        self._call = call


if __name__ == '__main__':
    print(printer({'text': "Something went wrong", 'date': "2022-01-01", 'likesCount': 0, 'url': 'http://asdf', 'nonexistant': {'lol': "wut"}}))
