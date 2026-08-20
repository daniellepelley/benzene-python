"""Validate a handler's request with a `pydantic <https://docs.pydantic.dev>`_ model.

Python developers reach for pydantic to validate and parse request payloads. The :func:`validated`
decorator makes a Benzene handler take a pydantic model as its request: the decoded body is validated
into the model *before* the handler runs, and a ``pydantic.ValidationError`` becomes a
``validation-error`` :class:`~benzene.results.Result` that names each bad field — so a malformed
request never reaches your handler and never crashes the pipeline.

    from benzene.core import message
    from benzene.pydantic import validated
    from benzene.results import Result
    from pydantic import BaseModel

    class PlaceOrder(BaseModel):
        sku: str
        quantity: int = 1

    @message("orders:place")            # no request_type — the raw body flows in, validated below
    @validated(PlaceOrder)
    async def place(order: PlaceOrder) -> Result:
        return Result.created(order)    # a model returned as payload serializes via model_dump

A pydantic model returned as a success payload is serialized by ``benzene.core``'s wire mapper
(``model_dump(by_alias=True)``), so a model with a camelCase alias generator crosses the wire in the
Benzene naming policy.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from benzene.core import Handler
from benzene.results import BenzeneError, Result

from pydantic import BaseModel, ValidationError

M = TypeVar("M", bound=BaseModel)


def format_validation_errors(exc: ValidationError) -> list[str]:
    """Turn a ``ValidationError`` into readable ``"field: message"`` strings (one per bad field).

    Kept for callers that want the flat prose. New code should prefer
    :func:`validation_errors`, which gives the same information without gluing it together.
    """
    return [
        f"{error.field or '(body)'}: {error.message}" for error in validation_errors(exc)
    ]


def validation_errors(exc: ValidationError) -> list[BenzeneError]:
    """Turn a ``ValidationError`` into structured errors (wire-contracts.md section 1.3).

    pydantic already knows, per bad field, exactly the three things a problem document wants: the
    message, the location it came from, and the identifier of the rule that rejected it. This maps
    them across unchanged - ``loc`` joined with dots becomes ``field``, pydantic's ``type`` becomes
    ``code`` - so a caller can attach the failure to the right input and branch on the rule, rather
    than string-matching prose that was glued together on the way out.
    """
    errors: list[BenzeneError] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        errors.append(
            BenzeneError(
                message=str(error["msg"]),
                field=location or None,
                code=str(error["type"]),
            )
        )
    return errors


def validated(model: type[M]) -> Callable[[Callable[[M], Awaitable[Result]]], Handler]:
    """Wrap a handler so its request is validated into ``model`` first (a pydantic ``BaseModel``).

    The wrapped handler receives a validated ``model`` instance; on a ``ValidationError`` the request
    is answered with ``validation-error`` and one structured error per bad field - each carrying
    pydantic's own field path and rule identifier - without invoking the handler. Apply
    ``@message(topic)`` *above* ``@validated(model)`` and leave ``request_type`` unset — the raw
    decoded body flows in and this decorator validates it.
    """

    def decorate(fn: Callable[[M], Awaitable[Result]]) -> Handler:
        async def wrapper(request: object) -> Result:
            try:
                instance = model.model_validate(request)
            except ValidationError as exc:
                return Result.validation_error(*validation_errors(exc))
            return await fn(instance)

        wrapper.__name__ = getattr(fn, "__name__", "validated_handler")
        wrapper.__qualname__ = getattr(fn, "__qualname__", wrapper.__name__)
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorate
