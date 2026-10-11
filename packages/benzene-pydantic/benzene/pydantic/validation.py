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

import types
import typing
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar, Union, get_args, get_origin

from benzene.core import Handler, json_pointer
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


def validation_errors(
    exc: ValidationError, model: type[BaseModel] | None = None
) -> list[BenzeneError]:
    """Turn a ``ValidationError`` into structured errors (wire-contracts.md section 1.3).

    pydantic already knows, per bad field, exactly the three things a problem document wants: the
    message, the location it came from, and the identifier of the rule that rejected it. The
    message and rule cross unchanged (pydantic's ``type`` becomes ``code``); the location becomes
    ``field`` as a **JSON Pointer** into the request's JSON form (``/items/0/sku``) — so a caller
    can resolve it against the body it sent, in any language.

    Pass the ``model`` that was validated and each property name is the one the model
    **serializes** under (``model_dump(by_alias=True)`` — an alias generator, a field's ``alias`` or
    ``serialization_alias``), which is the name on the wire; union-member tags pydantic puts in a
    location are dropped, because they are not part of the JSON. Without it, ``loc`` is used as
    pydantic reports it. A rule with no location — a ``@model_validator``, a cross-field rule —
    omits ``field`` rather than inventing one.
    """
    errors: list[BenzeneError] = []
    for error in exc.errors():
        errors.append(
            BenzeneError(
                message=str(error["msg"]),
                field=field_pointer(error["loc"], model),
                code=str(error["type"]),
            )
        )
    return errors


def field_pointer(loc: Sequence[int | str], model: type[BaseModel] | None = None) -> str | None:
    """The JSON Pointer for one pydantic error location, or ``None`` for a location-less rule.

    ``("items", 0, "skuCode")`` against a model serializing ``items[].sku_code`` as ``skuCode``
    → ``"/items/0/skuCode"``.
    """
    if not loc:
        return None
    segments: list[str | int] = []
    current: Any = model
    for part in loc:
        if isinstance(part, int):
            segments.append(part)
            current = _item_type(current)
            continue
        resolved = _resolve(current, part)
        if resolved is None:  # a union-member tag: narrows the type, is not a JSON segment
            current = _union_member(current, part)
            continue
        name, current = resolved
        segments.append(name)
    return json_pointer(segments) if segments else None


def _resolve(annotation: Any, part: str) -> tuple[str, Any] | None:
    """The serialized name ``part`` addresses in ``annotation`` and the type found there, or
    ``None`` when ``part`` is a union-member tag rather than a property."""
    annotation = _unwrap(annotation)
    for candidate in _members(annotation):
        if isinstance(candidate, type) and issubclass(candidate, BaseModel):
            model_fields: Mapping[str, Any] = getattr(candidate, "model_fields", {})
            for name, info in model_fields.items():
                validation_alias = info.validation_alias
                names = {name, info.alias, info.serialization_alias}
                if isinstance(validation_alias, str):
                    names.add(validation_alias)
                if part in names:
                    return (info.serialization_alias or info.alias or name), info.annotation
    if _union_member(annotation, part) is not None:
        return None
    # A mapping key, or a location this walk cannot type: the key is the JSON name verbatim.
    origin = get_origin(annotation)
    if isinstance(origin, type) and issubclass(origin, Mapping):
        args = get_args(annotation)
        return part, (args[1] if len(args) == 2 else None)
    return part, None


def _item_type(annotation: Any) -> Any:
    """The element type of a list/tuple/sequence annotation (``list[Item]`` → ``Item``)."""
    args = get_args(_unwrap(annotation))
    if not args:
        return None
    return args[0]


def _union_member(annotation: Any, tag: str) -> Any:
    """The member of a union whose pydantic tag (its class name) is ``tag``, if any."""
    for member in _members(_unwrap(annotation)):
        if getattr(member, "__name__", None) == tag:
            return member
    return None


def _members(annotation: Any) -> tuple[Any, ...]:
    if get_origin(annotation) in (Union, types.UnionType):
        return tuple(arg for arg in get_args(annotation) if arg is not type(None))
    return (annotation,)


def _unwrap(annotation: Any) -> Any:
    """Strip ``Annotated`` and ``Optional`` so the shape underneath can be walked."""
    while get_origin(annotation) is typing.Annotated:
        annotation = get_args(annotation)[0]
    members = _members(annotation)
    return members[0] if len(members) == 1 else annotation


def validated(model: type[M]) -> Callable[[Callable[[M], Awaitable[Result]]], Handler]:
    """Wrap a handler so its request is validated into ``model`` first (a pydantic ``BaseModel``).

    The wrapped handler receives a validated ``model`` instance; on a ``ValidationError`` the request
    is answered with ``validation-error`` and one structured error per bad field - each carrying a
    JSON Pointer to it in the request as sent (``/items/0/sku``, using the model's serialized names)
    and pydantic's rule identifier - without invoking the handler. Apply
    ``@message(topic)`` *above* ``@validated(model)`` and leave ``request_type`` unset — the raw
    decoded body flows in and this decorator validates it.
    """

    def decorate(fn: Callable[[M], Awaitable[Result]]) -> Handler:
        async def wrapper(request: object) -> Result:
            try:
                instance = model.model_validate(request)
            except ValidationError as exc:
                return Result.validation_error(*validation_errors(exc, model))
            return await fn(instance)

        wrapper.__name__ = getattr(fn, "__name__", "validated_handler")
        wrapper.__qualname__ = getattr(fn, "__qualname__", wrapper.__name__)
        wrapper.__doc__ = fn.__doc__
        return wrapper

    return decorate
