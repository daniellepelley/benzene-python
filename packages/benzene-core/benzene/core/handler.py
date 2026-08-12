"""Message handlers and their registration records (core-concepts.md sections 3 and 9).

A handler is ``async def handle(request) -> Result``. The concept is *explicit registration*: an
application hands the framework ``(topic, version, handler, request_type, response_type)`` records.
The ``@message`` decorator is idiomatic Python sugar that produces such a record — first-class
explicit registration remains available via :meth:`Registry.register`.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, get_origin, get_type_hints

from benzene.results import Result

#: A handler: an async function from a request to a Result.
Handler = Callable[[Any], Awaitable[Result]]

_BENZENE_MESSAGE_ATTR = "_benzene_message"


def infer_request_type(handler: Handler) -> type | None:
    """Infer a handler's request type from its first parameter's annotation.

    A handler already declares the shape it wants — ``async def place(request: PlaceOrder)`` — so the
    registration APIs (``@message``, ``Registry.register``, ``HttpRouter.register``) read it straight
    off the signature instead of making you repeat ``request_type=PlaceOrder``. An explicit
    ``request_type`` always wins; this only fills the gap when you leave it off.

    Only a **concrete, non-parameterized class** is inferred (a ``@dataclass`` or a plain type such as
    ``dict``), since that is what :func:`~benzene.core.to_request` maps against. A missing annotation,
    ``typing.Any``, a subscripted generic (``dict[str, Any]``), a union, or an annotation that can't be
    resolved yields ``None`` — the request then passes through unmapped, exactly as it does today when
    no ``request_type`` is given.

    Both exclusions matter for version portability, because ``to_request`` would raise if handed either:

    * A **subscripted generic** — ``get_origin(dict[str, Any])`` is ``dict`` (truthy), so it is rejected.
      This guard is load-bearing on Python 3.10, where ``isinstance(dict[str, Any], type)`` is ``True``
      (unlike 3.11+), so ``isinstance`` alone would let it slip through and then break
      ``isinstance(body, dict[str, Any])``.
    * ``typing.Any`` — on Python 3.11+ it is a class (passes ``isinstance(..., type)``) but can't be
      ``isinstance``-checked against, and ``request: Any`` means "give me whatever arrived" anyway.
    """
    try:
        parameters = list(inspect.signature(handler).parameters.values())
    except (TypeError, ValueError):
        return None
    if not parameters:
        return None
    try:
        hints = get_type_hints(handler)
    except Exception:  # unresolved forward ref, exotic annotation — fall back to no inference
        return None
    annotation = hints.get(parameters[0].name)
    if annotation is Any or get_origin(annotation) is not None or not isinstance(annotation, type):
        return None
    return annotation


@dataclass(frozen=True)
class HandlerDefinition:
    """An explicit (topic, version, handler, types) registration record."""

    topic: str
    handler: Handler
    version: str = ""
    request_type: type | None = None
    response_type: type | None = None


def message(
    topic: str,
    version: str = "",
    request_type: type | None = None,
    response_type: type | None = None,
) -> Callable[[Handler], Handler]:
    """Tag an async handler function with a topic (the annotation *idiom*).

    The tagged function is picked up by :meth:`Registry.add`; it stays an ordinary callable.
    """

    def decorate(fn: Handler) -> Handler:
        setattr(
            fn,
            _BENZENE_MESSAGE_ATTR,
            (topic, version, request_type or infer_request_type(fn), response_type),
        )
        return fn

    return decorate


def definition_of(fn: Handler) -> HandlerDefinition | None:
    """Return the :class:`HandlerDefinition` for a ``@message``-tagged function, if any."""
    tag = getattr(fn, _BENZENE_MESSAGE_ATTR, None)
    if tag is None:
        return None
    topic, version, request_type, response_type = tag
    return HandlerDefinition(topic, fn, version, request_type, response_type)
