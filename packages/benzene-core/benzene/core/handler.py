"""Message handlers and their registration records (core-concepts.md sections 3 and 9).

A handler is ``async def handle(request) -> Result``. The concept is *explicit registration*: an
application hands the framework ``(topic, version, handler, request_type, response_type)`` records.
The ``@message`` decorator is idiomatic Python sugar that produces such a record — first-class
explicit registration remains available via :meth:`Registry.register`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from benzene.results import Result

#: A handler: an async function from a request to a Result.
Handler = Callable[[Any], Awaitable[Result]]

_BENZENE_MESSAGE_ATTR = "_benzene_message"


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
            (topic, version, request_type, response_type),
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
