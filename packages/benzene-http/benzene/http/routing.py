"""HTTP route table for the inbound HTTP binding (transport-bindings.md section 2, HTTP).

The HTTP binding resolves a topic *from route/method conventions*. In Python that convention is
an explicit ``@http_endpoint(method, path)`` tag paired with the ``@message`` topic tag: the HTTP
route says *where* the request arrives, ``@message`` says *which handler* it resolves to. A handler
may carry several routes (stack the decorator).

Paths use ``{name}`` placeholders (e.g. ``/orders/{id}``); a placeholder matches a single path
segment and is surfaced to the handler as a request field. Routes are matched in registration
order, first match wins.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from benzene.core import (
    DuplicateHandlerError,
    Handler,
    HandlerDefinition,
    definition_of,
    infer_request_type,
)

_HTTP_ROUTES_ATTR = "_benzene_http_routes"

#: A ``{name}`` placeholder in a route template.
_PARAM_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def http_endpoint(method: str, path: str) -> Callable[[Handler], Handler]:
    """Tag an async handler with an HTTP ``(method, path)`` route.

    Pairs with :func:`benzene.core.message`, which supplies the topic the route resolves to. Stack
    the decorator to give one handler several routes. The tagged function stays an ordinary callable.
    """

    def decorate(fn: Handler) -> Handler:
        routes = list(getattr(fn, _HTTP_ROUTES_ATTR, ()))
        routes.append((method.upper(), path))
        setattr(fn, _HTTP_ROUTES_ATTR, routes)
        return fn

    return decorate


def routes_of(fn: Handler) -> list[tuple[str, str]]:
    """Return the ``(method, path)`` routes tagged onto a function, if any."""
    return list(getattr(fn, _HTTP_ROUTES_ATTR, ()))


def _compile(path: str) -> tuple[re.Pattern[str], tuple[str, ...]]:
    # Escape the literal segments between placeholders so a regex metacharacter in a route
    # (``.``, ``+``, ``(`` …) is matched literally — otherwise ``/users/me.json`` would also match
    # ``/users/meXjson``. Only the ``{name}`` placeholders become a real pattern.
    params: list[str] = []
    parts: list[str] = []
    last = 0
    for m in _PARAM_RE.finditer(path):
        parts.append(re.escape(path[last : m.start()]))
        params.append(m.group(1))
        parts.append(f"(?P<{m.group(1)}>[^/]+)")
        last = m.end()
    parts.append(re.escape(path[last:]))
    return re.compile("^" + "".join(parts) + "$"), tuple(params)


@dataclass(frozen=True)
class HttpEndpoint:
    """A single ``(method, path) -> (topic, version)`` route."""

    method: str
    path: str
    topic: str
    version: str = ""
    _regex: re.Pattern[str] = field(init=False, repr=False, compare=False)
    _params: tuple[str, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        regex, params = _compile(self.path)
        object.__setattr__(self, "_regex", regex)
        object.__setattr__(self, "_params", params)

    def match(self, method: str, path: str) -> dict[str, str] | None:
        """Return the captured path parameters if this endpoint matches, else ``None``."""
        if method.upper() != self.method:
            return None
        matched = self._regex.match(path)
        if matched is None:
            return None
        return matched.groupdict()


class HttpRouter:
    """A registration-ordered table of :class:`HttpEndpoint` s plus the handlers they resolve to.

    ``add`` registers a ``@message`` + ``@http_endpoint``-tagged function; ``register`` is the
    first-class explicit path (mirroring :meth:`benzene.core.Registry.register`). The router also
    carries the underlying :class:`~benzene.core.HandlerDefinition` records so the app can build a
    message registry from it.

    Like :class:`~benzene.core.Registry`, a ``(topic, version)`` pair maps to exactly one handler:
    binding an already-registered topic to a *different* handler raises
    :class:`~benzene.core.DuplicateHandlerError` at registration time. Several routes for the *same*
    handler are the intended stacked-decorator case and stay legal.
    """

    def __init__(self) -> None:
        self._endpoints: list[HttpEndpoint] = []
        self._definitions: dict[tuple[str, str], HandlerDefinition] = {}

    def add(self, fn: Handler) -> HttpRouter:
        """Register a ``@message`` + ``@http_endpoint``-tagged handler for all of its routes."""
        definition = definition_of(fn)
        if definition is None:
            raise ValueError(
                f"{getattr(fn, '__name__', fn)!r} is not a @message-tagged handler; "
                "use register(method, path, topic, fn) for explicit registration."
            )
        routes = routes_of(fn)
        if not routes:
            raise ValueError(
                f"{getattr(fn, '__name__', fn)!r} has no @http_endpoint route; "
                "add one, or use register(method, path, topic, fn)."
            )
        for method, path in routes:
            self._register(method, path, definition)
        return self

    def register(
        self,
        method: str,
        path: str,
        topic: str,
        handler: Handler,
        version: str = "",
        request_type: type | None = None,
        response_type: type | None = None,
    ) -> HttpRouter:
        """Explicitly map a route to a topic/handler (no decorators required).

        ``request_type`` is inferred from the handler's first-parameter annotation when omitted
        (:func:`~benzene.core.infer_request_type`); pass it explicitly to override.
        """
        if request_type is None:
            request_type = infer_request_type(handler)
        definition = HandlerDefinition(topic, handler, version, request_type, response_type)
        self._register(method, path, definition)
        return self

    def _register(self, method: str, path: str, definition: HandlerDefinition) -> None:
        key = (definition.topic, definition.version)
        existing = self._definitions.get(key)
        # One handler may own several routes (stacked @http_endpoint) — that registers the same
        # definition again, which is fine. A *different* handler on the same topic is a wiring
        # mistake: silently rebinding it would make every route serve the last handler registered.
        if existing is not None and existing.handler is not definition.handler:
            raise DuplicateHandlerError(
                f"Route {method.upper()} {path} maps topic {definition.topic!r} to a different "
                "handler than an earlier route. Two routes may share a topic only when they share "
                "the handler (stack @http_endpoint, or register the same function); otherwise give "
                "each handler its own topic."
            )
        self._endpoints.append(HttpEndpoint(method, path, definition.topic, definition.version))
        self._definitions[key] = definition

    def match(self, method: str, path: str) -> tuple[HttpEndpoint, dict[str, str]] | None:
        """Resolve ``(method, path)`` to an endpoint and its captured path params, first match."""
        for endpoint in self._endpoints:
            params = endpoint.match(method, path)
            if params is not None:
                return endpoint, params
        return None

    def endpoints(self) -> list[HttpEndpoint]:
        return list(self._endpoints)

    def definitions(self) -> list[HandlerDefinition]:
        return list(self._definitions.values())
