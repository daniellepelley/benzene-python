"""Transparent payload casting between schema versions (versioning.md §4).

The [casting-handler pattern](../registry.py) lets one canonical implementation accept an older
producer by hand-writing a thin forwarding handler per retired version. *Transparent casting* removes
that boilerplate: you register the one-step casts **between payload types** once, and Benzene builds
the forwarding handlers — upcasting an older request to the handler's canonical type on the way in and
downcasting the canonical response back on the way out. The handler only ever sees the canonical
version.

Casts compose. Register ``V1 → V2`` and ``V2 → V3`` and a ``V1 → V3`` cast is found automatically by a
breadth-first search over the registered steps, so a new version only needs a cast from the one before
it. A **direct** cast is always preferred over a multi-step chain (BFS yields the shortest path).

    casters = SchemaCasters()
    casters.cast_between(PlaceOrderV1, PlaceOrderV2, lambda v1: PlaceOrderV2(v1.sku, v1.count))
    casters.cast_between(OrderPlacedV2, OrderPlacedV1, lambda v2: OrderPlacedV1(v2.id))  # response

    registry.register("orders:place", place_v2, version="v2",       # the one real implementation
                      request_type=PlaceOrderV2, response_type=OrderPlacedV2)
    registry.register("orders:place",                               # v1 served transparently
                      casting_handler(place_v2, casters, to=PlaceOrderV2, response_to=OrderPlacedV1),
                      version="v1", request_type=PlaceOrderV1, response_type=OrderPlacedV1)

This port keeps its exact-match routing (an unknown version fails loudly), so each *served* version is
still a registration — ``casting_handler`` is what makes that registration a one-liner instead of a
hand-written ``async def``. The casts themselves live in one ``SchemaCasters`` shared across topics.
"""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from typing import Any, Callable, TypeVar

from benzene.results import Result

from .handler import Handler

#: A one-step cast: turn a value of one schema type into the next.
Cast = Callable[[Any], Any]

_F = TypeVar("_F")
_T = TypeVar("_T")


class NoCastPathError(LookupError):
    """Raised when no chain of registered casts connects two schema types."""


class SchemaCasters:
    """A registry of one-step casts between payload types, chained automatically (versioning.md §4).

    Register each step with :meth:`cast_between`; :meth:`cast` finds the shortest chain from a value's
    type to a target type and applies it. Shared across topics — a cast is keyed by its *types*, not by
    any topic, so the same ``PlaceOrderV1 → PlaceOrderV2`` step serves every handler that needs it.
    """

    def __init__(self) -> None:
        # from_type -> {to_type: cast}. A plain adjacency map; BFS walks it on demand.
        self._edges: dict[type, dict[type, Cast]] = {}

    def cast_between(
        self, from_type: type[_F], to_type: type[_T], cast: Callable[[_F], _T]
    ) -> "SchemaCasters":
        """Register a one-step cast ``from_type`` → ``to_type``. Returns self, so calls chain."""
        self._edges.setdefault(from_type, {})[to_type] = cast
        return self

    def cast(self, value: Any, to_type: type[_T]) -> _T:
        """Cast ``value`` to ``to_type`` via the shortest chain of registered steps.

        A value already of ``to_type`` is returned untouched. Raises :class:`NoCastPathError` if no
        chain connects the two types — a configuration error worth failing loudly on.
        """
        if isinstance(value, to_type):
            return value
        chain = self._path(type(value), to_type)
        if chain is None:
            raise NoCastPathError(
                f"No cast path from {type(value).__name__} to {to_type.__name__}. Register the "
                f"missing step with casters.cast_between(...) — steps compose, so you usually only "
                f"need a cast from the immediately preceding version."
            )
        for step in chain:
            value = step(value)
        return value

    def _path(self, from_type: type, to_type: type) -> list[Cast] | None:
        """BFS for the shortest chain of casts ``from_type`` → ``to_type`` (direct preferred)."""
        if from_type is to_type:
            return []
        # queue holds (type_reached, casts_taken_to_get_here); BFS => first arrival is shortest.
        queue: deque[tuple[type, list[Cast]]] = deque([(from_type, [])])
        seen = {from_type}
        while queue:
            current, taken = queue.popleft()
            for nxt, cast in self._edges.get(current, {}).items():
                if nxt is to_type:
                    return [*taken, cast]
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append((nxt, [*taken, cast]))
        return None


def casting_handler(
    target: Handler,
    casters: SchemaCasters,
    *,
    to: type,
    response_to: type | None = None,
) -> Handler:
    """Wrap ``target`` so it serves an older schema version transparently (versioning.md §4).

    The returned handler upcasts its request to ``to`` (the type ``target`` expects) via ``casters``,
    invokes ``target``, and — if ``response_to`` is given — downcasts the successful response payload
    back to the older type. Register it under the older ``(topic, version)`` with that version's
    ``request_type``/``response_type``; ``target`` itself keeps handling only the canonical version.

    The older registration **must** declare ``request_type`` — the router builds the request into that
    type *before* this wrapper runs, and the wrapper casts *from that built type*, not from the raw
    body. Omit it and the request arrives as a ``dict``, giving the puzzling ``No cast path from dict
    to ...`` rather than a clean upcast.

    A failure result passes straight through unchanged: only a successful payload is downcast,
    matching the wire (``encode_response`` serialises a payload only on success), so a domain
    failure never trips a stray ``NoCastPathError`` on its way out.
    """

    async def forward(request: Any) -> Result:
        result = await target(casters.cast(request, to))
        if response_to is not None and result.is_successful and result.payload is not None:
            return replace(result, payload=casters.cast(result.payload, response_to))
        return result

    return forward
