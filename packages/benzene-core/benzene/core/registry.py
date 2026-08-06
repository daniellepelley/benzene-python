"""The handler registry (core-concepts.md sections 2 and 9).

A (topic id, version) pair maps to at most one handler; registering two for the same pair is a
*startup* error, not a runtime dispatch ambiguity. When a message arrives without a version, the
unversioned handler (version = "") handles it; versioned handlers are selected only by exact match.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from .handler import Handler, HandlerDefinition, definition_of

#: How the router resolves a ``(topic, requested version)`` to a handler (versioning.md §3). A
#: selector is a plain callable, so an application can supply its own policy.
VersionSelector = Callable[["Registry", str, str], "HandlerDefinition | None"]


@runtime_checkable
class SupportsDefinitions(Protocol):
    """Anything that can enumerate its handler definitions — a :class:`Registry` or an ``HttpRouter``."""

    def definitions(self) -> list[HandlerDefinition]: ...


class DuplicateHandlerError(Exception):
    """Raised at registration time when a (topic, version) pair already has a handler."""


class Registry:
    """Maps each ``(topic, version)`` pair to at most one handler.

    ``add`` registers a ``@message``-tagged function; ``register`` is the explicit path (no
    decorator). Registering the same pair twice raises :class:`DuplicateHandlerError` at startup.
    """

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, str], HandlerDefinition] = {}

    @classmethod
    def from_definitions(cls, *sources: SupportsDefinitions) -> Registry:
        """Build a registry from anything exposing ``definitions()`` — e.g. an ``HttpRouter``.

        Collapses the common "a router is not a registry" bridge (``for d in router.definitions():
        registry.add_definition(d)``) into one call. Later registrations still raise
        :class:`DuplicateHandlerError` on a repeated ``(topic, version)`` — chain ``.register(...)``
        to add topics a router doesn't carry (e.g. a queue-only subscriber).
        """
        registry = cls()
        for source in sources:
            for definition in source.definitions():
                registry.add_definition(definition)
        return registry

    def register(
        self,
        topic: str,
        handler: Handler,
        version: str = "",
        request_type: type | None = None,
        response_type: type | None = None,
    ) -> Registry:
        """Explicitly register a handler (the first-class registration path)."""
        return self.add_definition(
            HandlerDefinition(topic, handler, version, request_type, response_type)
        )

    def add(self, fn: Handler) -> Registry:
        """Register a ``@message``-tagged function."""
        definition = definition_of(fn)
        if definition is None:
            raise ValueError(
                f"{getattr(fn, '__name__', fn)!r} is not a @message-tagged handler; "
                "use register(topic, fn) for explicit registration."
            )
        return self.add_definition(definition)

    def add_definition(self, definition: HandlerDefinition) -> Registry:
        key = (definition.topic, definition.version)
        if key in self._by_key:
            where = f"topic {definition.topic!r}" + (
                f" version {definition.version!r}"
                if definition.version
                else " (unversioned)"
            )
            raise DuplicateHandlerError(
                f"A handler is already registered for {where}. Each (topic, version) pair maps to "
                "exactly one handler — remove the duplicate registration, or give one handler a "
                "distinct version."
            )
        self._by_key[key] = definition
        return self

    def find(self, topic: str, version: str = "") -> HandlerDefinition | None:
        """Resolve a handler by exact (topic, version). No version falls back to unversioned."""
        return self._by_key.get((topic, version))

    def definitions(self) -> list[HandlerDefinition]:
        return list(self._by_key.values())

    def versions_of(self, topic: str) -> list[HandlerDefinition]:
        """Every handler registered for ``topic``, across all versions (registration order)."""
        return [d for d in self._by_key.values() if d.topic == topic]


def exact_version(registry: Registry, topic: str, version: str) -> HandlerDefinition | None:
    """The default selector: an exact ``(topic, version)`` match (unversioned when ``version`` is '')."""
    return registry.find(topic, version)


def _natural_key(version: str) -> tuple:
    """A natural-order sort key so ``v2 < v10`` and ``1.9 < 1.10``.

    Splits into digit and non-digit runs; each run is tagged ``(0, int)`` or ``(1, str)`` so ints and
    strings never compare against each other (which would raise).
    """
    return tuple(
        (0, int(run)) if run.isdigit() else (1, run) for run in re.findall(r"\d+|\D+", version)
    )


def highest_version(registry: Registry, topic: str, version: str) -> HandlerDefinition | None:
    """An opt-in selector: the exact match if present, else the **highest** available version.

    "Highest" is a natural ordering of the version strings (``v2`` before ``v10``); for a different
    scheme (e.g. semantic versioning) write your own selector — it is just a callable. Exact match is
    still preferred, so a pinned version always wins over the fallback.
    """
    exact = registry.find(topic, version)
    if exact is not None:
        return exact
    candidates = registry.versions_of(topic)
    if not candidates:
        return None
    return max(candidates, key=lambda d: _natural_key(d.version))
