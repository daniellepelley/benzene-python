"""The handler registry (core-concepts.md sections 2 and 9).

A (topic id, version) pair maps to at most one handler; registering two for the same pair is a
*startup* error, not a runtime dispatch ambiguity. When a message arrives without a version, the
unversioned handler (version = "") handles it; versioned handlers are selected only by exact match.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .handler import Handler, HandlerDefinition, definition_of

if TYPE_CHECKING:  # import cycle: discovery imports handler, registry imports discovery lazily
    from .discovery import HandlerSource


class DuplicateHandlerError(Exception):
    """Raised at registration time when a (topic, version) pair already has a handler."""


class Registry:
    def __init__(self) -> None:
        self._by_key: dict[tuple[str, str], HandlerDefinition] = {}

    def register(
        self,
        topic: str,
        handler: Handler,
        version: str = "",
        request_type: type | None = None,
        response_type: type | None = None,
    ) -> "Registry":
        """Explicitly register a handler (the first-class registration path)."""
        return self.add_definition(
            HandlerDefinition(topic, handler, version, request_type, response_type)
        )

    def add(self, fn: Handler) -> "Registry":
        """Register a ``@message``-tagged function."""
        definition = definition_of(fn)
        if definition is None:
            raise ValueError(
                f"{getattr(fn, '__name__', fn)!r} is not a @message-tagged handler; "
                "use register(topic, fn) for explicit registration."
            )
        return self.add_definition(definition)

    def add_definition(self, definition: HandlerDefinition) -> "Registry":
        key = (definition.topic, definition.version)
        if key in self._by_key:
            raise DuplicateHandlerError(
                f"A handler is already registered for topic {definition.topic!r} "
                f"version {definition.version!r}."
            )
        self._by_key[key] = definition
        return self

    def add_source(self, source: "HandlerSource") -> "Registry":
        """Register everything a :class:`~benzene.core.HandlerSource` produces.

        The seam that keeps scanning as sugar: a source yields the same records an application
        could have passed to :meth:`register` by hand, so nothing downstream can tell them apart.
        """
        for definition in source.definitions():
            self.add_definition(definition)
        return self

    def add_package(self, *packages: Any) -> "Registry":
        """Register every ``@message`` handler under the named package(s)."""
        from .discovery import PackageHandlers

        return self.add_source(PackageHandlers(*packages))

    @classmethod
    def from_package(cls, *packages: Any) -> "Registry":
        """Build a registry from whole package trees — the shortest path to a wired application.

        There is no per-handler registration line to forget: every ``@message`` handler under these
        packages is registered. See :mod:`benzene.core.discovery` for why the package must be named
        (Python discovers a handler only by importing the module that defines it).
        """
        return cls().add_package(*packages)

    def find(self, topic: str, version: str = "") -> HandlerDefinition | None:
        """Resolve a handler by exact (topic, version). No version falls back to unversioned."""
        return self._by_key.get((topic, version))

    def definitions(self) -> list[HandlerDefinition]:
        return list(self._by_key.values())

    def topics(self) -> list[str]:
        """The distinct topic ids registered, sorted. Used to diagnose an unresolved topic."""
        return sorted({topic for topic, _version in self._by_key})

    def versions_for(self, topic: str) -> list[str]:
        """The versions registered for ``topic`` (``""`` meaning unversioned), sorted.

        Empty means the topic itself is unknown — a different bug from a topic that is registered
        but not at the version a message asked for.
        """
        return sorted(version for registered, version in self._by_key if registered == topic)

    def __len__(self) -> int:
        return len(self._by_key)
