"""The handler registry (core-concepts.md sections 2 and 9).

A (topic id, version) pair maps to at most one handler; registering two for the same pair is a
*startup* error, not a runtime dispatch ambiguity. When a message arrives without a version, the
unversioned handler (version = "") handles it; versioned handlers are selected only by exact match.
"""

from __future__ import annotations

from .handler import Handler, HandlerDefinition, definition_of


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

    def find(self, topic: str, version: str = "") -> HandlerDefinition | None:
        """Resolve a handler by exact (topic, version). No version falls back to unversioned."""
        return self._by_key.get((topic, version))

    def definitions(self) -> list[HandlerDefinition]:
        return list(self._by_key.values())
