"""A minimal dependency container + per-invocation scope (core-concepts.md section 8).

Benzene defines a small container abstraction rather than mandating a DI framework. The semantics
that must survive a port are: per-invocation scoping, overridable defaults (``try_add*`` — the
framework registers its defaults with ``try_add``, an application's own registration wins), and
construction of handlers/middleware with their dependencies. This is a deliberately small
implementation of those semantics, keyed by an arbitrary token (typically a ``type`` or ``str``).

Mirrors .NET's ``Benzene.Dependencies``; folded into ``benzene.core`` here rather than shipped as a
separate distribution (the C# split existed for assembly isolation, which Python does not need — a
bring-your-own-container adapter would still be its own package).
"""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any


class ServiceNotRegisteredError(KeyError):
    """Raised by :meth:`Scope.get_service` when nothing is registered for a key.

    Subclasses :class:`KeyError` so existing ``except KeyError`` handlers keep working, but the
    message names the missing key and the next action — register it with ``add_singleton`` /
    ``add_scoped`` / ``add_transient`` (or override it in a test with ``add_instance``).
    """

    def __init__(self, key: Any) -> None:
        self.key = key
        super().__init__(
            f"No service registered for {key!r}. Register it on the Container with "
            "add_singleton/add_scoped/add_transient (or add_instance for a fixed value), "
            "or override it in a test via with_services(...)."
        )


class Lifetime(Enum):
    SINGLETON = "singleton"
    SCOPED = "scoped"
    TRANSIENT = "transient"


class _Registration:
    __slots__ = ("factory", "lifetime")

    def __init__(self, factory: Callable[[Scope], Any], lifetime: Lifetime) -> None:
        self.factory = factory
        self.lifetime = lifetime


class Container:
    """Registers services; ``create_scope`` produces a per-invocation :class:`Scope`."""

    def __init__(self) -> None:
        self._registrations: dict[Any, _Registration] = {}
        self._singletons: dict[Any, Any] = {}

    # --- registration ----------------------------------------------------------------------
    def _add(self, key: Any, factory: Callable[[Scope], Any], lifetime: Lifetime) -> Container:
        self._registrations[key] = _Registration(factory, lifetime)
        return self

    def add_singleton(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self._add(key, factory, Lifetime.SINGLETON)

    def add_scoped(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self._add(key, factory, Lifetime.SCOPED)

    def add_transient(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self._add(key, factory, Lifetime.TRANSIENT)

    def add_instance(self, key: Any, instance: Any) -> Container:
        self._singletons[key] = instance
        return self._add(key, lambda _scope: instance, Lifetime.SINGLETON)

    # try_add* register only if absent — the mechanism that makes framework defaults overridable.
    def try_add_singleton(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self if key in self._registrations else self.add_singleton(key, factory)

    def try_add_scoped(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self if key in self._registrations else self.add_scoped(key, factory)

    def try_add_transient(self, key: Any, factory: Callable[[Scope], Any]) -> Container:
        return self if key in self._registrations else self.add_transient(key, factory)

    def create_scope(self) -> Scope:
        return Scope(self)


class Scope:
    """A per-invocation resolution scope. Scoped services live and die with it."""

    def __init__(self, container: Container) -> None:
        self._container = container
        self._scoped: dict[Any, Any] = {}

    def try_get_service(self, key: Any) -> Any | None:
        reg = self._container._registrations.get(key)
        if reg is None:
            return None
        if reg.lifetime is Lifetime.SINGLETON:
            cache = self._container._singletons
        elif reg.lifetime is Lifetime.SCOPED:
            cache = self._scoped
        else:  # transient
            return reg.factory(self)
        if key not in cache:
            cache[key] = reg.factory(self)
        return cache[key]

    def get_service(self, key: Any) -> Any:
        service = self.try_get_service(key)
        if service is None:
            raise ServiceNotRegisteredError(key)
        return service
