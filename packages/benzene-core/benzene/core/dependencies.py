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

import inspect
from collections.abc import Callable
from enum import Enum
from typing import Any

#: A service factory: the full ``(scope) -> T`` form, a zero-arg ``() -> T``, or omitted entirely
#: (construct the type key). :func:`_as_factory` normalizes all three to the internal ``(scope) -> T``.
Factory = Callable[["Scope"], Any] | Callable[[], Any]


def _as_factory(key: Any, factory: Factory | None) -> Callable[[Scope], Any]:
    """Normalize a registration to the internal ``(scope) -> T`` factory.

    Three ergonomic forms collapse to one (core-concepts §8): ``add_singleton(OrderService)`` (no
    factory — construct the type), ``add_singleton(Log, Log)`` (a zero-arg callable), and
    ``add_singleton(Store, lambda scope: ...)`` (the full form, for services that need the scope).
    """
    if factory is None:
        if not isinstance(key, type):
            raise TypeError(
                f"add_*/try_add_* without a factory needs a type to construct; got {key!r}. "
                "Pass a factory: add_singleton(key, lambda scope: ...) or add_singleton(key, make)."
            )
        return lambda _scope: key()
    try:
        required = [
            p
            for p in inspect.signature(factory).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is p.empty
        ]
        takes_scope = len(required) >= 1
    except (ValueError, TypeError):
        takes_scope = True  # un-introspectable (e.g. a builtin) → assume the full (scope) -> T form
    if takes_scope:
        return factory  # type: ignore[return-value]  # the (scope) -> T form, used as-is
    zero_arg = factory
    return lambda _scope: zero_arg()  # type: ignore[call-arg]  # () -> T


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

    def add_singleton(self, key: Any, factory: Factory | None = None) -> Container:
        return self._add(key, _as_factory(key, factory), Lifetime.SINGLETON)

    def add_scoped(self, key: Any, factory: Factory | None = None) -> Container:
        return self._add(key, _as_factory(key, factory), Lifetime.SCOPED)

    def add_transient(self, key: Any, factory: Factory | None = None) -> Container:
        return self._add(key, _as_factory(key, factory), Lifetime.TRANSIENT)

    def add_instance(self, key: Any, instance: Any) -> Container:
        self._singletons[key] = instance
        return self._add(key, lambda _scope: instance, Lifetime.SINGLETON)

    # try_add* register only if absent — the mechanism that makes framework defaults overridable.
    def try_add_singleton(self, key: Any, factory: Factory | None = None) -> Container:
        return self if key in self._registrations else self.add_singleton(key, factory)

    def try_add_scoped(self, key: Any, factory: Factory | None = None) -> Container:
        return self if key in self._registrations else self.add_scoped(key, factory)

    def try_add_transient(self, key: Any, factory: Factory | None = None) -> Container:
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


def use_instance(key: Any, instance: Any) -> Callable[[Container], None]:
    """An override that swaps in one ready-made instance — for ``build_application(overrides=[...])``.

    A host's job is usually to boot the shared composition root with exactly one thing changed: the
    real outbound client for its transport. Saying that takes a named closure whose whole body is a
    single registration, so this is that closure.

    **The explicit form this composes** is the closure itself, which remains the thing to write the
    moment a host needs to register more than one service, or anything other than a fixed instance::

        def use_sns(services: Container) -> None:
            services.add_instance(MessageSender, SnsMessageSender(topic_arn))

        definition, _ = build_application(OrdersStartUp, overrides=[use_sns])

    becomes::

        definition, _ = build_application(
            OrdersStartUp,
            overrides=[use_instance(MessageSender, SnsMessageSender(topic_arn))],
        )

    Overrides run after the startup's own ``configure_services`` and last registration wins, so this
    replaces whatever the composition root registered under ``key`` — see :func:`build_application`.
    """

    def override(services: Container) -> None:
        services.add_instance(key, instance)

    return override
