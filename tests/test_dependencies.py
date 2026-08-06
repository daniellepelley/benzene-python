"""The per-invocation DI container — lifetimes and the missing-service error (core-concepts §7).

`Container` registers factories under three lifetimes and mints a `Scope` per invocation; the lifetime
decides caching: a singleton is one instance for the whole app, a scoped service is one per scope, a
transient is fresh on every resolve.
"""

from __future__ import annotations

import pytest
from benzene.core import Container, ServiceNotRegisteredError


class Thing:
    _n = 0

    def __init__(self) -> None:
        Thing._n += 1
        self.id = Thing._n


def setup_function() -> None:
    Thing._n = 0


def test_singleton_is_one_instance_across_scopes() -> None:
    container = Container().add_singleton(Thing, lambda _scope: Thing())
    a, b = container.create_scope(), container.create_scope()
    assert a.get_service(Thing) is b.get_service(Thing)  # same instance everywhere


def test_scoped_is_one_instance_within_a_scope_but_new_across_scopes() -> None:
    container = Container().add_scoped(Thing, lambda _scope: Thing())
    scope = container.create_scope()
    assert scope.get_service(Thing) is scope.get_service(Thing)  # cached within the scope
    other = container.create_scope()
    assert other.get_service(Thing) is not scope.get_service(Thing)  # fresh per scope


def test_transient_is_fresh_on_every_resolve() -> None:
    container = Container().add_transient(Thing, lambda _scope: Thing())
    scope = container.create_scope()
    assert scope.get_service(Thing) is not scope.get_service(Thing)  # never cached


def test_add_instance_registers_a_ready_made_singleton() -> None:
    thing = Thing()
    container = Container().add_instance(Thing, thing)
    assert container.create_scope().get_service(Thing) is thing


def test_try_add_respects_an_existing_registration() -> None:
    first = Thing()
    container = (
        Container()
        .add_instance(Thing, first)
        .try_add_singleton(Thing, lambda _scope: Thing())   # ignored — already registered
        .try_add_scoped(Thing, lambda _scope: Thing())
        .try_add_transient(Thing, lambda _scope: Thing())
    )
    assert container.create_scope().get_service(Thing) is first


def test_missing_service_raises_a_teaching_error() -> None:
    scope = Container().create_scope()
    assert scope.try_get_service(Thing) is None            # the soft form: None, no raise
    with pytest.raises(ServiceNotRegisteredError, match="No service registered for"):
        scope.get_service(Thing)                            # the hard form: a clear error naming the key
