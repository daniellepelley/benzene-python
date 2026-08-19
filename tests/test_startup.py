"""Tests for the composition root (``BenzeneStartUp`` + ``build_application``) and DI overrides.

Leads by example: exercises the feature through the public seams (a real ``BenzeneStartUp`` booted
by ``build_application``, driven end to end via ``InMemoryBenzeneHost``) and proves the
``with_services`` override — the same seam adopters and the cloud test hosts use — actually wins.
"""

from __future__ import annotations

import asyncio
import json

from benzene.core import (
    AppDefinition,
    BenzeneStartUp,
    Container,
    Registry,
    Scope,
    build_application,
    use_instance,
)
from benzene.results import Result, Status
from benzene.testing import InMemoryBenzeneHost


class Greeter:
    def __init__(self, prefix: str = "hello") -> None:
        self.prefix = prefix

    def greet(self, name: str) -> str:
        return f"{self.prefix} {name}"


class GreetStartUp(BenzeneStartUp):
    def configure_services(self, services: Container, config) -> None:
        services.try_add_singleton(Greeter, lambda _s: Greeter(config.get("prefix", "hello")))

    def configure(self, services: Scope, config) -> AppDefinition:
        greeter = services.get_service(Greeter)
        registry = Registry()

        async def handle(request: dict) -> Result:
            return Result.ok({"message": greeter.greet(request.get("name", ""))})

        registry.register("greet", handle)
        return AppDefinition(registry=registry)


def _greet(definition: AppDefinition, name: str) -> dict:
    host = InMemoryBenzeneHost(definition.registry)
    response = asyncio.run(host.send_message("greet", {"name": name}))
    assert response["statusCode"] == Status.OK
    return json.loads(response["body"])


def test_build_application_boots_the_app_from_its_startup() -> None:
    definition, scope = build_application(GreetStartUp)
    assert _greet(definition, "sam") == {"message": "hello sam"}
    assert isinstance(scope.get_service(Greeter), Greeter)


def test_with_services_override_wins_over_startup_defaults() -> None:
    definition, _ = build_application(
        GreetStartUp, overrides=[lambda c: c.add_instance(Greeter, Greeter("hi"))]
    )
    # The overridden dependency is what the handler resolves — the fake-injection seam works.
    assert _greet(definition, "sam") == {"message": "hi sam"}


def test_use_instance_is_the_named_closure_every_host_used_to_write() -> None:
    # The shorthand and the closure it composes must be interchangeable, or the ladder is a fiction.
    by_hand, _ = build_application(
        GreetStartUp, overrides=[lambda c: c.add_instance(Greeter, Greeter("hi"))]
    )
    by_shorthand, _ = build_application(
        GreetStartUp, overrides=[use_instance(Greeter, Greeter("hi"))]
    )
    assert _greet(by_shorthand, "sam") == _greet(by_hand, "sam") == {"message": "hi sam"}


def test_with_config_layers_onto_startup() -> None:
    definition, _ = build_application(GreetStartUp, config={"prefix": "yo"})
    assert _greet(definition, "x") == {"message": "yo x"}


def test_harness_with_config_threads_config_into_the_built_host() -> None:
    """The harness's ``with_config`` seam (parallel to ``with_services``) must reach the composition root."""
    import pytest

    pytest.importorskip("benzene.aws")
    from benzene.testing import create_test_host

    host = create_test_host(GreetStartUp).with_config({"prefix": "yo"}).build_aws()
    assert host.scope is not None
    # Config supplied through the harness reached configure_services and shaped the resolved service.
    assert host.scope.get_service(Greeter).prefix == "yo"
