"""The Cloud Service Profile's well-known surfaces, served through each cloud HTTP host.

`standard_paths` is declared in the composition root (`AppDefinition`, alongside the router and
middleware), so every HTTP-capable host and the test harness expose the same `/benzene/*` surfaces
from one declaration. These tests boot a real `BenzeneStartUp` through `create_test_host(...)` and
specialise to each cloud with a single `build_<cloud>()` call — the same lead-by-example path an
adopter copies — then drive the surfaces through the host's HTTP front door.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from benzene.core import (
    AppDefinition,
    BenzeneStartUp,
    HealthChecks,
    Registry,
    ServiceSpec,
)
from benzene.http import HttpRouter, StandardPaths
from benzene.results import Result
from benzene.testing import create_test_host


@dataclass
class Greet:
    name: str = ""


async def greet(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


class Db:
    """A stand-in dependency whose health the /benzene/health check reports (overridable in tests)."""

    def __init__(self, healthy: bool = True) -> None:
        self._healthy = healthy

    def ping(self) -> bool:
        return self._healthy


class GreeterStartUp(BenzeneStartUp):
    """One composition root: registers a service, wires a route, and declares the profile surfaces."""

    def configure_services(self, services, config) -> None:
        services.try_add_singleton(Db, lambda _scope: Db())

    def configure(self, services, config) -> AppDefinition:
        router = HttpRouter()
        router.register("POST", "/greet", "say:hello", greet, request_type=Greet)
        registry = Registry.from_definitions(router)
        db = services.get_service(Db)
        return AppDefinition(
            router=router,
            standard_paths=StandardPaths(
                health=HealthChecks().add("db", db.ping),          # health reflects the injected Db
                spec=ServiceSpec.derive(registry, service="greeter"),
            ),
        )


BUILDERS = ["build_gcp", "build_aws", "build_azure"]
CLOUD_IDS = ["gcp", "aws", "azure"]


def _host(builder_name: str, *, healthy: bool = True):
    """Boot GreeterStartUp through the harness and specialise to one cloud (overriding Db if asked)."""
    builder = create_test_host(GreeterStartUp)
    if not healthy:
        builder = builder.with_services(lambda services: services.add_instance(Db, Db(healthy=False)))
    return getattr(builder, builder_name)()


@pytest.fixture(params=BUILDERS, ids=CLOUD_IDS)
def host(request):
    """A cloud HTTP test host booted from GreeterStartUp, profile surfaces on."""
    return _host(request.param)


def test_health_surface_on_the_cloud_host(host) -> None:
    response = host.send_http("GET", "/benzene/health")
    assert response.status_code == 200
    assert json.loads(response.body)["isHealthy"] is True


def test_spec_surface_on_the_cloud_host(host) -> None:
    # R5's document: the Contract Document, on every cloud host, with no query string needed.
    response = host.send_http("GET", "/benzene/spec")
    assert response.status_code == 200
    doc = json.loads(response.body)
    assert doc["openapi"] == "3.0.1"
    assert doc["info"]["title"] == "greeter"
    assert "say:hello" in {r["topic"] for r in doc["requests"]}


def test_the_native_spec_shape_stays_reachable_on_the_cloud_host(host) -> None:
    # ?type=native has to survive each host's own query-string plumbing, not just the ASGI app's.
    response = host.send_http("GET", "/benzene/spec", query={"type": "native"})
    assert response.status_code == 200
    doc = json.loads(response.body)
    assert doc["service"] == "greeter"
    assert "say:hello" in {t["id"] for t in doc["topics"]}


def test_invoke_surface_on_the_cloud_host(host) -> None:
    envelope = {"topic": "say:hello", "headers": {}, "body": json.dumps({"name": "cloud"})}
    response = host.send_http("POST", "/benzene/invoke", body=envelope)
    assert response.status_code == 200
    outer = json.loads(response.body)
    assert outer["statusCode"] == "ok"
    assert json.loads(outer["body"]) == {"greeting": "Hello cloud"}


def test_ordinary_routes_still_work_alongside_the_surfaces(host) -> None:
    response = host.send_http("POST", "/greet", body={"name": "there"})
    assert response.status_code == 200
    assert json.loads(response.body) == {"greeting": "Hello there"}


@pytest.mark.parametrize("builder_name", BUILDERS, ids=CLOUD_IDS)
def test_unhealthy_check_maps_to_503_through_the_cloud_host(builder_name) -> None:
    # Override Db with a failing one through with_services (the real adopter seam) — the health check
    # the startup declared now reports unhealthy, and 503 + the full aggregate thread out through the
    # cloud host's response, not just the HTTP binding in isolation.
    host = _host(builder_name, healthy=False)
    response = host.send_http("GET", "/benzene/health")
    assert response.status_code == 503
    report = json.loads(response.body)
    assert report["isHealthy"] is False
    assert report["healthChecks"]["db"]["isHealthy"] is False
