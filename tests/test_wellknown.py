"""The Cloud Service Profile's well-known HTTP surfaces (profile R3/R4/R5/R7).

`/benzene/invoke` (the wire-envelope endpoint), `/benzene/health` (the health aggregate), and
`/benzene/spec` (the derived spec document), under a configurable `/benzene/` prefix, plus the
transport-neutral `benzene:spec` interception the HTTP `/benzene/spec` surface mirrors.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from benzene.core import (
    SPEC_TOPIC,
    BenzeneMessageApplication,
    HealthCheckResult,
    HealthChecks,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
    spec_interception,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.results import Result


@dataclass
class Greet:
    name: str = ""


async def greet(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})


def _router_and_registry() -> tuple[HttpRouter, Registry]:
    router = HttpRouter()
    router.register("POST", "/greet", "say:hello", greet, request_type=Greet)
    return router, Registry.from_definitions(router)


def _app(standard: StandardPaths | None = None) -> BenzeneHttpApp:
    router, registry = _router_and_registry()
    return BenzeneHttpApp(
        router, application=BenzeneMessageApplication(registry), standard_paths=standard
    )


# --- R4: /benzene/invoke ------------------------------------------------------------------------

def test_invoke_round_trips_a_message_envelope() -> None:
    app = _app(StandardPaths())
    envelope = {"topic": "say:hello", "headers": {}, "body": json.dumps({"name": "world"})}
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(envelope)))

    assert response.status_code == 200            # transport processed the envelope
    outer = json.loads(response.body)
    assert outer["statusCode"] == "ok"            # domain outcome lives in the response envelope
    assert json.loads(outer["body"]) == {"greeting": "Hello world"}


def test_invoke_carries_the_domain_outcome_in_the_envelope_not_the_http_status() -> None:
    app = _app(StandardPaths())
    envelope = {"topic": "no:such:topic", "headers": {}, "body": "{}"}
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(envelope)))
    assert response.status_code == 200            # the envelope endpoint is a uniform transport
    assert json.loads(response.body)["statusCode"] == "not-found"


def test_invoke_rejects_a_malformed_envelope_at_the_transport() -> None:
    app = _app(StandardPaths())
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body="{not json"))
    assert response.status_code == 400            # a bad request envelope is a transport error


def test_invoke_off_by_default_without_standard_paths() -> None:
    app = _app(None)
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body="{}"))
    assert response.status_code == 404            # no well-known surfaces wired -> just a route miss


# --- R3: /benzene/health ------------------------------------------------------------------------

def test_health_returns_the_aggregate_200_when_healthy() -> None:
    checks = HealthChecks().add("db", lambda: True).add("queue", lambda: HealthCheckResult.healthy())
    app = _app(StandardPaths(health=checks))
    response = asyncio.run(app.handle("GET", "/benzene/health"))

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body["isHealthy"] is True
    assert body["healthChecks"]["db"]["isHealthy"] is True
    assert set(body["healthChecks"]) == {"db", "queue"}


def test_health_returns_the_full_aggregate_503_when_unhealthy() -> None:
    checks = HealthChecks().add("db", lambda: True).add("queue", lambda: False)
    app = _app(StandardPaths(health=checks))
    response = asyncio.run(app.handle("GET", "/benzene/health"))

    assert response.status_code == 503
    body = json.loads(response.body)
    # The whole aggregate survives on an unhealthy response (the envelope would have dropped it).
    assert body["isHealthy"] is False
    assert body["healthChecks"]["db"]["isHealthy"] is True
    assert body["healthChecks"]["queue"]["isHealthy"] is False


def test_health_absent_when_no_checks_supplied() -> None:
    app = _app(StandardPaths())  # invoke on, health/spec off
    assert asyncio.run(app.handle("GET", "/benzene/health")).status_code == 404


# --- R5: /benzene/spec --------------------------------------------------------------------------

def test_spec_derives_topics_and_schemas_from_the_registry() -> None:
    router, registry = _router_and_registry()
    spec = ServiceSpec.derive(registry, service="greeter")
    app = BenzeneHttpApp(
        router, application=BenzeneMessageApplication(registry), standard_paths=StandardPaths(spec=spec)
    )

    response = asyncio.run(app.handle("GET", "/benzene/spec"))
    assert response.status_code == 200
    doc = json.loads(response.body)
    assert doc["service"] == "greeter"
    topics = {t["id"]: t for t in doc["topics"]}
    assert "say:hello" in topics
    # payload schema is projected from the request dataclass (wire naming policy)
    assert topics["say:hello"]["requestSchema"]["properties"]["name"] == {"type": "string"}


def test_spec_source_may_be_a_callable_redrived_each_request() -> None:
    router, registry = _router_and_registry()
    app = BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(spec=lambda: ServiceSpec.derive(registry, service="greeter")),
    )
    doc = json.loads(asyncio.run(app.handle("GET", "/benzene/spec")).body)
    assert doc["service"] == "greeter"


# --- R7: prefix convention ----------------------------------------------------------------------

def test_prefix_is_configurable_and_relocates_every_surface() -> None:
    checks = HealthChecks().add("db", lambda: True)
    app = _app(StandardPaths(prefix="/_ops", health=checks))
    assert asyncio.run(app.handle("GET", "/_ops/health")).status_code == 200
    assert asyncio.run(app.handle("GET", "/benzene/health")).status_code == 404  # moved, not both


def test_standard_surfaces_do_not_shadow_ordinary_routes() -> None:
    app = _app(StandardPaths())
    response = asyncio.run(app.handle("POST", "/greet", body=json.dumps({"name": "there"})))
    assert response.status_code == 200
    assert json.loads(response.body) == {"greeting": "Hello there"}


# --- the transport-neutral benzene:spec interception (core) --------------------------------------

def test_spec_interception_answers_the_reserved_topic_on_any_transport() -> None:
    _, registry = _router_and_registry()
    spec = ServiceSpec.derive(registry, service="greeter")
    pipeline = MiddlewarePipeline().use(spec_interception(spec))
    app = BenzeneMessageApplication(registry, pipeline)

    response = asyncio.run(app.handle({"topic": SPEC_TOPIC, "headers": {}, "body": "{}"}))
    assert response["statusCode"] == "ok"
    assert json.loads(response["body"])["service"] == "greeter"
