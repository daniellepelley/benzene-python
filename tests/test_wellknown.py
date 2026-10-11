"""The Cloud Service Profile's well-known HTTP surfaces (profile R3/R4/R5/R7).

`/benzene/invoke` (the wire-envelope endpoint), `/benzene/health` (the health aggregate), and
`/benzene/spec` (the Contract Document, through the pipeline), under a configurable `/benzene/`
prefix, plus the transport-neutral `benzene:spec` interception the HTTP route reaches.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, make_dataclass

import pytest
from benzene.core import (
    SPEC_TOPIC,
    AppDefinition,
    BenzeneMessageApplication,
    ComponentNameCollisionError,
    Context,
    ContractDocument,
    ContractInfo,
    HealthCheckResult,
    HealthChecks,
    Middleware,
    MiddlewarePipeline,
    Next,
    OutboundRegistry,
    Registry,
    application_from,
    contract_hash,
    registration_hash,
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


def test_invoke_answers_200_for_every_processed_envelope_whatever_its_status() -> None:
    # transport-bindings §1 (D17): the envelope carries the outcome, so the status line does not - a
    # deleted result is 200 with its envelope (not 204), a failure is 200 with its problem document,
    # and that problem document omits RFC 9457's `status` because no HTTP response is its own.
    async def remove(_request: dict) -> Result:
        return Result.deleted({"removed": True})

    async def refuse(_request: dict) -> Result:
        return Result.unauthorized("no token")

    router = HttpRouter()
    registry = Registry().register("orders:delete", remove).register("orders:secret", refuse)
    app = BenzeneHttpApp(
        router, application=BenzeneMessageApplication(registry), standard_paths=StandardPaths()
    )

    deleted = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(
        {"topic": "orders:delete", "headers": {}, "body": "{}"})))
    assert deleted.status_code == 200
    assert json.loads(json.loads(deleted.body)["body"]) == {"removed": True}

    refused = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(
        {"topic": "orders:secret", "headers": {}, "body": "{}"})))
    assert refused.status_code == 200  # the pipeline's own auth outcome is a processed envelope
    outer = json.loads(refused.body)
    assert outer["statusCode"] == "unauthorized" and outer["isSuccessful"] is False
    assert "status" not in json.loads(outer["body"])


def test_invoke_topics_restricts_the_endpoint_to_not_found() -> None:
    app = _app(StandardPaths(invoke_topics={"benzene:healthcheck"}))
    envelope = {"topic": "say:hello", "headers": {}, "body": json.dumps({"name": "world"})}
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(envelope)))
    assert response.status_code == 200
    outer = json.loads(response.body)
    # Exactly as if no handler matched (D21) - the restricted handler never runs.
    assert outer["statusCode"] == "not-found"
    assert json.loads(outer["body"])["benzeneStatus"] == "not-found"
    # The ordinary route to the same handler is unaffected: the restriction is the endpoint's.
    routed = asyncio.run(app.handle("POST", "/greet", body=json.dumps({"name": "world"})))
    assert routed.status_code == 200


def test_invoke_topics_accepts_any_container_with_a_rule() -> None:
    class Public:
        def __contains__(self, topic: object) -> bool:
            return isinstance(topic, str) and topic.startswith("say:")

    app = _app(StandardPaths(invoke_topics=Public()))
    envelope = {"topic": "say:hello", "headers": {}, "body": json.dumps({"name": "world"})}
    response = asyncio.run(app.handle("POST", "/benzene/invoke", body=json.dumps(envelope)))
    assert json.loads(response.body)["statusCode"] == "ok"


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
    assert body["healthChecks"]["db"]["status"] == "ok"
    assert set(body["healthChecks"]) == {"db", "queue"}


def test_health_returns_the_full_aggregate_503_when_unhealthy() -> None:
    checks = HealthChecks().add("db", lambda: True).add("queue", lambda: False)
    app = _app(StandardPaths(health=checks))
    response = asyncio.run(app.handle("GET", "/benzene/health"))

    assert response.status_code == 503
    # The health report, not a problem document: problem+json is for an unsuccessful result, and the
    # health query succeeded - only the verdict is unhealthy (wire-contracts §4.1, §5).
    assert response.headers["content-type"] == "application/json"
    body = json.loads(response.body)
    assert body["isHealthy"] is False
    assert body["healthChecks"]["db"]["status"] == "ok"
    assert body["healthChecks"]["queue"]["status"] == "failed"


def test_a_warning_check_is_still_200() -> None:
    checks = HealthChecks().add("disk", lambda: HealthCheckResult.warning("nearly full"))
    response = asyncio.run(_app(StandardPaths(health=checks)).handle("GET", "/benzene/health"))
    assert response.status_code == 200
    assert json.loads(response.body)["healthChecks"]["disk"]["status"] == "warning"


def test_health_absent_when_no_checks_supplied() -> None:
    app = _app(StandardPaths())  # invoke on, health/spec off
    assert asyncio.run(app.handle("GET", "/benzene/health")).status_code == 404


# --- R5: /benzene/spec --------------------------------------------------------------------------


def _spec_app(*middleware: Middleware, outbound: OutboundRegistry | None = None) -> BenzeneHttpApp:
    router, registry = _router_and_registry()
    return BenzeneHttpApp.from_definition(
        AppDefinition(
            registry=registry,
            router=router,
            middleware=list(middleware),
            standard_paths=StandardPaths(),
            outbound=outbound,
            contract=ContractInfo("greeter", service_version="1.2.0", version_scheme="semver"),
        )
    )


def test_spec_serves_the_contract_document_derived_at_the_composition_root() -> None:
    response = asyncio.run(_spec_app().handle("GET", "/benzene/spec"))
    assert response.status_code == 200
    doc = json.loads(response.body)
    assert doc["schemaDialect"] == "openapi-3.0"
    assert (doc["service"], doc["serviceVersion"], doc["versionScheme"]) == (
        "greeter",
        "1.2.0",
        "semver",
    )
    assert doc["messageEndpoint"] == "/benzene/invoke"  # the host's envelope endpoint
    (entry,) = doc["requests"]
    assert entry["topic"] == "say:hello"
    assert entry["httpMappings"] == [{"method": "POST", "path": "/greet"}]  # the host's routes
    # payload schema is projected from the request dataclass (wire naming policy), named once
    assert entry["request"] == {"$ref": "#/components/schemas/Greet"}
    assert doc["components"]["schemas"]["Greet"]["properties"]["name"] == {"type": "string"}
    assert doc["contractHash"] == contract_hash(doc)
    assert doc["registrationHash"] == registration_hash(doc)


def test_spec_carries_the_declared_events_and_sends() -> None:
    # A handler registration makes a service that topic's *consumer*; what it puts on the wire is
    # declared, never inferred — an event it defines, or a send to a topic another party defines.
    outbound = OutboundRegistry().register_event("greet:sent", Greet).register_send("audit:log")
    doc = json.loads(asyncio.run(_spec_app(outbound=outbound).handle("GET", "/benzene/spec")).body)
    assert [e["topic"] for e in doc["events"]] == ["greet:sent"]
    assert [s["topic"] for s in doc["sends"]] == ["audit:log"]
    assert "response" not in doc["sends"][0]  # one-way
    assert [r["topic"] for r in doc["requests"]] == ["say:hello"]  # the consumed side is untouched


@pytest.mark.parametrize(
    ("query", "status"),
    [("type=benzene&format=json", 200), ("type=BENZENE", 200), ("type=asyncapi", 501), ("format=yaml", 501)],
)
def test_spec_copies_the_query_onto_the_benzene_spec_request(query: str, status: int) -> None:
    response = asyncio.run(_spec_app().handle("GET", "/benzene/spec", query))
    assert response.status_code == status


def test_spec_runs_through_the_pipeline_and_its_authentication() -> None:
    # contract-document §7: GET /benzene/spec MUST go through the pipeline, so the same middleware
    # that guards every topic guards the self-description (it lists the service's sends).
    async def deny_without_a_key(context: Context, next: Next) -> None:  # noqa: A002
        if context.headers.get("x-api-key") != "secret":
            context.result = Result.unauthorized("no key")
            return
        await next()

    app = _spec_app(deny_without_a_key)
    assert asyncio.run(app.handle("GET", "/benzene/spec")).status_code == 401
    allowed = asyncio.run(app.handle("GET", "/benzene/spec", headers={"x-api-key": "secret"}))
    assert allowed.status_code == 200


def test_spec_is_absent_when_the_service_declares_no_contract() -> None:
    # Declined -> no output: with no ContractInfo there is no benzene:spec handler to route to.
    assert asyncio.run(_app(StandardPaths()).handle("GET", "/benzene/spec")).status_code == 404


def test_a_component_name_collision_fails_building_the_app() -> None:
    # contract-document §4.1: before the first message, naming both types and the remedy.
    def item(module: str, field_name: str) -> type:
        cls = make_dataclass("Item", [(field_name, str)])
        cls.__module__ = module
        return cls

    async def handle(_request: object) -> Result:
        return Result.ok()

    registry = (
        Registry()
        .register("orders:item", handle, request_type=item("orders", "sku"))
        .register("payments:item", handle, request_type=item("payments", "payment_id"))
    )
    definition = AppDefinition(registry=registry, contract=ContractInfo("shop"))
    with pytest.raises(ComponentNameCollisionError) as raised:
        application_from(definition)
    message = str(raised.value)
    assert "orders.Item" in message and "payments.Item" in message
    assert "orders:item" in message and "payments:item" in message
    assert "@schema_name" in message


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

def _document() -> ContractDocument:
    _, registry = _router_and_registry()
    return ContractDocument.derive(registry, service="greeter", message_endpoint="/benzene/invoke")


def _spec_topic_app() -> BenzeneMessageApplication:
    _, registry = _router_and_registry()
    pipeline = MiddlewarePipeline().use(spec_interception(_document()))
    return BenzeneMessageApplication(registry, pipeline)


def _ask_spec(app: BenzeneMessageApplication, body: str | None) -> dict:
    envelope: dict = {"topic": SPEC_TOPIC, "headers": {}}
    if body is not None:
        envelope["body"] = body
    return asyncio.run(app.handle(envelope))


@pytest.mark.parametrize(
    "body", [None, "", "null", "{}", '{"type": "benzene", "format": "json"}', '{"type": "Benzene"}']
)
def test_spec_topic_answers_the_contract_document_by_default(body: str | None) -> None:
    # contract-document §7: absent, empty, null and {} all mean type benzene / format json.
    response = _ask_spec(_spec_topic_app(), body)
    assert response["statusCode"] == "ok"
    assert json.loads(response["body"]) == _document().to_payload()


@pytest.mark.parametrize("body", ['{"type": "native"}', '{"type": "asyncapi"}', '{"format": "yaml"}'])
def test_spec_topic_answers_an_unsupported_type_or_format_not_implemented(body: str) -> None:
    response = _ask_spec(_spec_topic_app(), body)
    assert response["statusCode"] == "not-implemented"
    assert response["isSuccessful"] is False


def test_spec_topic_rejects_a_request_that_is_not_an_object() -> None:
    assert _ask_spec(_spec_topic_app(), '"benzene"')["statusCode"] == "bad-request"


def test_benzene_ping_is_answered_ok_with_no_payload_without_registration() -> None:
    from benzene.core import PING_TOPIC

    app = BenzeneMessageApplication(Registry())
    response = asyncio.run(
        app.handle({"topic": PING_TOPIC, "headers": {"benzene-version": "v9"}, "body": ""})
    )
    assert response["statusCode"] == "ok"  # version ignored: interception is by topic id alone
    assert response["body"] == ""
