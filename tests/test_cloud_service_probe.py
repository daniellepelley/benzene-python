"""The Cloud Service Profile live-probe checker — audit a service over HTTP, tri-state per R1–R8.

Routes the probe's HTTP calls at a real in-memory ``BenzeneHttpApp`` (no sockets): a fully-provisioned
service audits clean with R6/R7/R8 the by-design ``inconclusive`` trio, a bare service is positively
found missing its surfaces, and an unreachable host degrades to inconclusive rather than a false
failure.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from urllib.parse import urlsplit

from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import (
    ServiceDescriptor,
    ServiceInfo,
    Verdict,
    mesh_interception,
    probe_cloud_service,
)
from benzene.results import Result


@dataclass
class Order:
    sku: str = ""


async def _place(_request: Order) -> Result:
    return Result.created({})


def _client(app: BenzeneHttpApp):
    """An HttpCall that routes the probe's requests to an in-memory app (by path, no socket)."""

    async def call(method: str, url: str, body: str | None) -> tuple[int, str]:
        response = await app.handle(method, urlsplit(url).path, "", {}, body or "")
        return response.status_code, response.body

    return call


def _profiled_app() -> BenzeneHttpApp:
    """A fully-provisioned service: an app topic, mesh descriptor, and the /benzene/* surfaces."""
    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    registry = Registry.from_definitions(router)
    descriptor = ServiceDescriptor.derive(registry, ServiceInfo(service="orders"))
    application = BenzeneMessageApplication(
        registry, MiddlewarePipeline([mesh_interception(descriptor)])
    )
    return BenzeneHttpApp(
        router,
        application=application,
        standard_paths=StandardPaths(
            health=HealthChecks().add("core", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )


def test_a_fully_provisioned_service_audits_clean() -> None:
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(_profiled_app())))
    assert report.is_clean
    assert report.not_satisfied == []
    # The observable requirements are satisfied outright...
    for satisfied in ("R1", "R2", "R3", "R4", "R5", "R7"):
        assert report.verdict(satisfied) is Verdict.SATISFIED, satisfied
    # ...and the structurally-unobservable trio stays inconclusive by design.
    assert report.inconclusive == ["R6", "R8"]


def test_a_bare_service_is_found_missing_its_surfaces() -> None:
    # Only an application route — no standard_paths, no mesh interception.
    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    app = BenzeneHttpApp(router)
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(app)))

    assert report.verdict("R3") is Verdict.NOT_SATISFIED  # /benzene/health 404s
    assert report.verdict("R5") is Verdict.NOT_SATISFIED  # /benzene/spec 404s
    assert report.verdict("R4") is Verdict.NOT_SATISFIED  # /benzene/invoke 404s
    assert "R3" in report.not_satisfied
    assert not report.is_clean


def test_an_unreachable_service_is_inconclusive_not_a_failure() -> None:
    async def dead(_method: str, _url: str, _body: str | None) -> tuple[int, str]:
        raise ConnectionError("connection refused")

    report = asyncio.run(probe_cloud_service("http://down", http=dead))
    # Nothing was reachable, so nothing is positively "not satisfied" — a black-box probe must not
    # claim non-conformance it cannot prove.
    assert report.not_satisfied == []
    assert report.verdict("R1") is Verdict.INCONCLUSIVE
    assert report.verdict("R5") is Verdict.INCONCLUSIVE


def test_a_non_default_prefix_makes_r7_inconclusive() -> None:
    report = asyncio.run(
        probe_cloud_service("http://svc", prefix="/_internal", http=_client(_profiled_app()))
    )
    assert report.verdict("R7") is Verdict.INCONCLUSIVE
    assert "non-default prefix" in _reason(report, "R7")


def test_report_payload_round_trips() -> None:
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(_profiled_app())))
    payload = report.to_payload()
    assert payload["baseUrl"] == "http://svc"
    ids = [r["id"] for r in payload["requirements"]]
    assert ids == ["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8"]
    assert all(r["reason"] for r in payload["requirements"])


def _reason(report, requirement_id: str) -> str:
    return next(p.reason for p in report.probes if p.id == requirement_id)


# --- grading a service this port did not build (the point of a language-neutral probe) ------------

#: What a .NET service actually answers at /benzene/spec: the Contract Document (R5,
#: contract-document.md), including its reserved framework topics. Written out literally rather than
#: produced by this port, so the test would still fail if this port's own emitter drifted.
_DOTNET_CONTRACT_DOCUMENT = {
    "openapi": "3.0.1",
    "info": {"title": "payments-api", "description": "", "version": "2.1.0"},
    "messageEndpoint": "/benzene/invoke",
    "transports": ["api-gateway", "sqs"],
    "requests": [
        {
            "topic": "payments:capture",
            "httpMappings": [{"method": "POST", "path": "/payments"}],
            "request": {"$ref": "#/components/schemas/CapturePayment"},
            "response": {"$ref": "#/components/schemas/PaymentDto"},
        },
        {"topic": "benzene:spec", "reserved": True, "request": {}, "response": {}},
    ],
    "events": [{"topic": "payment:captured", "message": {"$ref": "#/components/schemas/Captured"}}],
    "components": {
        "schemas": {
            "CapturePayment": {"type": "object", "properties": {"orderId": {"type": "string"}}},
            "PaymentDto": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Captured": {"type": "object", "properties": {"id": {"type": "string"}}},
        }
    },
}


def _foreign_service_client():
    """An HttpCall standing in for a non-Python service: R5 answers the Contract Document."""

    async def call(method: str, url: str, body: str | None) -> tuple[int, str]:
        path = urlsplit(url).path
        if path == "/benzene/spec":
            return 200, json.dumps(_DOTNET_CONTRACT_DOCUMENT)
        if path == "/benzene/health":
            return 200, json.dumps({"isHealthy": True, "healthChecks": {}})
        if path == "/benzene/invoke":
            inner = json.dumps({"service": "payments-api", "topics": [{"id": "payments:capture"}]})
            return 200, json.dumps({"statusCode": "ok", "headers": {}, "body": inner})
        return 404, ""

    return call


def test_a_contract_document_service_grades_the_same_as_a_native_one() -> None:
    # The README's claim is that this probe "grades any port's service the same way". It did not:
    # R2 read `topics` and R5 required a {service, topics} document, so the reference implementation
    # of the format R5 actually names came back with R2 unsatisfied and R5 malformed.
    report = asyncio.run(probe_cloud_service("http://payments", http=_foreign_service_client()))
    assert report.not_satisfied == []
    assert report.verdict("R5") is Verdict.SATISFIED
    assert report.verdict("R2") is Verdict.SATISFIED


def test_the_probes_topic_count_ignores_the_reserved_framework_topics() -> None:
    report = asyncio.run(probe_cloud_service("http://payments", http=_foreign_service_client()))
    reason = next(p.reason for p in report.probes if p.id == "R2")
    # benzene:spec is in requests[] and is not an application topic (§5.1's flag-or-prefix rule).
    assert reason == "spec lists 1 application topic(s)"
