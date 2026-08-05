"""The mesh collector: the language-neutral collector fixture plus focused derivation unit tests."""

from __future__ import annotations

import asyncio
import json

import pytest

from benzene.core import BenzeneMessageApplication
from benzene.mesh import MeshCollector, collector_registry

from .conformance_runner import CONFORMANCE_DIR, _mesh_subset, run_mesh_collector


def _collector_cases() -> list:
    return json.loads((CONFORMANCE_DIR / "mesh-collector-cases.json").read_text())["cases"]


def test_mesh_collector_conforms() -> None:
    assert run_mesh_collector() == []


@pytest.mark.parametrize("case", _collector_cases(), ids=lambda c: c["name"])
def test_collector_case(case: dict) -> None:
    app = BenzeneMessageApplication(collector_registry(MeshCollector()))
    for step in case["steps"]:
        response = asyncio.run(app.handle(step["request"]))
        expected = step["expected"]
        assert response["statusCode"] == expected["statusCode"]
        if "body" in expected:
            body = json.loads(response["body"]) if response["body"] else {}
            assert _mesh_subset(expected["body"], body)


# --- focused unit tests of the derivation rules (against MeshCollector directly) ------------------

def _register(c: MeshCollector, service: str, topics: list[str], descriptor_hash: str | None = None) -> None:
    body = {"service": service, "topics": [{"id": t} for t in topics]}
    if descriptor_hash:
        body["descriptorHash"] = descriptor_hash
    c.ingest_register(body)


def test_reregistration_replaces_provider_edges() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])
    _register(c, "orders", ["order:cancel"])  # replaces wholesale
    # order:create is still a known topic, but orders no longer provides it
    assert c.query_topic({"topic": "order:create"})["providers"] == []
    assert c.query_topic({"topic": "order:cancel"})["providers"] == ["orders"]


def test_consumer_edges_are_derived_from_trace_parentage() -> None:
    c = MeshCollector()
    _register(c, "greeter", ["greet"])
    c.ingest_traces({"events": [
        {"traceId": "t1", "spanId": "s-front", "service": "frontdoor", "topic": "welcome", "status": "ok"},
        {"traceId": "t1", "spanId": "s-greet", "parentSpanId": "s-front", "service": "greeter", "topic": "greet", "status": "ok"},
    ]})
    topic = c.query_topic({"topic": "greet"})
    assert topic["providers"] == ["greeter"]
    assert topic["consumers"] == ["frontdoor"]  # parent span from a different service


def test_heartbeats_drive_health_and_hash_mismatch() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"], descriptor_hash="sha256:aaaa")
    c.ingest_heartbeat({"service": "orders", "instanceId": "i1", "descriptorHash": "sha256:aaaa",
                        "health": {"isHealthy": True, "healthChecks": {}}})
    assert c.query_service({"service": "orders"})["health"] == "healthy"
    c.ingest_heartbeat({"service": "orders", "instanceId": "i2", "descriptorHash": "sha256:bbbb",
                        "health": {"isHealthy": False, "healthChecks": {}}})
    service = c.query_service({"service": "orders"})
    assert service["health"] == "degraded"                 # mixed instances
    i2 = next(i for i in service["instances"] if i["instanceId"] == "i2")
    assert i2["healthy"] is False and i2["hashMatches"] is False


def test_missing_feeds_and_anonymous_service() -> None:
    c = MeshCollector()
    _register(c, "orders", ["order:create"])  # descriptor only
    fleet = c.query_fleet({})
    orders = next(s for s in fleet["services"] if s["service"] == "orders")
    assert orders["health"] == "unknown"
    assert orders["missingFeeds"] == ["health", "traces"]
    # a trace-only service is anonymous (no descriptor, no health)
    c.ingest_traces({"events": [{"traceId": "t", "spanId": "s", "service": "frontdoor", "topic": "welcome", "status": "ok"}]})
    front = c.query_service({"service": "frontdoor"})
    assert front["invocations"] == 1
    assert front["missingFeeds"] == ["descriptor", "health"]
