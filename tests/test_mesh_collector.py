"""The mesh collector: the language-neutral collector fixture plus focused derivation unit tests."""

from __future__ import annotations

import asyncio
import json

import pytest

from benzene.core import BenzeneMessageApplication
from benzene.mesh import MeshCollector, collector_registry

from .conformance_runner import CONFORMANCE_DIR, _mesh_subset, run_mesh_collector


def _cases(fixture: str) -> list:
    return json.loads((CONFORMANCE_DIR / fixture).read_text())["cases"]


def test_mesh_collector_conforms() -> None:
    assert run_mesh_collector() == []  # collector + issue fixtures


@pytest.mark.parametrize(
    "case",
    _cases("mesh-collector-cases.json") + _cases("mesh-issue-cases.json"),
    ids=lambda c: c["name"],
)
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


def test_issues_merge_by_fingerprint_with_delta_counts() -> None:
    c = MeshCollector()
    fp = "aaaa1111aaaa1111aaaa1111aaaa1111"
    c.ingest_issues({"service": "orders", "issues": [
        {"fingerprint": fp, "topic": "order:create", "status": "service-unavailable", "count": 2, "exemplarTraceIds": ["trace-1"]}]})
    c.ingest_issues({"service": "orders", "issues": [
        {"fingerprint": fp, "topic": "order:create", "status": "service-unavailable", "count": 3, "exemplarTraceIds": ["trace-2"]}]})
    issue = next(i for i in c.query_fleet({})["issues"] if i["fingerprint"] == fp)
    assert issue["count"] == 5                              # deltas merge: 2 + 3
    assert issue["exemplarTraceIds"] == ["trace-1", "trace-2"]


def test_issue_merge_keeps_newest_three_exemplars_and_latest_fields() -> None:
    c = MeshCollector()
    fp = "cccc3333cccc3333cccc3333cccc3333"
    for i in range(5):  # five batches, each a new exemplar
        c.ingest_issues({"service": "orders", "issues": [
            {"fingerprint": fp, "topic": "t", "status": "service-unavailable", "count": 1,
             "transport": f"transport-{i}", "exemplarTraceIds": [f"trace-{i}"]}]})
    issue = next(i for i in c.query_fleet({})["issues"] if i["fingerprint"] == fp)
    assert issue["count"] == 5
    assert issue["exemplarTraceIds"] == ["trace-2", "trace-3", "trace-4"]  # newest ≤3 (mesh.md §4.1)
    assert issue["transport"] == "transport-4"                              # other fields latest-wins


def test_invalid_issue_entries_are_skipped_not_rejected() -> None:
    c = MeshCollector()
    accepted = c.ingest_issues({"service": "orders", "issues": [
        {"fingerprint": "", "topic": "order:create", "status": "bad-request", "count": 1},   # skipped
        {"fingerprint": "bbbb", "topic": "order:create", "status": "bad-request", "count": 1}]})
    assert accepted == {"accepted": 1}                     # one valid, one skipped, batch accepted


def test_issues_feed_absence_flagged_only_when_a_failure_needs_explaining() -> None:
    c = MeshCollector()
    c.ingest_traces({"events": [{"traceId": "t", "spanId": "s", "service": "orders",
                                 "topic": "order:create", "status": "service-unavailable"}]})
    orders = next(s for s in c.query_fleet({})["services"] if s["service"] == "orders")
    assert orders["missingFeeds"] == ["descriptor", "health", "issues"]  # unexplained failure
    c.ingest_issues({"service": "orders", "issues": []})   # a liveness beat clears the flag
    orders = next(s for s in c.query_fleet({})["services"] if s["service"] == "orders")
    assert orders["missingFeeds"] == ["descriptor", "health"]
