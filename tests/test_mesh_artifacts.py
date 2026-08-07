"""Unit tests for the mesh **artifact emitter** — its six projections, against the pinned shapes.

These drive :class:`~benzene.mesh.MeshArtifactEmitter` directly off a hand-built
:class:`~benzene.mesh.MeshCollector` and a couple of :class:`~benzene.mesh.ServiceCatalog` spine
entries, then assert both the *derivation* (who consumes/produces a topic, the collector-derived
edge and usage) and the *shape* (exact field sets) match the mesh-UI artifact contract
(``Benzene.Mesh.Contracts`` / ``website/demos/mesh``). The emitter is pure given a fixed
``generated_at``, so no clock or filesystem is needed here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from benzene.mesh import (
    HttpMapping,
    MeshArtifactEmitter,
    MeshCollector,
    ServiceCatalog,
    TopologyEdge,
    UsageEntry,
    spec_hash,
)

_AT = datetime(2026, 7, 16, 9, 15, 0, tzinfo=timezone.utc)


def _collector() -> MeshCollector:
    """orders provides orders:create; payments provides payment:capture; orders calls payments once."""
    collector = MeshCollector()
    collector.ingest_register({"service": "orders", "topics": [{"id": "orders:create"}], "descriptorHash": "o1"})
    collector.ingest_register({"service": "payments", "topics": [{"id": "payment:capture"}], "descriptorHash": "p1"})
    # A trace: orders (span so) handles orders:create; payments (span sp, parent so) handles payment:capture.
    collector.ingest_traces(
        {
            "events": [
                {"traceId": "t1", "spanId": "so", "service": "orders", "topic": "orders:create", "status": "created"},
                {"traceId": "t1", "spanId": "sp", "parentSpanId": "so", "service": "payments",
                 "topic": "payment:capture", "status": "ok"},
                {"traceId": "t2", "spanId": "so2", "service": "orders", "topic": "orders:create", "status": "created"},
                {"traceId": "t2", "spanId": "sp2", "parentSpanId": "so2", "service": "payments",
                 "topic": "payment:capture", "status": "service-unavailable"},
            ]
        }
    )
    return collector


def _orders() -> ServiceCatalog:
    spec = {"service": "orders", "topics": [
        {"id": "orders:create", "requestSchema": {"type": "object", "properties": {"sku": {"type": "string"}}},
         "responseSchema": {}}]}
    health = {"isHealthy": True, "healthChecks": {"db": {"isHealthy": True}}}
    return ServiceCatalog(
        name="orders", spec=spec, health=health,
        spec_url="https://orders/spec", health_url="https://orders/health",
        http_mappings={"orders:create": [HttpMapping("POST", "/orders")]},
    )


def _payments(previous_hash: str | None) -> ServiceCatalog:
    spec = {"service": "payments", "topics": [
        {"id": "payment:capture", "requestSchema": {"type": "object"}, "responseSchema": {}}]}
    health = {"isHealthy": False, "healthChecks": {"gateway": {"isHealthy": False, "detail": "timeout"}}}
    return ServiceCatalog(
        name="payments", spec=spec, health=health,
        spec_url="https://payments/spec", health_url="https://payments/health",
        previous_spec_hash=previous_hash,
    )


def _emitter(services) -> MeshArtifactEmitter:
    return MeshArtifactEmitter(services, _collector(), generated_at=_AT, window_start=_AT, window_end=_AT)


# --- manifest ---------------------------------------------------------------------------------
def test_manifest_shape_and_statuses() -> None:
    services = [_orders(), _payments(previous_hash="stale-hash"),
                ServiceCatalog("shipping", None, None, "https://s/spec", "https://s/health", error="boom")]
    manifest = _emitter(services).build_manifest()

    assert set(manifest) == {"generatedAtUtc", "services"}
    assert manifest["generatedAtUtc"] == "2026-07-16T09:15:00Z"
    by_name = {s["name"]: s for s in manifest["services"]}
    assert set(by_name["orders"]) == {"name", "status", "contractDrift", "specUrl", "healthUrl"}
    assert by_name["orders"]["status"] == "healthy"
    assert by_name["payments"]["status"] == "unhealthy"
    assert by_name["payments"]["contractDrift"] is True
    assert by_name["shipping"]["status"] == "unreachable"
    assert by_name["shipping"]["contractDrift"] is False


# --- services/<name>.json ---------------------------------------------------------------------
def test_service_snapshot_shape_and_drift() -> None:
    payments = _payments(previous_hash="stale-hash")
    snapshots = _emitter([payments]).build_service_snapshots()
    snap = snapshots["payments"]

    assert set(snap) == {
        "name", "fetchedAtUtc", "specJson", "specHash", "previousSpecHash",
        "contractDrift", "health", "error",
    }
    assert snap["specHash"] == spec_hash(payments.spec_json)
    assert snap["previousSpecHash"] == "stale-hash"
    assert snap["contractDrift"] is True
    assert snap["error"] is None
    assert json.loads(snap["specJson"])["service"] == "payments"


def test_unreachable_service_snapshot_nulls_spec_and_health() -> None:
    shipping = ServiceCatalog("shipping", None, None, "https://s/spec", "https://s/health", error="ConnectionError")
    snap = _emitter([shipping]).build_service_snapshots()["shipping"]
    assert snap["specJson"] is None and snap["specHash"] is None
    assert snap["health"] is None
    assert snap["contractDrift"] is False
    assert snap["error"] == "ConnectionError"


# --- topics.json ------------------------------------------------------------------------------
def test_topics_consumers_from_spec_producers_from_trace_parentage() -> None:
    topics = _emitter([_orders(), _payments(None)]).build_topics()
    assert set(topics) == {"generatedAtUtc", "topics", "removedTopics"}

    by_topic = {t["topic"]: t for t in topics["topics"]}
    assert set(by_topic) == {"orders:create", "payment:capture"}

    capture = by_topic["payment:capture"]
    assert set(capture) == {
        "topic", "version", "reserved", "consumers", "producers", "status",
        "requestSchema", "responseSchema", "messageSchema", "schemaMismatch",
    }
    # consumers = the handler (from the spec); producers = the trace-parent caller (from the collector).
    assert [c["service"] for c in capture["consumers"]] == ["payments"]
    assert [p["service"] for p in capture["producers"]] == ["orders"]
    assert capture["reserved"] is False
    assert capture["messageSchema"] is None

    create = by_topic["orders:create"]
    assert create["consumers"][0]["httpMappings"] == [{"method": "POST", "path": "/orders"}]
    assert create["producers"] == []


def test_reserved_topic_flag_and_hidden_by_prefix() -> None:
    spec = {"service": "svc", "topics": [{"id": "benzene:spec", "requestSchema": {}, "responseSchema": {}}]}
    svc = ServiceCatalog("svc", spec, {"isHealthy": True, "healthChecks": {}}, "u", "u")
    entry = MeshArtifactEmitter([svc], MeshCollector(), generated_at=_AT).build_topics()["topics"][0]
    assert entry["reserved"] is True
    assert entry["status"] is None  # reserved topics never carry a value/deprecation status


def test_schema_mismatch_when_two_consumers_disagree() -> None:
    spec_a = {"service": "a", "topics": [{"id": "shared", "requestSchema": {"type": "object", "properties": {"x": {"type": "string"}}}, "responseSchema": {}}]}
    spec_b = {"service": "b", "topics": [{"id": "shared", "requestSchema": {"type": "object", "properties": {"x": {"type": "integer"}}}, "responseSchema": {}}]}
    health = {"isHealthy": True, "healthChecks": {}}
    services = [ServiceCatalog("a", spec_a, health, "u", "u"), ServiceCatalog("b", spec_b, health, "u", "u")]
    entry = MeshArtifactEmitter(services, MeshCollector(), generated_at=_AT).build_topics()["topics"][0]
    assert entry["schemaMismatch"] is True


# --- topology.json ----------------------------------------------------------------------------
def test_topology_edge_from_collector_trace_parentage() -> None:
    topology = _emitter([_orders(), _payments(None)]).build_topology()
    assert set(topology) == {"generatedAtUtc", "edges"}
    assert len(topology["edges"]) == 1
    edge = topology["edges"][0]
    assert set(edge) == {
        "client", "server", "source", "requestsPerMinute", "errorRate",
        "p50LatencyMs", "p95LatencyMs", "p99LatencyMs",
    }
    assert (edge["client"], edge["server"], edge["source"]) == ("orders", "payments", "collector")
    assert edge["requestsPerMinute"] is None  # no trace-source timing in this MVP
    assert edge["p50LatencyMs"] is None
    assert edge["errorRate"] == 0.5  # 1 of 2 payment:capture invocations failed


# --- usage.json -------------------------------------------------------------------------------
def test_usage_entries_from_collector_status_counts() -> None:
    usage = _emitter([_orders(), _payments(None)]).build_usage()
    assert set(usage) == {"generatedAtUtc", "windowStartUtc", "windowEndUtc", "entries"}
    for entry in usage["entries"]:
        assert set(entry) == {
            "topic", "version", "service", "transport", "status", "count", "avgDurationMs", "source",
        }
        assert entry["source"] == "collector"
        assert entry["service"] is None and entry["transport"] is None and entry["avgDurationMs"] is None

    by_key = {(e["topic"], e["status"]): e["count"] for e in usage["entries"]}
    assert by_key[("orders:create", "created")] == 2
    assert by_key[("payment:capture", "ok")] == 1
    assert by_key[("payment:capture", "service-unavailable")] == 1


# --- external enrichment sources (X-Ray topology / CloudWatch usage) --------------------------
class _FakeTopologySource:
    """A canned external topology source (stands in for the X-Ray source) returning fixed edges."""

    def __init__(self, edges):
        self._edges = edges

    def topology(self):
        return self._edges


class _FakeUsageSource:
    """A canned external usage source (stands in for the CloudWatch source) returning fixed entries."""

    def __init__(self, entries):
        self._entries = entries

    def usage(self):
        return self._entries


def test_topology_source_edge_wins_its_pair_collector_fills_gaps() -> None:
    # The collector derives orders→payments (from trace parentage). X-Ray enriches that same pair with
    # real timing, and adds orders→shipping that the collector never saw.
    xray = _FakeTopologySource([
        TopologyEdge("orders", "payments", "xray", requests_per_minute=86.4, error_rate=0.18,
                     p50_latency_ms=45.0, p95_latency_ms=420.0, p99_latency_ms=890.0),
        TopologyEdge("orders", "shipping", "xray", requests_per_minute=24.1, error_rate=0.004,
                     p50_latency_ms=12.0, p95_latency_ms=35.0, p99_latency_ms=58.0),
    ])
    emitter = MeshArtifactEmitter(
        [_orders(), _payments(None)], _collector(),
        generated_at=_AT, window_start=_AT, window_end=_AT, topology_source=xray,
    )
    edges = {(e["client"], e["server"]): e for e in emitter.build_topology()["edges"]}
    # The X-Ray edge wins the orders→payments pair: source flips to xray and carries real percentiles.
    op = edges[("orders", "payments")]
    assert op["source"] == "xray"
    assert (op["requestsPerMinute"], op["p50LatencyMs"], op["p95LatencyMs"], op["p99LatencyMs"]) == (
        86.4, 45.0, 420.0, 890.0,
    )
    # The X-Ray-only pair appears too; edges stay sorted by (client, server).
    assert list(edges) == [("orders", "payments"), ("orders", "shipping")]
    assert edges[("orders", "shipping")]["source"] == "xray"


def test_topology_unchanged_without_a_source() -> None:
    baseline = _emitter([_orders(), _payments(None)]).build_topology()
    assert [e["source"] for e in baseline["edges"]] == ["collector"]
    assert baseline["edges"][0]["p50LatencyMs"] is None


def test_usage_source_replaces_collector_per_covered_topic() -> None:
    # CloudWatch covers orders:create + payment:capture (with transport/duration); it does NOT cover
    # payment:capture's peer... nothing else, so any topic it omits keeps its collector entries.
    cloudwatch = _FakeUsageSource([
        UsageEntry("orders:create", 8460, "cloudwatch", transport="AspNet", status="created",
                   avg_duration_ms=38.1),
        UsageEntry("payment:capture", 10290, "cloudwatch", transport="Sqs", status="ok",
                   avg_duration_ms=55.0),
    ])
    emitter = MeshArtifactEmitter(
        [_orders(), _payments(None)], _collector(),
        generated_at=_AT, window_start=_AT, window_end=_AT, usage_source=cloudwatch,
    )
    entries = emitter.build_usage()["entries"]
    by_key = {(e["topic"], e["status"], e["source"]): e for e in entries}
    # Covered topics now carry the cloudwatch rows (transport + duration + source tag), replacing collector.
    assert ("orders:create", "created", "cloudwatch") in by_key
    assert by_key[("orders:create", "created", "cloudwatch")]["transport"] == "AspNet"
    assert by_key[("orders:create", "created", "cloudwatch")]["avgDurationMs"] == 38.1
    assert ("payment:capture", "ok", "cloudwatch") in by_key
    # No collector row survives for a covered topic (cloudwatch replaced it wholesale).
    assert not any(e["topic"] == "payment:capture" and e["source"] == "collector" for e in entries)


def test_usage_keeps_collector_entries_for_uncovered_topics() -> None:
    # CloudWatch only covers orders:create; payment:capture stays a collector feed (the fixture's mix).
    cloudwatch = _FakeUsageSource([
        UsageEntry("orders:create", 8460, "cloudwatch", transport="AspNet", status="created",
                   avg_duration_ms=38.1),
    ])
    emitter = MeshArtifactEmitter(
        [_orders(), _payments(None)], _collector(),
        generated_at=_AT, window_start=_AT, window_end=_AT, usage_source=cloudwatch,
    )
    entries = emitter.build_usage()["entries"]
    capture = [e for e in entries if e["topic"] == "payment:capture"]
    assert capture and all(e["source"] == "collector" for e in capture)
    assert {e["status"] for e in capture} == {"ok", "service-unavailable"}


def test_usage_unchanged_without_a_source() -> None:
    baseline = _emitter([_orders(), _payments(None)]).build_usage()
    assert all(e["source"] == "collector" for e in baseline["entries"])
    assert all(e["avgDurationMs"] is None for e in baseline["entries"])


# --- annotations.json -------------------------------------------------------------------------
def test_annotations_empty_by_default_and_seedable() -> None:
    empty = _emitter([_orders()]).build_annotations()
    assert empty == {"generatedAtUtc": "2026-07-16T09:15:00Z", "annotations": []}

    note = {"id": "a1", "entity": "service:payments", "author": "Dani", "text": "note", "createdAtUtc": "2026-07-16T08:00:00Z"}
    seeded = MeshArtifactEmitter([_orders()], _collector(), generated_at=_AT, annotations=[note]).build_annotations()
    assert seeded["annotations"] == [note]


# --- emit (filesystem) ------------------------------------------------------------------------
def test_emit_writes_all_six_artifacts(tmp_path) -> None:
    _emitter([_orders(), _payments(None)]).emit(str(tmp_path))
    for name in ("manifest.json", "topics.json", "topology.json", "usage.json", "annotations.json"):
        assert (tmp_path / name).is_file()
    assert (tmp_path / "services" / "orders.json").is_file()
    assert (tmp_path / "services" / "payments.json").is_file()
    # Every file is valid JSON.
    assert json.loads((tmp_path / "manifest.json").read_text())["services"][0]["name"] == "orders"
