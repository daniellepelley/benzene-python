"""Integration test: the demo fleet drives real traffic that the emitter projects into the artifacts.

This boots the three in-process services, generates traffic (``orders`` → ``payments`` → ``shipping``,
with forwarded mesh spans), and runs :class:`~benzene.mesh.MeshArtifactEmitter` over the resulting
catalog spine + live collector — the same path :mod:`mesh_fleet.prove` renders in the browser, minus
Chromium. It asserts the collector genuinely derived the mesh (the ``orders → payments`` edge from trace
parentage, the fan-out producers on ``shipping:book``, observed usage) and that the three manifest
states come out healthy / unhealthy+drift / unreachable, so the fleet stays a faithful fixture for the UI.
"""

from __future__ import annotations

from datetime import datetime, timezone

from benzene.mesh import MeshArtifactEmitter

from .fleet import build_fleet

_AT = datetime(2026, 8, 7, 9, 15, 0, tzinfo=timezone.utc)


def _artifacts():
    fleet = build_fleet(generated_at=_AT)
    emitter = MeshArtifactEmitter(
        fleet.services, fleet.collector,
        generated_at=fleet.generated_at, window_start=fleet.window_start, window_end=fleet.window_end,
    )
    return {
        "manifest": emitter.build_manifest(),
        "topics": emitter.build_topics(),
        "topology": emitter.build_topology(),
        "usage": emitter.build_usage(),
        "services": emitter.build_service_snapshots(),
    }


def test_manifest_shows_the_three_demonstrated_states() -> None:
    by_name = {s["name"]: s for s in _artifacts()["manifest"]["services"]}
    assert by_name["orders"]["status"] == "healthy"
    assert by_name["orders"]["contractDrift"] is False
    assert by_name["payments"]["status"] == "unhealthy"
    assert by_name["payments"]["contractDrift"] is True
    assert by_name["shipping"]["status"] == "unreachable"


def test_unreachable_service_has_no_spec_but_stays_in_the_catalog() -> None:
    artifacts = _artifacts()
    shipping = artifacts["services"]["shipping"]
    assert shipping["specJson"] is None and shipping["health"] is None and shipping["error"]
    # ...yet its topic is still catalogued from the live collector feeds (it registered + traced).
    book = next(t for t in artifacts["topics"]["topics"] if t["topic"] == "shipping:book")
    assert [c["service"] for c in book["consumers"]] == ["shipping"]
    assert [p["service"] for p in book["producers"]] == ["orders", "payments"]


def test_topology_edges_are_collector_derived_from_trace_parentage() -> None:
    edges = _artifacts()["topology"]["edges"]
    pairs = {(e["client"], e["server"]) for e in edges}
    assert ("orders", "payments") in pairs
    assert ("orders", "shipping") in pairs
    assert ("payments", "shipping") in pairs
    for edge in edges:
        assert edge["source"] == "collector"
        assert edge["requestsPerMinute"] is None  # MVP: no trace-source timing
    orders_payments = next(e for e in edges if (e["client"], e["server"]) == ("orders", "payments"))
    assert orders_payments["errorRate"] is not None and orders_payments["errorRate"] > 0


def test_topics_carry_producers_from_the_collector_and_consumers_from_specs() -> None:
    topics = {t["topic"]: t for t in _artifacts()["topics"]["topics"]}
    capture = topics["payment:capture"]
    assert [c["service"] for c in capture["consumers"]] == ["payments"]
    assert capture["consumers"][0]["httpMappings"] == [{"method": "POST", "path": "/payments/capture"}]
    assert [p["service"] for p in capture["producers"]] == ["orders"]


def test_usage_reports_observed_counts_per_status_from_the_collector() -> None:
    entries = _artifacts()["usage"]["entries"]
    by_key = {(e["topic"], e["status"]): e["count"] for e in entries}
    assert by_key[("orders:create", "created")] == 12
    assert by_key[("orders:get-all", "ok")] == 5
    # payments fails 1-in-6 → a real split of ok vs service-unavailable in the feed.
    assert by_key[("payment:capture", "ok")] == 10
    assert by_key[("payment:capture", "service-unavailable")] == 2
    assert all(e["source"] == "collector" for e in entries)


# --- AWS-enriched artifacts (the prove_enriched wiring, minus Chromium) ------------------------
def _enriched_artifacts() -> dict:
    """The same emit path :mod:`mesh_fleet.prove_enriched` renders: canned X-Ray + CloudWatch sources."""
    import json
    import tempfile
    from pathlib import Path

    from .prove_enriched import emit_enriched

    with tempfile.TemporaryDirectory() as out_dir:
        emit_enriched(out_dir)
        root = Path(out_dir)
        return {
            "topology": json.loads((root / "topology.json").read_text()),
            "usage": json.loads((root / "usage.json").read_text()),
        }


def test_enriched_topology_carries_xray_edges_with_real_latency() -> None:
    edges = _enriched_artifacts()["topology"]["edges"]
    xray = [e for e in edges if e["source"] == "xray"]
    assert xray, "expected the X-Ray topology source to enrich the edges"
    op = next(e for e in xray if (e["client"], e["server"]) == ("orders", "payments"))
    # Real X-Ray timing, not the collector plane's nulls.
    assert op["requestsPerMinute"] == 86.4
    assert (op["p50LatencyMs"], op["p95LatencyMs"], op["p99LatencyMs"]) == (45.0, 420.0, 890.0)


def test_enriched_usage_carries_cloudwatch_rows_and_collector_fallback() -> None:
    entries = _enriched_artifacts()["usage"]["entries"]
    cloudwatch = [e for e in entries if e["source"] == "cloudwatch"]
    assert cloudwatch, "expected the CloudWatch usage source to enrich the feed"
    getall = next(
        e for e in cloudwatch if e["topic"] == "orders:get-all" and e["status"] == "ok"
    )
    assert getall["transport"] == "AspNet" and getall["avgDurationMs"] == 14.2
    # shipping:book isn't in the CloudWatch feed, so it stays a collector entry (the fixture's mix).
    shipping = [e for e in entries if e["topic"] == "shipping:book"]
    assert shipping and all(e["source"] == "collector" for e in shipping)
