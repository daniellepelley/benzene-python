"""The mesh dashboard end to end: a typed, health-beating, traced fleet → the full artifact set.

Asserts that :func:`write_demo_artifacts` lays down every mesh-ui artifact and that each view is
genuinely populated from the fleet — schemas + version on the functional map, per-check health on the
service page, and a call graph + usage derived from real traces — not just present-but-empty.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from mesh_dashboard.demo import build_demo_artifacts, build_demo_mesh, write_demo_artifacts


def test_write_demo_artifacts_lays_down_the_full_set(tmp_path: Path) -> None:
    artifacts = asyncio.run(write_demo_artifacts(tmp_path, orders=3))

    for name in ("manifest", "topics", "topology", "usage", "asyncapi", "annotations"):
        assert (tmp_path / f"{name}.json").is_file(), f"{name}.json not written"
    for service in ("orders", "inventory", "notifications"):
        assert (tmp_path / "services" / f"{service}.json").is_file()

    # The returned dicts match what was written to disk.
    on_disk = json.loads((tmp_path / "manifest.json").read_text())
    assert on_disk == artifacts["manifest"]


def test_manifest_lists_the_healthy_fleet() -> None:
    artifacts = build_demo_artifacts()
    statuses = {s["name"]: s["status"] for s in artifacts["manifest"]["services"]}
    assert statuses == {"orders": "healthy", "inventory": "healthy", "notifications": "healthy"}


def test_topics_carry_schemas_and_version() -> None:
    artifacts = build_demo_artifacts()
    topics = {t["topic"]: t for t in artifacts["topics"]["topics"]}

    place = topics["orders:place"]
    assert place["version"] == "2"  # the descriptor declared orders:place at version 2
    # The request/response schemas are derived from the typed registry, not hand-written.
    assert place["requestSchema"]["properties"].keys() >= {"sku", "quantity"}
    assert place["responseSchema"]["properties"].keys() >= {"sku", "reserved"}
    assert place["schemaMismatch"] is False


def test_service_page_has_per_check_health_and_spec() -> None:
    artifacts = build_demo_artifacts()
    orders = artifacts["services"]["orders"]

    assert orders["health"]["isHealthy"] is True
    assert set(orders["health"]["healthChecks"]) == {"database", "inventory-api"}
    assert orders["specHash"] is not None
    # specJson round-trips to the retained contract for the service's provided topic.
    spec = json.loads(orders["specJson"])
    assert spec["service"] == "orders"
    assert [t["id"] for t in spec["topics"]] == ["orders:place"]


def test_topology_and_usage_come_from_real_traces() -> None:
    mesh = build_demo_mesh()
    asyncio.run(mesh.place_order("ABC", 2))
    from benzene.mesh import build_artifacts

    artifacts = build_artifacts(mesh.collector, generated_at="2026-01-01T00:00:00Z")

    # The call graph the collector stitched from span parentage: orders→inventory→notifications.
    edges = {(e["client"], e["server"]) for e in artifacts["topology"]["edges"]}
    assert ("orders", "inventory") in edges
    assert ("inventory", "notifications") in edges

    # Usage counts one exercise per (topic, service, status) per hop.
    exercised = {(e["topic"], e["service"]) for e in artifacts["usage"]["entries"]}
    assert ("orders:place", "orders") in exercised
    assert ("inventory:reserve", "inventory") in exercised


def test_absent_metrics_degrade_to_null_not_fabricated() -> None:
    # The pull+trace catalog has no metrics feed, so latency/rate stay null rather than invented —
    # the projection's honesty contract (mesh-ui.md §6), asserted here so the demo keeps proving it.
    artifacts = asyncio.run(write_demo_artifacts_dict())
    for edge in artifacts["topology"]["edges"]:
        assert edge["p50LatencyMs"] is None
        assert edge["requestsPerMinute"] is None
    assert artifacts["usage"]["windowStartUtc"] is None
    for entry in artifacts["usage"]["entries"]:
        assert entry["avgDurationMs"] is None


async def write_demo_artifacts_dict() -> dict:
    mesh = build_demo_mesh()
    await mesh.place_order("ABC", 1)
    from benzene.mesh import build_artifacts

    return build_artifacts(mesh.collector, generated_at="2026-01-01T00:00:00Z")


def test_asyncapi_exports_the_domain_topics() -> None:
    artifacts = build_demo_artifacts()
    asyncapi = artifacts["asyncapi"]
    assert asyncapi["asyncapi"] == "3.0.0"
    addresses = {ch["address"] for ch in asyncapi["channels"].values()}
    assert {"orders:place", "inventory:reserve", "notify:send"} <= addresses
    # Reserved benzene:* plumbing is kept out of the domain export.
    assert not any(addr.startswith("benzene:") for addr in addresses)
