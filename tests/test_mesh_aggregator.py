"""The mesh aggregator: one pass over a registry of stub services (incl. an unreachable one) → artifacts.

The aggregator is driven through the *real* :class:`~benzene.mesh.aggregator.SpecHealthSource`, only its
HTTP GET is faked — a ``url -> HttpReply`` map — so URL resolution, JSON parsing, the 503-still-has-a-body
health rule, and the connection-error → unreachable rule are all exercised, not stubbed over. The live
collector is seeded with feeds (register/heartbeat/traces) so the derived topology + the
still-in-catalog-though-unreachable behaviour show up in the emitted artifacts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from benzene.http import HttpReply
from benzene.mesh import MeshCollector, spec_hash
from benzene.mesh.aggregator import (
    MeshAggregator,
    MeshServiceEntry,
    MeshServiceRegistry,
    SpecHealthSource,
    run_poll_loop,
)

_AT = datetime(2026, 8, 7, 9, 15, 0, tzinfo=timezone.utc)


def _spec(service: str, topics: list[str]) -> dict:
    return {"service": service, "topics": [{"id": t, "requestSchema": {}, "responseSchema": {}} for t in topics]}


def _healthy() -> dict:
    return {"isHealthy": True, "healthChecks": {"db": {"isHealthy": True}}}


def _unhealthy() -> dict:
    return {"isHealthy": False, "healthChecks": {"gateway": {"isHealthy": False}}}


def _fake_get(routes: dict[str, HttpReply | Exception]):
    """A fake HttpGet: resolve url → HttpReply, or raise (a connection-level failure)."""

    async def get(url: str) -> HttpReply:
        outcome = routes.get(url)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is None:
            raise ConnectionError(f"connection refused: {url}")
        return outcome

    return get


def _seed_collector() -> MeshCollector:
    """A collector fed the three-service demo shape: orders → payments edge, shipping alive via feeds."""
    collector = MeshCollector()
    collector.ingest_register({"service": "orders", "topics": [{"id": "orders:create"}]})
    collector.ingest_register({"service": "payments", "topics": [{"id": "payment:capture"}]})
    collector.ingest_register({"service": "shipping", "topics": [{"id": "shipping:book"}]})
    collector.ingest_heartbeat({"service": "orders", "instanceId": "o1", "health": {"isHealthy": True}})
    collector.ingest_heartbeat({"service": "payments", "instanceId": "p1", "health": {"isHealthy": False}})
    collector.ingest_heartbeat({"service": "shipping", "instanceId": "s1", "health": {"isHealthy": True}})
    # orders(span A) calls payment:capture(span B, parent A) → collector derives orders as producer.
    collector.ingest_traces(
        {
            "events": [
                {"traceId": "t1", "spanId": "A", "service": "orders", "topic": "orders:create", "status": "created"},
                {"traceId": "t1", "spanId": "B", "parentSpanId": "A", "service": "payments",
                 "topic": "payment:capture", "status": "ok"},
                {"traceId": "t1", "spanId": "C", "parentSpanId": "B", "service": "shipping",
                 "topic": "shipping:book", "status": "ok"},
            ]
        }
    )
    return collector


def _registry() -> MeshServiceRegistry:
    return MeshServiceRegistry(
        [
            MeshServiceEntry(name="orders", base_url="http://orders:8080"),
            MeshServiceEntry(name="payments", base_url="http://payments:8080"),
            MeshServiceEntry(name="shipping", base_url="http://shipping:8080"),
        ]
    )


def _run(tmp_path) -> dict:
    collector = _seed_collector()
    routes: dict[str, HttpReply | Exception] = {
        "http://orders:8080/benzene/spec": HttpReply(200, json.dumps(_spec("orders", ["orders:create"]))),
        "http://orders:8080/benzene/health": HttpReply(200, json.dumps(_healthy())),
        "http://payments:8080/benzene/spec": HttpReply(200, json.dumps(_spec("payments", ["payment:capture"]))),
        # payments maps its unhealthy aggregate to 503 — the body must still come through as unhealthy.
        "http://payments:8080/benzene/health": HttpReply(503, json.dumps(_unhealthy())),
        # shipping is unreachable to the aggregator (connection refused) — yet alive in the collector.
        "http://shipping:8080/benzene/spec": ConnectionError("refused"),
        "http://shipping:8080/benzene/health": ConnectionError("refused"),
    }
    source = SpecHealthSource(get=_fake_get(routes))
    # Seed payments' previous hash to something different so contract-drift shows on this first pass.
    aggregator = MeshAggregator(
        collector, source=source, previous_hashes={"payments": "sha-of-a-previous-shape"}
    )
    out_dir = str(tmp_path)
    asyncio.run(aggregator.run_once(_registry(), out_dir=out_dir, generated_at=_AT))
    return out_dir


def _read(out_dir: str, name: str) -> dict:
    with open(os.path.join(out_dir, name)) as handle:
        return json.load(handle)


def test_manifest_shows_healthy_unhealthy_and_unreachable(tmp_path) -> None:
    out_dir = _run(tmp_path)
    by_name = {s["name"]: s for s in _read(out_dir, "manifest.json")["services"]}
    assert by_name["orders"]["status"] == "healthy"
    assert by_name["orders"]["contractDrift"] is False
    assert by_name["payments"]["status"] == "unhealthy"  # 503-with-body, not unreachable
    assert by_name["payments"]["contractDrift"] is True  # seeded previous hash differs
    assert by_name["shipping"]["status"] == "unreachable"  # connection refused on spec + health


def test_unreachable_service_snapshot_has_error_but_stays_in_the_catalog(tmp_path) -> None:
    out_dir = _run(tmp_path)
    shipping = _read(out_dir, os.path.join("services", "shipping.json"))
    assert shipping["specJson"] is None and shipping["health"] is None and shipping["error"]
    # ...yet shipping:book is still catalogued from the live collector feeds.
    book = next(t for t in _read(out_dir, "topics.json")["topics"] if t["topic"] == "shipping:book")
    assert [c["service"] for c in book["consumers"]] == ["shipping"]


def test_topology_carries_the_collector_derived_orders_payments_edge(tmp_path) -> None:
    out_dir = _run(tmp_path)
    edges = {(e["client"], e["server"]): e for e in _read(out_dir, "topology.json")["edges"]}
    assert ("orders", "payments") in edges
    assert edges[("orders", "payments")]["source"] == "collector"
    assert ("payments", "shipping") in edges


def test_usage_reports_collector_observed_counts(tmp_path) -> None:
    out_dir = _run(tmp_path)
    by_key = {(e["topic"], e["status"]): e["count"] for e in _read(out_dir, "usage.json")["entries"]}
    assert by_key[("orders:create", "created")] == 1
    assert by_key[("payment:capture", "ok")] == 1


def test_drift_is_computed_against_the_previous_pass(tmp_path) -> None:
    # Two passes with an unchanged spec → no drift the second time (previous == current).
    collector = _seed_collector()
    routes = {
        "http://orders:8080/benzene/spec": HttpReply(200, json.dumps(_spec("orders", ["orders:create"]))),
        "http://orders:8080/benzene/health": HttpReply(200, json.dumps(_healthy())),
    }
    aggregator = MeshAggregator(collector, source=SpecHealthSource(get=_fake_get(routes)))
    registry = MeshServiceRegistry([MeshServiceEntry(name="orders", base_url="http://orders:8080")])
    out_dir = str(tmp_path)

    async def scenario() -> None:
        await aggregator.run_once(registry, out_dir=out_dir, generated_at=_AT)
        await aggregator.run_once(registry, out_dir=out_dir, generated_at=_AT)

    asyncio.run(scenario())
    orders = next(s for s in _read(out_dir, "manifest.json")["services"] if s["name"] == "orders")
    assert orders["contractDrift"] is False
    # The stored hash is the real spec hash of what was fetched.
    expected = spec_hash(json.dumps(_spec("orders", ["orders:create"]), separators=(",", ":"), sort_keys=True))
    snap = _read(out_dir, os.path.join("services", "orders.json"))
    assert snap["specHash"] == expected


def test_a_failing_pass_never_crashes_the_poll_loop(tmp_path, caplog) -> None:
    class _Boom(SpecHealthSource):
        async def fetch(self, entry):  # type: ignore[override]
            raise RuntimeError("source exploded")

    aggregator = MeshAggregator(MeshCollector(), source=_Boom())
    registry = MeshServiceRegistry([MeshServiceEntry(name="x", base_url="http://x")])

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_poll_loop(aggregator, registry, out_dir=str(tmp_path), interval_seconds=0.02, stop=stop)
        )
        await asyncio.sleep(0.1)  # let a couple of passes fail and be swallowed
        stop.set()
        await task

    with caplog.at_level(logging.ERROR):
        asyncio.run(scenario())
    assert any("aggregation pass failed" in record.message.lower() for record in caplog.records)


def test_poll_loop_re_discovers_the_registry_each_pass(tmp_path) -> None:
    # registry_provider is called each pass, so a fleet that appears after the loop starts is picked up
    # (the hosted re-discovery seam: the host can boot before the services it points at exist).
    calls = {"n": 0}

    def provider() -> MeshServiceRegistry:
        calls["n"] += 1
        # Empty on the first pass, then one service — mimics discovery finding the fleet a pass later.
        if calls["n"] == 1:
            return MeshServiceRegistry(())
        return MeshServiceRegistry([MeshServiceEntry(name="orders", base_url="http://orders")])

    # A source that tolerates the (unreachable) service — the pass must still complete.
    source = SpecHealthSource(get=_fake_get({}))
    aggregator = MeshAggregator(MeshCollector(), source=source)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_poll_loop(
                aggregator,
                MeshServiceRegistry(()),  # the static fallback — ignored while the provider is set
                out_dir=str(tmp_path),
                interval_seconds=0.02,
                stop=stop,
                registry_provider=provider,
            )
        )
        await asyncio.sleep(0.12)
        stop.set()
        await task

    asyncio.run(scenario())
    assert calls["n"] >= 2  # the provider was consulted on every pass, not once at start
    # The most recent pass wrote the discovered service into the catalog spine.
    catalog = _read(str(tmp_path), os.path.join("services", "orders.json"))
    assert catalog["name"] == "orders"
