"""Integration test: the distributed mesh, over real localhost sockets, with no browser.

This is the browser-free half of the Mesh Host proof (``prove.py`` adds Chromium): it brings the four
separate ASGI apps up on real ports, drives ``orders`` traffic over HTTP, lets every service push its
feeds to the host over HTTP, runs one aggregation pass, then asserts against the artifacts the **host
serves over HTTP** that the live multi-process fleet came through — the three manifest states, the
collector-derived ``orders → payments`` edge (from trace parentage that crossed process boundaries), and
the still-in-catalog-though-unreachable ``shipping``. It runs in the normal pytest suite (localhost only,
seconds).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from benzene.http import stdlib_get_transport

from mesh.stack import run_stack

_UI_HTML = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "examples", "mesh_fleet", "mesh-ui.html")
)
_AT = datetime(2026, 8, 7, 9, 15, 0, tzinfo=timezone.utc)


async def _fetch_json(url: str) -> dict:
    reply = await stdlib_get_transport()(url)
    assert reply.status_code == 200, f"{url} → HTTP {reply.status_code}"
    return json.loads(reply.body)


def test_distributed_fleet_reaches_the_host_and_renders_the_three_states() -> None:
    async def scenario() -> None:
        stack = await run_stack(ui_html=_UI_HTML, generated_at=_AT, creates=12, lists=5)
        try:
            base = stack.host_url

            # 1. The host serves the vendored UI and every artifact over HTTP (not a throwaway server).
            ui = await stdlib_get_transport()(stack.ui_url)
            assert ui.status_code == 200 and "<html" in ui.body.lower()

            # 2. The manifest the host serves shows the three states from the live HTTP fleet.
            manifest = await _fetch_json(f"{base}/manifest.json")
            by_name = {s["name"]: s for s in manifest["services"]}
            assert by_name["orders"]["status"] == "healthy"
            assert by_name["orders"]["contractDrift"] is False
            assert by_name["payments"]["status"] == "unhealthy"  # /benzene/health 503-with-body
            assert by_name["payments"]["contractDrift"] is True  # seeded previous hash differs
            assert by_name["shipping"]["status"] == "unreachable"  # no spec/health surfaces

            # 3. shipping is unreachable to the aggregator, yet alive on the mesh via its HTTP feeds.
            shipping = await _fetch_json(f"{base}/services/shipping.json")
            assert shipping["specJson"] is None and shipping["error"]
            topics = await _fetch_json(f"{base}/topics.json")
            book = next(t for t in topics["topics"] if t["topic"] == "shipping:book")
            assert [c["service"] for c in book["consumers"]] == ["shipping"]
            assert [p["service"] for p in book["producers"]] == ["orders", "payments"]

            # 3b. The HTTP route mappings now travel over the wire: orders' /benzene/spec carries the
            #     optional topics[].http field, so topics.json consumers[].httpMappings are populated for
            #     the HTTP-fronted topics — closing the "producer gap" the distributed path used to show
            #     (orders:create / orders:get-all were `gap` because the neutral spec had no route table).
            by_topic = {t["topic"]: t for t in topics["topics"]}
            create = by_topic["orders:create"]
            create_consumer = next(c for c in create["consumers"] if c["service"] == "orders")
            assert create_consumer["httpMappings"] == [{"method": "POST", "path": "/orders"}]
            assert create["status"] != "gap"
            get_all = by_topic["orders:get-all"]
            get_all_consumer = next(c for c in get_all["consumers"] if c["service"] == "orders")
            assert get_all_consumer["httpMappings"] == [{"method": "GET", "path": "/orders"}]
            assert get_all["status"] != "gap"

            # 4. The topology edges were derived by the collector from trace parentage that crossed
            #    process boundaries over HTTP — the distinguishing win.
            topology = await _fetch_json(f"{base}/topology.json")
            edges = {(e["client"], e["server"]): e for e in topology["edges"]}
            assert ("orders", "payments") in edges
            assert edges[("orders", "payments")]["source"] == "collector"

            # 5. The collector observed real per-status usage from the cross-process traffic.
            usage = await _fetch_json(f"{base}/usage.json")
            counts = {(e["topic"], e["status"]): e["count"] for e in usage["entries"]}
            assert counts[("orders:create", "created")] == 12
            assert counts[("payment:capture", "service-unavailable")] == 2
        finally:
            await stack.close()

    asyncio.run(scenario())


def test_collector_is_reachable_as_a_networked_service() -> None:
    async def scenario() -> None:
        stack = await run_stack(ui_html=_UI_HTML, generated_at=_AT, drive=False)
        try:
            # The host's own /benzene/spec + /benzene/health answer — the collector is a real service.
            health = await _fetch_json(f"{stack.host_url}/benzene/health")
            assert health["isHealthy"] is True
            spec = await _fetch_json(f"{stack.host_url}/benzene/spec")
            topic_ids = {t["id"] for t in spec["topics"]}
            assert "benzene:mesh:query:fleet" in topic_ids
        finally:
            await stack.close()

    asyncio.run(scenario())
