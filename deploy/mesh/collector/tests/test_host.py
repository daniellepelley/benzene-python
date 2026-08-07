"""The Mesh Host wiring — the collector's feeds + queries served over HTTP, and config loading.

Exercises the deployable host in-memory (no container, no AWS): a POST to an ingest route lands in the
collector and a GET to a query route reads it back; a poll sweep folds a fake fleet in; and the fleet
config parses from inline JSON.
"""

from __future__ import annotations

import asyncio
import json

from benzene.mesh import CallableServiceSource, MeshCollector, MeshPoller

from collector.config import load_config
from collector.host import build_mesh_host, run_poll_loop


def test_ingest_and_query_over_http() -> None:
    host = build_mesh_host(sources=[])

    # A service registers by POSTing its descriptor to the ingest route...
    register = asyncio.run(
        host.app.handle(
            "POST",
            "/mesh/register",
            body=json.dumps({"service": "orders", "topics": [{"id": "orders:place"}]}),
        )
    )
    assert register.status_code == 200

    # ...and it shows up in the fleet query.
    fleet = asyncio.run(host.app.handle("GET", "/mesh/fleet"))
    assert fleet.status_code == 200
    assert "orders" in {s["service"] for s in json.loads(fleet.body)["services"]}

    # A per-topic query reads the path parameter (topic ids contain ':').
    topic = asyncio.run(host.app.handle("GET", "/mesh/topic/orders:place"))
    assert json.loads(topic.body)["providers"] == ["orders"]


def test_health_surface_is_served_for_the_load_balancer() -> None:
    host = build_mesh_host(sources=[])
    health = asyncio.run(host.app.handle("GET", "/benzene/health"))
    assert health.status_code == 200
    assert json.loads(health.body)["isHealthy"] is True


def test_poll_loop_folds_a_source_then_stops() -> None:
    async def spec() -> dict:
        return {"service": "inventory", "topics": [{"id": "inventory:reserve"}]}

    async def health() -> dict:
        return {"isHealthy": True, "healthChecks": {}}

    collector = MeshCollector()
    poller = MeshPoller(collector, [CallableServiceSource("inventory", spec=spec, health=health)])

    async def drive() -> None:
        task = asyncio.create_task(run_poll_loop(poller, interval_seconds=0.01))
        for _ in range(100):  # let at least one sweep land
            if collector.query_fleet({})["services"]:
                break
            await asyncio.sleep(0.01)
        task.cancel()

    asyncio.run(drive())
    assert "inventory" in {s["service"] for s in collector.query_fleet({})["services"]}


def test_config_parses_inline_services() -> None:
    config = load_config(
        {"MESH_SERVICES": json.dumps(
            {"pollIntervalSeconds": 15, "services": [{"name": "orders", "baseUrl": "https://orders.svc"}]}
        )}
    )
    assert config.poll_interval_seconds == 15
    assert [s.name for s in config.sources] == ["orders"]
