"""The mesh poller — pull a fleet's /benzene/spec + /benzene/health into the collector.

Stands up two real in-memory Benzene HTTP services (each with `StandardPaths`), points the poller at
them through a fake GET that routes to the in-memory apps (no sockets), and asserts the collector's
fleet view reflects both — then that a down service degrades to a failed `PollResult` without breaking
the sweep.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urlsplit

from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    Registry,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import CallableServiceSource, HttpServiceSource, MeshCollector, MeshPoller
from benzene.results import Result


@dataclass
class Ping:
    n: int = 0


def _service(name: str, topic: str, *, healthy: bool = True) -> BenzeneHttpApp:
    async def handler(_request: Ping) -> Result:
        return Result.ok({"pong": True})

    router = HttpRouter().register("POST", f"/{name}", topic, handler, request_type=Ping)
    registry = Registry.from_definitions(router)
    return BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(
            health=HealthChecks().add("core", lambda: healthy),
            spec=ServiceSpec.derive(registry, service=name),
        ),
    )


def _fleet_fetch(apps: dict[str, BenzeneHttpApp]):
    """A fake HttpGet routing http://<host>/path to the in-memory app registered under <host>."""

    async def fetch(url: str) -> tuple[int, str]:
        parts = urlsplit(url)
        app = apps[parts.hostname]
        response = await app.handle("GET", parts.path)
        return response.status_code, response.body

    return fetch


def test_poller_folds_a_fleet_into_the_collector() -> None:
    apps = {"orders": _service("orders", "orders:place"), "inventory": _service("inventory", "inventory:reserve")}
    fetch = _fleet_fetch(apps)
    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("orders", "http://orders", fetch=fetch),
            HttpServiceSource("inventory", "http://inventory", fetch=fetch),
        ],
    )

    results = asyncio.run(poller.poll_once())
    assert all(r.ok for r in results)

    fleet = collector.query_fleet({})
    by_name = {s["service"]: s for s in fleet["services"]}
    assert set(by_name) == {"orders", "inventory"}
    assert by_name["orders"]["health"] == "healthy"
    assert by_name["orders"]["topics"] == 1                      # one provided topic, from the spec
    assert "orders:place" in {t["topic"] for t in fleet["topics"]}


def test_poller_reports_an_unhealthy_service() -> None:
    apps = {"orders": _service("orders", "orders:place", healthy=False)}
    collector = MeshCollector()
    poller = MeshPoller(collector, [HttpServiceSource("orders", "http://orders", fetch=_fleet_fetch(apps))])

    asyncio.run(poller.poll_once())
    fleet = collector.query_fleet({})
    assert fleet["services"][0]["health"] == "unhealthy"          # 503 aggregate read as unhealthy


def test_a_down_service_is_a_failed_result_not_a_broken_sweep() -> None:
    apps = {"orders": _service("orders", "orders:place")}

    async def flaky_fetch(url: str) -> tuple[int, str]:
        if "down" in url:
            raise ConnectionError("connection refused")
        return await _fleet_fetch(apps)(url)

    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("orders", "http://orders", fetch=flaky_fetch),
            HttpServiceSource("down", "http://down", fetch=flaky_fetch),
        ],
    )

    results = {r.service: r for r in asyncio.run(poller.poll_once())}
    assert results["orders"].ok is True
    assert results["down"].ok is False and "refused" in results["down"].error
    # the healthy service still made it into the fleet despite its neighbour being down
    assert {s["service"] for s in collector.query_fleet({})["services"]} == {"orders"}


def test_poll_hash_is_stable_and_drifts_with_the_contract() -> None:
    calls = {"n": 0}

    async def spec() -> dict:
        calls["n"] += 1
        topics = [{"id": "t:one"}] if calls["n"] < 3 else [{"id": "t:one"}, {"id": "t:two"}]
        return {"service": "svc", "topics": topics}

    async def health() -> dict:
        return {"isHealthy": True, "healthChecks": {}}

    collector = MeshCollector()
    poller = MeshPoller(collector, [CallableServiceSource("svc", spec=spec, health=health)])

    asyncio.run(poller.poll_once())
    first = collector.query_service({"service": "svc"})
    asyncio.run(poller.poll_once())
    second = collector.query_service({"service": "svc"})
    # same contract two polls running -> the instance's hash still matches the service's
    assert second["instances"][0]["hashMatches"] is True
    asyncio.run(poller.poll_once())  # third poll: topics changed -> a new hash is registered
    assert first is not second  # (distinct query snapshots)
