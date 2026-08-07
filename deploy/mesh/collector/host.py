"""The Mesh Host — the Benzene-Python analogue of .NET's ``Benzene.Mesh.Host``.

A single long-running service (deployed on Fargate) that:

- runs the :class:`~benzene.mesh.MeshCollector` and exposes its **ingest** feeds and **query** read
  models over HTTP (it *is* a Benzene HTTP service — the same ``collector_registry`` handlers, mapped
  onto routes), and
- runs the :class:`~benzene.mesh.MeshPoller` on a timer, pulling each configured service's
  ``/benzene/spec`` + ``/benzene/health`` into the collector (the .NET "poll the fleet" model).

Pull covers identity/topics/health; the call-graph edges come from **pushed traces** — services POST
their trace batches to ``/mesh/traces``. Both feeds land in the one collector.

This module is transport- and cloud-neutral and fully testable in-memory (see
``deploy/mesh/collector/tests``). ``main.py`` is the container entrypoint that reads the fleet from
config and serves it under uvicorn.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterable
from dataclasses import dataclass

from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import (
    HEARTBEAT_TOPIC,
    ISSUES_TOPIC,
    QUERY_FLEET_TOPIC,
    QUERY_SERVICE_TOPIC,
    QUERY_TOPIC_TOPIC,
    QUERY_TRACE_TOPIC,
    REGISTER_TOPIC,
    TRACES_TOPIC,
    MeshCollector,
    MeshPoller,
    ServiceSource,
    collector_registry,
)

# HTTP surface (method, path, topic): ingest feeds are POSTs; query read models are GETs whose path
# parameter name matches the body key the collector handler reads (service / topic / traceId).
_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("POST", "/mesh/register", REGISTER_TOPIC),
    ("POST", "/mesh/heartbeat", HEARTBEAT_TOPIC),
    ("POST", "/mesh/traces", TRACES_TOPIC),
    ("POST", "/mesh/issues", ISSUES_TOPIC),
    ("GET", "/mesh/fleet", QUERY_FLEET_TOPIC),
    ("GET", "/mesh/service/{service}", QUERY_SERVICE_TOPIC),
    ("GET", "/mesh/topic/{topic}", QUERY_TOPIC_TOPIC),
    ("GET", "/mesh/trace/{traceId}", QUERY_TRACE_TOPIC),
)


@dataclass
class MeshHost:
    """The wired mesh host: the ASGI app to serve, the collector, and the poller."""

    app: BenzeneHttpApp
    collector: MeshCollector
    poller: MeshPoller


def build_mesh_host(
    sources: Iterable[ServiceSource], *, collector: MeshCollector | None = None
) -> MeshHost:
    """Wire the collector's ingest + query topics onto HTTP routes and a poller over ``sources``."""
    collector = collector or MeshCollector()
    registry = collector_registry(collector)

    router = HttpRouter()
    for method, path, topic in _ROUTES:
        definition = registry.find(topic)
        if definition is None:  # pragma: no cover - the collector registry always has these
            raise RuntimeError(f"collector registry is missing the {topic!r} handler")
        router.register(method, path, topic, definition.handler)

    poller = MeshPoller(collector, sources)
    # The host is itself a profile-conformant service: /benzene/health reports the collector is up and
    # /benzene/spec describes the mesh API — so the ALB health check and any peer can see it.
    standard = StandardPaths(
        health=HealthChecks().add("collector", lambda: True),
        spec=ServiceSpec.derive(registry, service="benzene-mesh-host"),
    )
    app = BenzeneHttpApp(
        router, application=BenzeneMessageApplication(registry), standard_paths=standard
    )
    return MeshHost(app=app, collector=collector, poller=poller)


async def run_poll_loop(poller: MeshPoller, *, interval_seconds: float) -> None:
    """Poll the fleet forever on ``interval_seconds`` — the background task the container runs.

    A sweep never raises (a down service is a failed :class:`~benzene.mesh.PollResult`), so the loop
    keeps the mesh fresh regardless of any one service's state.
    """
    while True:
        with contextlib.suppress(Exception):  # defensive: the loop must outlive any sweep hiccup
            await poller.poll_once()
        await asyncio.sleep(interval_seconds)
