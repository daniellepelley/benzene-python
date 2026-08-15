"""One discovery + aggregation pass: discover the ``benzene``-labelled Kubernetes Services, interrogate
each over in-cluster HTTP (``/benzene/spec`` + ``/benzene/health``), fold the result into the
:class:`~benzene.mesh.MeshCollector`, and write the mesh-ui catalog artifacts. Shared by the on-demand
``POST /mesh/refresh`` endpoint and the periodic background loop (``main.py``) — mirrors .NET's
``MeshAggregationService``.

Reuses, rather than reimplements, the real Kubernetes discovery mechanism
(``benzene.mesh_fleet.discovery_adapters.KubernetesDiscovery``) and the mesh module's poller +
artifact writer (``benzene.mesh.MeshPoller`` / ``write_artifacts``) — this module is the glue between
them, not a new discovery or aggregation implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

from benzene.mesh import (
    HttpServiceSource,
    MeshCollector,
    MeshPoller,
    ServiceSource,
    write_artifacts,
)
from benzene.mesh_fleet.discovery import Discovery, ServiceEndpoint


def _default_source(endpoint: ServiceEndpoint) -> ServiceSource:
    # Each Kubernetes Service address already carries its port (KubernetesDiscovery reads the
    # Service's first declared port), so a plain http:// prefix is the whole base URL.
    return HttpServiceSource(endpoint.name, f"http://{endpoint.address}")


@dataclass
class MeshDiscoveryService:
    """Discovers the fleet, polls it into the collector, and (optionally) publishes the artifacts."""

    collector: MeshCollector
    discovery: Discovery
    artifact_dir: str | None = None
    #: How a discovered endpoint becomes a pollable source — injectable so a test can drive the poll
    #: step with a fake HTTP fetch (see ``tests/test_mesh.py``) without a real network call.
    source_factory: Callable[[ServiceEndpoint], ServiceSource] = _default_source

    async def run_once(self) -> int:
        """Run one discovery + poll + publish pass. Returns the number of services discovered."""
        endpoints = await self.discovery.discover()
        sources = [self.source_factory(endpoint) for endpoint in endpoints]
        poller = MeshPoller(self.collector, sources)
        await poller.poll_once()
        if self.artifact_dir:
            generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            # write_artifacts's sources param wants spec_url/health_url (for the manifest's links) —
            # a structural superset of the ServiceSource protocol MeshPoller needs. HttpServiceSource
            # (the production source_factory) satisfies both; build_artifacts reads the extra
            # attributes with getattr(default=None), so a narrower test double degrades gracefully
            # rather than crashing (mesh.md §6, "must not invent fields").
            write_artifacts(
                self.artifact_dir, self.collector, sources=cast(Any, sources), generated_at=generated_at
            )
        return len(endpoints)
