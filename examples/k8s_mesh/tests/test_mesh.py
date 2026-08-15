"""In-memory tests for the mesh's discovery + aggregation pass.

Fakes only the two external edges — the Kubernetes API (a fake ``Discovery``) and the polled HTTP
surfaces (a ``CallableServiceSource`` per discovered endpoint) — and drives the *real*
``MeshDiscoveryService`` against a real ``MeshCollector``, proving discover -> poll -> collector ->
artifacts end to end without a cluster, matching .NET's kind-workflow assertion
(``{"discovered": 3}``) but in memory.
"""

from __future__ import annotations

import asyncio
import json

from benzene.mesh import CallableServiceSource, MeshCollector
from benzene.mesh_fleet.discovery import ServiceEndpoint

from k8s_mesh.mesh.discovery_service import MeshDiscoveryService


class _FakeDiscovery:
    """A :class:`~benzene.mesh_fleet.discovery.Discovery` returning a fixed endpoint list."""

    def __init__(self, endpoints: list[ServiceEndpoint]) -> None:
        self._endpoints = endpoints

    async def discover(self) -> list[ServiceEndpoint]:
        return list(self._endpoints)


def _fake_source(name: str, healthy: bool = True):
    async def spec() -> dict:
        return {"service": name, "topics": [{"id": f"{name}:topic", "requestSchema": {}, "responseSchema": {}}]}

    async def health() -> dict:
        return {"isHealthy": healthy, "healthChecks": {}}

    return CallableServiceSource(name=name, spec=spec, health=health)


def _service_factory(sources: dict[str, CallableServiceSource]):
    def factory(endpoint: ServiceEndpoint):
        return sources[endpoint.name]

    return factory


async def _run(service: MeshDiscoveryService) -> int:
    return await service.run_once()


def test_run_once_discovers_and_registers_every_endpoint() -> None:
    endpoints = [
        ServiceEndpoint(name="orders", address="orders.benzene-mesh.svc.cluster.local:80"),
        ServiceEndpoint(name="payments", address="payments.benzene-mesh.svc.cluster.local:80"),
        ServiceEndpoint(name="shipping", address="shipping.benzene-mesh.svc.cluster.local:80"),
    ]
    sources = {e.name: _fake_source(e.name) for e in endpoints}
    collector = MeshCollector()
    service = MeshDiscoveryService(
        collector, _FakeDiscovery(endpoints), source_factory=_service_factory(sources)
    )

    discovered = asyncio.run(_run(service))

    assert discovered == 3
    fleet_names = {s["service"] for s in collector.query_fleet({})["services"]}
    assert fleet_names == {"orders", "payments", "shipping"}


def test_run_once_writes_manifest_and_topics_artifacts(tmp_path) -> None:
    endpoints = [ServiceEndpoint(name="orders", address="orders.benzene-mesh.svc.cluster.local:80")]
    sources = {e.name: _fake_source(e.name) for e in endpoints}
    collector = MeshCollector()
    service = MeshDiscoveryService(
        collector,
        _FakeDiscovery(endpoints),
        artifact_dir=str(tmp_path),
        source_factory=_service_factory(sources),
    )

    discovered = asyncio.run(_run(service))

    assert discovered == 1
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["services"][0]["name"] == "orders"
    topics = json.loads((tmp_path / "topics.json").read_text())
    assert any(t["topic"] == "orders:topic" for t in topics["topics"])


def test_empty_mesh_is_zero_not_an_error() -> None:
    collector = MeshCollector()
    service = MeshDiscoveryService(collector, _FakeDiscovery([]))

    assert asyncio.run(_run(service)) == 0
