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
from k8s_mesh.mesh.static import MeshUiApp


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


async def _serve(app: MeshUiApp, path: str) -> tuple[int, str, bytes]:
    events: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.disconnect"}

    async def send(event: dict) -> None:
        events.append(event)

    await app({"type": "http", "path": path, "method": "GET"}, receive, send)
    status = events[0]["status"]
    content_type = next(v for k, v in events[0]["headers"] if k == b"content-type").decode()
    body = events[1]["body"]
    return status, content_type, body


def test_ui_page_carries_the_mount_s_own_absolute_manifest_and_fleet_urls(tmp_path) -> None:
    """Regression: the page's default *relative* ``manifest.json`` fetch resolves against
    ``document.baseURI`` — the page's own URL. Reached at the bare mount (``/mesh-ui``, no trailing
    slash, exactly what .NET's k8s Ingress/Service and this repo's own README both point at), that
    resolves to ``/manifest.json`` at the site root — but this mount serves artifacts nested under
    ``/mesh-ui/`` (deliberately, to avoid colliding with ``/benzene/*``/``/mesh/*``), so the unstamped
    page 404s. Stamping the absolute URLs sidesteps ``document.baseURI`` entirely.
    """
    ui_html = tmp_path / "mesh-ui.html"
    ui_html.write_text('<!doctype html><html lang="en"><body>mesh-ui</body></html>')
    app = MeshUiApp(inner=None, ui_html=ui_html, artifacts_dir=tmp_path)

    status, content_type, body = asyncio.run(_serve(app, "/mesh-ui"))

    assert status == 200
    assert content_type.startswith("text/html")
    html = body.decode()
    assert 'data-manifest-url="/mesh-ui/manifest.json"' in html
    assert 'data-fleet-url="/benzene/invoke"' in html


def test_ui_page_s_stamped_manifest_url_actually_resolves(tmp_path) -> None:
    """End-to-end proof, not just a string assertion: the URL stamped onto the page is the exact path
    this same app serves the artifact at."""
    ui_html = tmp_path / "mesh-ui.html"
    ui_html.write_text('<html lang="en"></html>')
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "manifest.json").write_text(json.dumps({"services": []}))
    app = MeshUiApp(inner=None, ui_html=ui_html, artifacts_dir=artifacts_dir)

    _, _, page_body = asyncio.run(_serve(app, "/mesh-ui"))
    manifest_url = page_body.decode().split('data-manifest-url="')[1].split('"')[0]

    status, content_type, body = asyncio.run(_serve(app, manifest_url))
    assert status == 200
    assert content_type == "application/json"
    assert json.loads(body) == {"services": []}
