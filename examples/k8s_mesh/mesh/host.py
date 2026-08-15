"""The mesh service: discovers the ``benzene``-labelled Kubernetes Services, interrogates each over
in-cluster HTTP, and serves the Mesh UI + catalog artifacts. It also hosts the live
:class:`~benzene.mesh.MeshCollector` at ``/benzene/invoke`` — the same generic envelope endpoint every
domain service exposes — so the three services' register/heartbeat/trace pushes
(``MESH_COLLECTOR_ENVELOPE_URL``) and the on-demand ``POST /mesh/refresh`` discovery trigger both land
on one well-known surface. Mirrors .NET's ``examples/K8sMesh/Mesh/Startup.cs``.

Env vars (matching .NET's ``examples/K8sMesh/k8s/mesh.yaml``):

- ``MESH_NAMESPACE`` — the Kubernetes namespace to discover ``benzene``-labelled Services in.
- ``MESH_ARTIFACT_DIR`` — where to write the mesh-ui catalog artifacts (and serve them from); unset
  disables the Mesh UI (the collector + refresh endpoint keep working either way).
- ``PORT`` — the HTTP port (default ``8080``).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benzene.core import BenzeneMessageApplication, HealthChecks, ServiceSpec
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import MeshCollector, collector_registry
from benzene.mesh_fleet.discovery_adapters import KubernetesDiscovery
from benzene.results import Result

from .discovery_service import MeshDiscoveryService
from .static import MeshUiApp

MESH_REFRESH_TOPIC = "mesh:refresh"
_UI_HTML = Path(__file__).parent / "ui" / "mesh-ui.html"


@dataclass
class MeshHost:
    """The wired mesh host: the ASGI app to serve, the collector, and the discovery service."""

    app: Any
    collector: MeshCollector
    discovery_service: MeshDiscoveryService


def build_mesh_host(env: Mapping[str, str] | None = None, *, discovery: Any | None = None) -> MeshHost:
    """Wire the mesh host from env vars. ``discovery`` is injectable (a fake in tests); production
    derives a real :class:`~benzene.mesh_fleet.discovery_adapters.KubernetesDiscovery` from the env.
    """
    env = os.environ if env is None else env
    namespace = env.get("MESH_NAMESPACE", "default")
    artifact_dir = env.get("MESH_ARTIFACT_DIR") or None

    collector = MeshCollector()
    if discovery is None:
        discovery = KubernetesDiscovery(namespace=namespace, label_selector="benzene=true")
    discovery_service = MeshDiscoveryService(collector, discovery, artifact_dir)

    # The collector's ingest (register/heartbeat/traces/issues) + query (fleet/service/topic/trace)
    # topics — the same registry deploy/mesh/collector/host.py builds, here reached solely through the
    # generic /benzene/invoke surface rather than one REST route per feed (mirrors .NET's mesh, which
    # hosts its collector at exactly one path: POST /benzene/invoke).
    registry = collector_registry(collector)

    async def handle_refresh(_request: dict[str, Any]) -> Result:
        discovered = await discovery_service.run_once()
        return Result.created({"discovered": discovered})

    registry.register(MESH_REFRESH_TOPIC, handle_refresh)

    router = HttpRouter().register("POST", "/mesh/refresh", MESH_REFRESH_TOPIC, handle_refresh)
    standard = StandardPaths(
        health=HealthChecks().add("collector", lambda: True),
        spec=ServiceSpec.derive(registry, service="benzene-mesh"),
    )
    app: Any = BenzeneHttpApp(
        router, application=BenzeneMessageApplication(registry), standard_paths=standard
    )
    if artifact_dir:
        app = MeshUiApp(app, ui_html=_UI_HTML, artifacts_dir=Path(artifact_dir))

    return MeshHost(app=app, collector=collector, discovery_service=discovery_service)
