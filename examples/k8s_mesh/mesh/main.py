"""Container entrypoint for the mesh service: two legs on one :class:`~benzene.core.WorkerHost` —
serve the mesh API/UI, and re-run discovery on an interval (the Kubernetes analogue of .NET's ``MeshAggregationBackgroundService``), so the catalog stays
fresh without anyone hitting ``POST /mesh/refresh``. A discovery/poll hiccup is swallowed and retried
next tick — never lets a transient API or interrogation error crash the mesh pod.

    MESH_NAMESPACE=benzene-mesh MESH_ARTIFACT_DIR=/artifacts PORT=8080 python -m k8s_mesh.mesh.main
"""

from __future__ import annotations

import asyncio
import contextlib
import os

from benzene.core import WorkerHost, background_worker
from benzene.http import uvicorn_worker

from .discovery_service import MeshDiscoveryService
from .host import build_mesh_host

_INTERVAL_SECONDS = 30.0


async def _run_discovery_loop(discovery_service: MeshDiscoveryService) -> None:
    while True:
        with contextlib.suppress(Exception):
            await discovery_service.run_once()
        await asyncio.sleep(_INTERVAL_SECONDS)


def build_worker_host() -> WorkerHost:
    """The mesh API/UI surface, plus the discovery loop that keeps the catalog fresh."""
    host = build_mesh_host()
    return (
        WorkerHost()
        .add(
            "http",
            uvicorn_worker(host.app, port=int(os.environ.get("PORT", "8080")), access_log=False),
        )
        .add("discovery", background_worker(lambda: _run_discovery_loop(host.discovery_service)))
    )


if __name__ == "__main__":  # pragma: no cover - the real container entry point
    asyncio.run(build_worker_host().run())
