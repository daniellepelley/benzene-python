"""Container entrypoint for the mesh service: serve the mesh API/UI and re-run discovery on an
interval (the Kubernetes analogue of .NET's ``MeshAggregationBackgroundService``), so the catalog stays
fresh without anyone hitting ``POST /mesh/refresh``. A discovery/poll hiccup is swallowed and retried
next tick — never lets a transient API or interrogation error crash the mesh pod.

    MESH_NAMESPACE=benzene-mesh MESH_ARTIFACT_DIR=/artifacts PORT=8080 python -m k8s_mesh.mesh.main
"""

from __future__ import annotations

import asyncio
import contextlib
import os

from .discovery_service import MeshDiscoveryService
from .host import build_mesh_host

_INTERVAL_SECONDS = 30.0


async def _run_discovery_loop(discovery_service: MeshDiscoveryService) -> None:
    while True:
        with contextlib.suppress(Exception):
            await discovery_service.run_once()
        await asyncio.sleep(_INTERVAL_SECONDS)


async def main() -> None:
    import uvicorn  # a container-only dependency, imported lazily (matches deploy/mesh/collector)

    host = build_mesh_host()
    port = int(os.environ.get("PORT", "8080"))
    server = uvicorn.Server(
        uvicorn.Config(host.app, host="0.0.0.0", port=port, access_log=False)  # noqa: S104
    )
    poll_task = asyncio.create_task(_run_discovery_loop(host.discovery_service))
    try:
        await server.serve()
    finally:
        poll_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poll_task


if __name__ == "__main__":
    asyncio.run(main())
