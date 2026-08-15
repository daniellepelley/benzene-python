"""Container entrypoint for a domain service (orders/payments/shipping, selected by ``MESH_SERVICE``).

Runs uvicorn (the HTTP + envelope surface) and, when a collector is configured
(``MESH_COLLECTOR_ENVELOPE_URL``), the mesh reporter's background loop together on one event loop —
mirrors ``deploy/mesh/collector/main.py``'s ``poll_task`` pattern (a sibling ``asyncio`` task, cancelled
in ``finally`` once uvicorn's own signal handling ends ``serve()``).

    MESH_SERVICE=orders PORT=8080 \\
    DOWNSTREAM_MSG_URL=http://payments/benzene/invoke \\
    MESH_COLLECTOR_ENVELOPE_URL=http://mesh/benzene/invoke \\
      python -m k8s_mesh.service.main
"""

from __future__ import annotations

import asyncio
import contextlib
import os

from .host import build_service_host


async def main() -> None:
    import uvicorn  # a container-only dependency, imported lazily (matches deploy/mesh/collector)

    host = build_service_host()
    port = int(os.environ.get("PORT", "8080"))
    server = uvicorn.Server(
        uvicorn.Config(host.app, host="0.0.0.0", port=port, access_log=False)  # noqa: S104
    )
    reporter_task = asyncio.create_task(host.reporter.run_forever()) if host.reporter else None
    try:
        await server.serve()
    finally:
        if reporter_task is not None:
            reporter_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reporter_task


if __name__ == "__main__":
    asyncio.run(main())
