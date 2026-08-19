"""Container entrypoint for a domain service (orders/payments/shipping, selected by ``MESH_SERVICE``).

Two legs on one :class:`~benzene.core.WorkerHost`: uvicorn (the HTTP + envelope surface) and, when a
collector is configured (``MESH_COLLECTOR_ENVELOPE_URL``), the mesh reporter's background loop.
Whichever stops first winds the other down — see ``benzene.core.worker`` for the hand-rolled
``create_task``/``finally: cancel()`` this is shorthand for.

    MESH_SERVICE=orders PORT=8080 \\
    DOWNSTREAM_MSG_URL=http://payments/benzene/invoke \\
    MESH_COLLECTOR_ENVELOPE_URL=http://mesh/benzene/invoke \\
      python -m k8s_mesh.service.main
"""

from __future__ import annotations

import asyncio
import os

from benzene.core import WorkerHost, background_worker
from benzene.http import uvicorn_worker

from .host import build_service_host


def build_worker_host() -> WorkerHost:
    """The service's HTTP surface, plus the mesh reporter when one is configured."""
    host = build_service_host()
    workers = WorkerHost().add(
        "http",
        uvicorn_worker(host.app, port=int(os.environ.get("PORT", "8080")), access_log=False),
    )
    if host.reporter:
        workers.add("mesh-reporter", background_worker(host.reporter.run_forever))
    return workers


if __name__ == "__main__":  # pragma: no cover - the real container entry point
    asyncio.run(build_worker_host().run())
