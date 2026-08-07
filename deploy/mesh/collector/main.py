"""Container entrypoint for the Mesh Host: serve the mesh API and poll the fleet on a timer.

Runs uvicorn with ``lifespan="off"`` and the poll loop as a sibling task in the same event loop, so
no ASGI-lifespan wrapper is needed — ``BenzeneHttpApp`` only ever sees ``http`` scopes.

    python -m collector.main        # honours MESH_CONFIG / MESH_SERVICES and PORT
"""

from __future__ import annotations

import asyncio
import os

from benzene.mesh import MeshCollector

from .config import load_config
from .host import build_mesh_host, run_poll_loop


async def serve() -> None:
    import uvicorn  # a container-only dependency (see requirements.txt), imported lazily

    config = load_config()
    # With a store configured, the collector rehydrates the last snapshot on construction.
    collector = MeshCollector(store=config.store) if config.store else None
    host = build_mesh_host(config.sources, collector=collector)
    port = int(os.environ.get("PORT", "8080"))

    server = uvicorn.Server(
        uvicorn.Config(host.app, host="0.0.0.0", port=port, lifespan="off", access_log=False)  # noqa: S104
    )
    poll_task = asyncio.create_task(
        run_poll_loop(host.poller, interval_seconds=config.poll_interval_seconds)
    )
    try:
        await server.serve()
    finally:
        poll_task.cancel()


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
