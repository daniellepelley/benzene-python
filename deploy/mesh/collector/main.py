"""Container entrypoint for the Mesh Host: serve the mesh API and poll the fleet on a timer.

Runs uvicorn with ``lifespan="off"`` and the poll loop as a sibling task in the same event loop, so
no ASGI-lifespan wrapper is needed — ``BenzeneHttpApp`` only ever sees ``http`` scopes.

When ``MESH_ARTIFACTS_DIR`` is set, the host also (re)publishes the mesh-ui artifacts after each poll
sweep and serves them — plus the vendored ``mesh-ui.html`` — under ``/mesh-ui/``.

    python -m collector.main        # honours MESH_CONFIG / MESH_SERVICES and PORT
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benzene.mesh import MeshCollector, write_artifacts

from .config import MeshConfig, load_config
from .host import MeshHost, build_mesh_host, run_poll_loop
from .static import StaticUiApp

_UI_HTML = Path(__file__).parent / "ui" / "mesh-ui.html"


def _artifact_writer(host: MeshHost, config: MeshConfig):
    """A callback that republishes the mesh-ui artifacts from the current catalog after each sweep."""
    directory = config.artifacts_dir

    def write() -> None:
        assert directory is not None
        write_artifacts(
            directory,
            host.collector,
            sources=config.sources,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

    return write


async def serve() -> None:
    import uvicorn  # a container-only dependency (see requirements.txt), imported lazily

    config = load_config()
    # With a store configured, the collector rehydrates the last snapshot on construction.
    collector = MeshCollector(store=config.store) if config.store else None
    host = build_mesh_host(config.sources, collector=collector)
    port = int(os.environ.get("PORT", "8080"))

    app: Any = host.app  # BenzeneHttpApp, optionally wrapped by the static UI server below
    after_sweep = None
    if config.artifacts_dir:
        after_sweep = _artifact_writer(host, config)
        after_sweep()  # publish once up front so the UI has data before the first interval elapses
        app = StaticUiApp(app, ui_html=_UI_HTML, artifacts_dir=Path(config.artifacts_dir))

    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=port, lifespan="off", access_log=False)  # noqa: S104
    )
    poll_task = asyncio.create_task(
        run_poll_loop(
            host.poller,
            interval_seconds=config.poll_interval_seconds,
            after_sweep=after_sweep,
        )
    )
    try:
        await server.serve()
    finally:
        poll_task.cancel()


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
