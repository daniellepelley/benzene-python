"""The **Mesh Host** — one service that is collector + aggregator + UI, over HTTP (mesh deployment).

A Benzene mesh needs one long-running process that:

1. **is the collector** — exposes the four ingest topics + the ``benzene:mesh:query:*`` read models over
   HTTP so real services *push* their feeds to it (:func:`collector_service_app`, served at
   ``/benzene/invoke``);
2. **aggregates** — on a timer (or a scheduled trigger) polls every registered service's ``/benzene/spec``
   + ``/benzene/health``, projects them together with the live collector into the six mesh-UI artifacts
   (:class:`~benzene.mesh.MeshAggregator`);
3. **serves the UI** — hosts the vendored ``mesh-ui.html`` and the freshly-emitted artifacts statically,
   same-origin, so the page's relative ``fetch("manifest.json")`` resolves against the host itself.

:class:`MeshHost` is the ASGI application that composes all three: ``/benzene/*`` is the networked
collector; every other GET serves a file from the artifact directory (``/`` → the mesh UI). Mirrors
.NET's ``deploy/Mesh/Benzene.Mesh.Host`` (``Startup`` wiring the aggregator + a static file provider +
the mesh UI, with a background poll loop) — collapsed onto Python's ASGI + the port's own
:class:`~benzene.mesh.MeshArtifactEmitter`.

The collector's catalog is **in-memory in this one process**: the host is a single collector process, so
the aggregator queries the very object the HTTP ingest feeds mutate (no shared store needed). Running
more than one host replica would need a shared collector store — out of scope here, and flagged as such.

Lives behind the ``benzene-mesh[host]`` extra (it needs ``benzene-http``); importing :mod:`benzene.mesh`
never pulls it in.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from benzene.core import BenzeneMessageApplication, HealthChecks, ServiceSpec
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths

from .aggregator import (
    MeshAggregator,
    MeshServiceRegistry,
    SpecHealthSource,
    run_poll_loop,
)
from .collector import MeshCollector, collector_registry

#: The service name the collector reports for itself (its ``/benzene/spec``).
COLLECTOR_SERVICE_NAME = "benzene-mesh-collector"

# Minimal content-type map for the static artifacts + the UI (all same-origin, so no CORS concerns).
_CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".json": "application/json",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


def collector_service_app(
    collector: MeshCollector, *, prefix: str = "/benzene"
) -> BenzeneHttpApp:
    """Wrap a :class:`~benzene.mesh.MeshCollector` as a real networked Benzene service.

    Registers the collector's ingest + query topics (:func:`~benzene.mesh.collector_registry`) behind
    the profile's ``/benzene/invoke`` surface, so any service reaches the collector by POSTing a wire
    envelope — the ingest feeds (``benzene:mesh:register`` / ``heartbeat`` / ``traces`` / ``issues``)
    and the ``benzene:mesh:query:*`` read models alike. Also exposes ``/benzene/health`` (the collector
    is always healthy) and ``/benzene/spec`` (its own derived spec), so the collector is itself a
    first-class, describable mesh citizen.
    """
    registry = collector_registry(collector)
    application = BenzeneMessageApplication(registry)
    spec = ServiceSpec.derive(registry, service=COLLECTOR_SERVICE_NAME)
    standard = StandardPaths(prefix=prefix, invoke=True, health=HealthChecks(), spec=spec)
    return BenzeneHttpApp(HttpRouter(), application=application, standard_paths=standard)


@dataclass
class MeshHostConfig:
    """The config shape of a Mesh Host — mirrors .NET's ``MeshHostConfig`` (``mesh.json``).

    ``registry`` is the set of services to poll each pass; ``out_dir`` is where the emitted artifacts are
    written *and* served from (bind-mount it for persistence across restarts); ``ui_html`` is the path to
    the vendored ``mesh-ui.html`` copied next to the artifacts at startup (reuse the single canonical
    copy — never fork the UI). ``poll_interval_seconds`` drives the background loop for the compose case;
    ``prefix`` is the well-known path prefix (R7). ``annotations`` seeds the human-discussion artifact;
    ``previous_hashes`` seeds contract-drift so it can show on the first pass.
    """

    registry: MeshServiceRegistry
    out_dir: str
    ui_html: str | None = None
    poll_interval_seconds: float = 60.0
    prefix: str = "/benzene"
    annotations: Sequence[Mapping[str, Any]] = ()
    previous_hashes: Mapping[str, str] = field(default_factory=dict)


class MeshHost:
    """The Mesh Host ASGI app: the networked collector + a static server for the emitted UI + artifacts.

    Construct it from a :class:`MeshHostConfig` (and, optionally, a pre-existing collector to co-host and
    a custom :class:`~benzene.mesh.SpecHealthSource` for tests). ``/benzene/*`` requests are served by the
    collector service; every other GET serves a file from ``out_dir`` (``/`` → ``mesh-ui.html``). Drive a
    pass with :meth:`run_once`, or run the background loop with :meth:`start_polling` / :meth:`stop_polling`.
    """

    def __init__(
        self,
        config: MeshHostConfig,
        *,
        collector: MeshCollector | None = None,
        source: SpecHealthSource | None = None,
        clock: Any = None,
    ) -> None:
        self._config = config
        self._out_dir = os.path.abspath(config.out_dir)
        self.collector = collector or MeshCollector()
        self._collector_app = collector_service_app(self.collector, prefix=config.prefix)
        self.aggregator = MeshAggregator(
            self.collector,
            source=source,
            clock=clock,
            previous_hashes=config.previous_hashes,
            annotations=config.annotations,
        )
        self._prefix = config.prefix
        self._stop: asyncio.Event | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._prepare_out_dir()

    # --- lifecycle -----------------------------------------------------------------------------
    def _prepare_out_dir(self) -> None:
        os.makedirs(os.path.join(self._out_dir, "services"), exist_ok=True)
        if self._config.ui_html is not None:
            shutil.copy(self._config.ui_html, os.path.join(self._out_dir, "mesh-ui.html"))

    async def run_once(self, *, generated_at: datetime | None = None) -> dict[str, Any]:
        """Run one aggregation pass, writing the artifacts into the served directory. Returns the manifest."""
        return await self.aggregator.run_once(
            self._config.registry, out_dir=self._out_dir, generated_at=generated_at
        )

    def start_polling(self) -> None:
        """Start the background poll loop on the running event loop (the compose seam)."""
        if self._poll_task is not None:
            return
        self._stop = asyncio.Event()
        self._poll_task = asyncio.create_task(
            run_poll_loop(
                self.aggregator,
                self._config.registry,
                out_dir=self._out_dir,
                interval_seconds=self._config.poll_interval_seconds,
                stop=self._stop,
            )
        )

    async def stop_polling(self) -> None:
        """Signal the background poll loop to stop and await its exit."""
        if self._stop is not None:
            self._stop.set()
        if self._poll_task is not None:
            await self._poll_task
            self._poll_task = None

    # --- ASGI ----------------------------------------------------------------------------------
    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            raise ValueError(f"MeshHost only handles 'http' scopes, got {scope.get('type')!r}")
        method = scope.get("method", "GET")
        path = scope.get("path", "/")

        if path == self._prefix or path.startswith(self._prefix + "/"):
            await self._serve_collector(scope, method, path, receive, send)
            return
        if method in ("GET", "HEAD"):
            await self._serve_static(path, method, send)
            return
        await self._send(send, 405, {"content-type": "text/plain; charset=utf-8"}, b"Method Not Allowed")

    async def _serve_collector(
        self, scope: dict, method: str, path: str, receive: Any, send: Any
    ) -> None:
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        body = await _read_body(receive)
        response = await self._collector_app.handle(
            method=method,
            path=path,
            query_string=scope.get("query_string", b"").decode("latin-1"),
            headers=headers,
            body=body,
        )
        await self._send(
            send,
            response.status_code,
            response.headers,
            response.body.encode("utf-8"),
            method=method,
        )

    async def _serve_static(self, path: str, method: str, send: Any) -> None:
        rel = path.lstrip("/") or "mesh-ui.html"
        full = os.path.normpath(os.path.join(self._out_dir, rel))
        # Refuse to escape the served directory (path-traversal guard).
        if not (full == self._out_dir or full.startswith(self._out_dir + os.sep)):
            await self._send(send, 404, {"content-type": "text/plain; charset=utf-8"}, b"Not Found")
            return
        if not os.path.isfile(full):
            await self._send(send, 404, {"content-type": "text/plain; charset=utf-8"}, b"Not Found")
            return
        with open(full, "rb") as handle:
            data = handle.read()
        ext = os.path.splitext(full)[1].lower()
        content_type = _CONTENT_TYPES.get(ext, "application/octet-stream")
        await self._send(send, 200, {"content-type": content_type}, data, method=method)

    async def _send(
        self,
        send: Any,
        status: int,
        headers: Mapping[str, str],
        body: bytes,
        *,
        method: str = "GET",
    ) -> None:
        out_headers = [
            [key.encode("latin-1"), value.encode("latin-1")] for key, value in headers.items()
        ]
        out_headers.append([b"content-length", str(len(body)).encode("latin-1")])
        await send({"type": "http.response.start", "status": status, "headers": out_headers})
        await send({"type": "http.response.body", "body": b"" if method == "HEAD" else body})


async def _read_body(receive: Any) -> str:
    chunks: list[bytes] = []
    more = True
    while more:
        message = await receive()
        if message.get("type") != "http.request":
            break
        chunks.append(message.get("body", b"") or b"")
        more = message.get("more_body", False)
    return b"".join(chunks).decode("utf-8")
