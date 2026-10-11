"""Serve the vendored mesh-ui and the generated artifacts under ``/benzene/mesh-ui/``.

A thin ASGI wrapper around the collector's :class:`~benzene.http.BenzeneHttpApp`: a request under the
``/benzene/mesh-ui/`` mount — the default path every port gives the Mesh UI (design-principles §5.2),
configurable with ``mount=`` — is served as a static file (the canonical ``mesh-ui.html`` from the image, or a
generated artifact JSON from the volume); everything else is delegated to the inner app unchanged, so
the collector's ingest/query routes and ``/benzene/*`` surfaces are untouched.

The UI fetches ``manifest.json`` (and ``services/{name}.json`` / ``topics.json`` / ``topology.json``)
by path relative to itself, so serving the page at ``/benzene/mesh-ui/`` with the artifacts alongside it
needs no UI configuration at all (mesh-ui.md §5).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

#: The Mesh UI's default path (design-principles §5.2): under ``/benzene/`` like every framework surface.
DEFAULT_MOUNT = "/benzene/mesh-ui"


class StaticUiApp:
    """Wrap ``inner`` and serve the mesh-ui page + artifacts under ``mount`` (``/benzene/mesh-ui/``)."""

    def __init__(
        self, inner: Any, *, ui_html: Path, artifacts_dir: Path, mount: str = DEFAULT_MOUNT
    ) -> None:
        self._inner = inner
        self._ui_html = Path(ui_html)
        self._artifacts_dir = Path(artifacts_dir).resolve()
        self._mount = mount.rstrip("/")

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = scope.get("path", "/")
            mount = self._mount
            if path in (mount, mount + "/", mount + "/index.html"):
                await self._send_file(send, self._ui_html, "text/html; charset=utf-8")
                return
            if path.startswith(mount + "/"):
                await self._serve_artifact(send, path[len(mount) + 1 :])
                return
        await self._inner(scope, receive, send)

    async def _serve_artifact(self, send: Any, relative: str) -> None:
        # Resolve within the artifacts dir and refuse anything that escapes it (path traversal) or is
        # not a generated JSON artifact — the mount serves data, never arbitrary files.
        target = (self._artifacts_dir / relative).resolve()
        if (
            self._artifacts_dir not in target.parents
            or target.suffix != ".json"
            or not target.is_file()
        ):
            await self._send(send, 404, "application/json", b'{"error":"not found"}')
            return
        await self._send_file(send, target, "application/json")

    async def _send_file(self, send: Any, path: Path, content_type: str) -> None:
        try:
            body = path.read_bytes()
        except OSError:
            await self._send(send, 404, "application/json", b'{"error":"not found"}')
            return
        await self._send(send, 200, content_type, body)

    async def _send(self, send: Any, status: int, content_type: str, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    [b"content-type", content_type.encode("latin-1")],
                    [b"content-length", str(len(body)).encode("latin-1")],
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
