"""Serve the vendored mesh-ui page and the generated artifacts under ``/mesh-ui/``.

A thin ASGI wrapper around the mesh's :class:`~benzene.http.BenzeneHttpApp`: a request under the
``/mesh-ui/`` mount is served as a static file (the canonical ``mesh-ui.html``, or a generated artifact
JSON from ``MESH_ARTIFACT_DIR``); everything else is delegated to the inner app unchanged, so the
``/benzene/invoke`` collector surface and ``/mesh/refresh`` stay untouched. Mirrors
``deploy/mesh/collector/static.py``'s ``StaticUiApp``, kept local to this example (it wraps a plain
ASGI callable, not anything specific to the Fargate collector's own wiring).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

_MOUNT = "/mesh-ui"


class MeshUiApp:
    """Wrap ``inner`` and serve the mesh-ui page + artifacts under ``/mesh-ui/``."""

    def __init__(self, inner: Any, *, ui_html: Path, artifacts_dir: Path) -> None:
        self._inner = inner
        self._ui_html = Path(ui_html)
        self._artifacts_dir = Path(artifacts_dir).resolve()

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = scope.get("path", "/")
            if path in (_MOUNT, _MOUNT + "/", _MOUNT + "/index.html"):
                await self._send_file(send, self._ui_html, "text/html; charset=utf-8")
                return
            if path.startswith(_MOUNT + "/"):
                await self._serve_artifact(send, path[len(_MOUNT) + 1 :])
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
