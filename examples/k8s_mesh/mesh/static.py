"""Serve the vendored mesh-ui page and the generated artifacts under ``/mesh-ui/``.

A thin ASGI wrapper around the mesh's :class:`~benzene.http.BenzeneHttpApp`: a request under the
``/mesh-ui/`` mount is served as a static file (the canonical ``mesh-ui.html``, or a generated artifact
JSON from ``MESH_ARTIFACT_DIR``); everything else is delegated to the inner app unchanged, so the
``/benzene/invoke`` collector surface and ``/mesh/refresh`` stay untouched. Mirrors
``deploy/mesh/collector/static.py``'s ``StaticUiApp``, kept local to this example (it wraps a plain
ASGI callable, not anything specific to the Fargate collector's own wiring).

The served page has ``data-manifest-url``/``data-fleet-url`` stamped onto its ``<html>`` root, pointing
at this mount's own absolute artifact/collector paths (``/mesh-ui/manifest.json``, ``/benzene/invoke``)
— mirrors .NET's ``MeshUiPage.GetHtml(manifestUrl, envelopeUrl)`` / ``UseMeshUi("/mesh-ui",
"manifest.json", "/benzene/invoke")``. .NET's own default of the *bare* filename ``"manifest.json"``
only resolves correctly there because its artifacts are served at the site **root**, not nested under
``/mesh-ui/`` as they are here; nesting them (deliberately, so they can't collide with this app's own
``/benzene/*``/``/mesh/*`` routes) means the page's default relative fetch — resolved against
``document.baseURI``, i.e. the page's own URL, ``/mesh-ui`` with no trailing slash — lands on
``/manifest.json`` instead of ``/mesh-ui/manifest.json`` and 404s. Stamping the absolute path sidesteps
that entirely, regardless of whether the page was reached with or without a trailing slash.
"""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any

_MOUNT = "/mesh-ui"
_COLLECTOR_ENVELOPE_PATH = "/benzene/invoke"


def _inject_urls(html: str, manifest_url: str, fleet_url: str) -> str:
    attrs = f' data-manifest-url="{escape(manifest_url)}" data-fleet-url="{escape(fleet_url)}"'
    return html.replace('<html lang="en">', f'<html lang="en"{attrs}>', 1)


class MeshUiApp:
    """Wrap ``inner`` and serve the mesh-ui page + artifacts under ``/mesh-ui/``."""

    def __init__(self, inner: Any, *, ui_html: Path, artifacts_dir: Path) -> None:
        self._inner = inner
        self._artifacts_dir = Path(artifacts_dir).resolve()
        self._html = _inject_urls(
            Path(ui_html).read_text(encoding="utf-8"),
            manifest_url=f"{_MOUNT}/manifest.json",
            fleet_url=_COLLECTOR_ENVELOPE_PATH,
        ).encode("utf-8")

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            path = scope.get("path", "/")
            if path in (_MOUNT, _MOUNT + "/", _MOUNT + "/index.html"):
                await self._send(send, 200, "text/html; charset=utf-8", self._html)
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
