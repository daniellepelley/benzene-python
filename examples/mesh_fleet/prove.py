"""Prove the canonical mesh UI renders a live Python fleet from the emitter's artifacts.

End to end: build the three-service fleet (:func:`mesh_fleet.fleet.build_fleet`), emit the six catalog
artifacts, drop the *canonical* ``mesh-ui.html`` next to them, serve the directory over HTTP (the UI's
``fetch()`` calls are relative, so ``file://`` would be blocked), then drive **headless Chromium via
Playwright** to load the page, wait for it to render, and assert against the live DOM that the estate
shows the three services with their health and the collector-derived ``orders → payments`` edge — the
render is the acceptance test. A screenshot is saved to ``mesh-fleet-proof.png`` at the repo root.

Run it directly::

    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers python -m mesh_fleet.prove

It exits non-zero if any assertion fails. The browser binary is auto-detected from
``PLAYWRIGHT_BROWSERS_PATH`` (no ``playwright install`` needed).
"""

from __future__ import annotations

import functools
import glob
import http.server
import os
import shutil
import socketserver
import threading
from datetime import datetime, timezone

from benzene.mesh import MeshArtifactEmitter

from .fleet import build_fleet

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_SCREENSHOT = os.path.join(_REPO_ROOT, "mesh-fleet-proof.png")


def emit_artifacts(out_dir: str) -> dict:
    """Build the fleet, emit the six artifacts into ``out_dir``, and vendor the mesh UI next to them."""
    generated_at = datetime.now(timezone.utc)
    fleet = build_fleet(generated_at=generated_at)
    annotations = [
        {
            "id": "a1",
            "entity": "service:payments",
            "author": "Dani (PO)",
            "text": "Contract drift here is the planned v2 capture payload — expected. The gateway "
            "health failure is the real thing to chase.",
            "createdAtUtc": generated_at.isoformat().replace("+00:00", "Z"),
        },
    ]
    emitter = MeshArtifactEmitter(
        fleet.services,
        fleet.collector,
        generated_at=fleet.generated_at,
        window_start=fleet.window_start,
        window_end=fleet.window_end,
        annotations=annotations,
    )
    manifest = emitter.emit(out_dir)
    shutil.copy(os.path.join(_HERE, "mesh-ui.html"), os.path.join(out_dir, "mesh-ui.html"))
    return manifest


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *_args):  # silence the per-request stderr logging
        pass


class _ThreadingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _serve(directory: str) -> tuple[socketserver.TCPServer, int]:
    handler = functools.partial(_QuietHandler, directory=directory)
    httpd = _ThreadingServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, port


def _chromium_executable() -> str | None:
    """The pre-installed Chromium binary under ``PLAYWRIGHT_BROWSERS_PATH`` (revision-agnostic)."""
    root = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers")
    for pattern in ("chromium-*/chrome-linux/chrome", "chromium_headless_shell-*/chrome-linux/headless_shell"):
        matches = sorted(glob.glob(os.path.join(root, pattern)))
        if matches:
            return matches[-1]
    return None


# The DOM facts the UI must show, extracted in one page.evaluate — kept as a JS string so the whole
# read is one round-trip and the assertions below are plain Python.
_EXTRACT_JS = """
() => {
  const cards = Array.from(document.querySelectorAll('article.svc'));
  const services = cards.map(c => {
    const badges = Array.from(c.querySelectorAll('.svc-head .badge')).map(b => b.className + '=' + b.textContent.trim());
    return { name: c.dataset.service, badges };
  });
  const rows = Array.from(document.querySelectorAll('#topology-table tbody tr'));
  const edges = rows.map(r => {
    const tds = r.querySelectorAll('td');
    const src = r.querySelector('.badge');
    return { client: tds[0] && tds[0].textContent.trim(), server: tds[1] && tds[1].textContent.trim(),
             source: src ? src.textContent.trim() : null };
  });
  // The catalog's topics table: column order is Topic, Version, Type, Producers, Consumers, HTTP, Status.
  // The HTTP cell (td[5], .t-http) shows the (method, path) mappings, or "–" when none are known.
  const topicRows = Array.from(document.querySelectorAll('#topics-tbody tr'));
  const topics = topicRows.map(r => {
    const tds = r.querySelectorAll('td');
    return { topic: tds[0] && tds[0].textContent.trim(),
             http: tds[5] ? tds[5].textContent.trim() : '',
             status: tds[6] ? tds[6].textContent.trim() : '' };
  }).filter(t => t.topic);
  return { services, edges, topics, docTitle: (document.getElementById('doc-title') || {}).textContent };
}
"""


def drive_and_assert(port: int, screenshot: str) -> dict:
    """Load the UI in headless Chromium, assert the live DOM, and screenshot it. Returns the DOM facts."""
    from playwright.sync_api import sync_playwright

    url = f"http://127.0.0.1:{port}/mesh-ui.html"
    executable = _chromium_executable()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=executable, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1280, "height": 1600})
        page.goto(url, wait_until="networkidle")
        page.wait_for_selector("article.svc", timeout=15000)
        # The topology table is populated after topology.json loads; the section is collapsed on the
        # estate overview, so wait for a row to be *attached* (present in the DOM), not visible.
        page.wait_for_selector("#topology-table tbody tr", state="attached", timeout=15000)
        facts = page.evaluate(_EXTRACT_JS)
        # Expand the (collapsed-by-default) Topology section so the derived edge graph is visible in
        # the screenshot — the assertions above already read it from the DOM regardless of visibility.
        toggle = page.query_selector('button[data-sec-toggle="topology-content"]')
        if toggle:
            toggle.click()
            page.wait_for_timeout(400)
        page.screenshot(path=screenshot, full_page=True)
        browser.close()

    _assert_facts(facts)
    return facts


def _assert_facts(facts: dict) -> None:
    names = {svc["name"] for svc in facts["services"]}
    assert names == {"orders", "payments", "shipping"}, f"expected the 3 Python services, got {names}"

    badges = {svc["name"]: " ".join(svc["badges"]) for svc in facts["services"]}
    assert "b-healthy=healthy" in badges["orders"], f"orders health badge missing: {badges['orders']}"
    assert "b-unhealthy=unhealthy" in badges["payments"], f"payments health badge missing: {badges['payments']}"
    assert "b-drift" in badges["payments"], f"payments drift badge missing: {badges['payments']}"
    assert "b-unreachable=unreachable" in badges["shipping"], f"shipping badge missing: {badges['shipping']}"

    edge_keys = {(e["client"], e["server"], e["source"]) for e in facts["edges"]}
    assert ("orders", "payments", "collector") in edge_keys, f"orders→payments collector edge missing: {facts['edges']}"


def main() -> None:
    work = os.path.join(_REPO_ROOT, ".mesh-fleet-artifacts")
    os.makedirs(work, exist_ok=True)
    emit_artifacts(work)
    httpd, port = _serve(work)
    try:
        facts = drive_and_assert(port, _SCREENSHOT)
    finally:
        httpd.shutdown()

    print("Mesh UI rendered the live Python fleet:")
    for svc in facts["services"]:
        print(f"  - {svc['name']}: {', '.join(svc['badges'])}")
    print("Edges (collector-derived):")
    for edge in facts["edges"]:
        print(f"  - {edge['client']} -> {edge['server']} [{edge['source']}]")
    print(f"Screenshot: {_SCREENSHOT}")


if __name__ == "__main__":
    main()
