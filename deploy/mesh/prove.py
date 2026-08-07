"""Prove the **host-served** mesh UI renders the live, multi-process Python fleet — with a real browser.

The distinguishing claim over ``mesh_fleet.prove`` (which serves throwaway artifacts from a stdlib
``http.server``): here the fleet data reaches the collector via **real HTTP feed pushes between separate
ASGI processes**, and the UI is served by **the Mesh Host itself**, not a throwaway server. This script
brings the whole distributed stack up on real localhost sockets (:func:`deploy.mesh.stack.run_stack`),
then drives **headless Chromium via Playwright** against the *host's* UI URL, asserts the live DOM shows
the three services with health / drift / unreachable and the collector-derived ``orders → payments``
edge, and saves a screenshot to ``mesh-host-proof.png`` at the repo root.

The stack runs on a background asyncio event loop (its four servers must keep answering Chromium's
requests while the synchronous Playwright driver blocks the main thread); the browser talks to them over
genuine HTTP the entire time.

Run it directly::

    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers \\
    PYTHONPATH=packages/benzene-results:packages/benzene-core:packages/benzene-http:packages/benzene-mesh:examples:deploy \\
      python -m mesh.prove

It exits non-zero if any assertion fails. The browser binary is auto-detected from
``PLAYWRIGHT_BROWSERS_PATH`` (no ``playwright install`` needed).
"""

from __future__ import annotations

import asyncio
import os
import threading
from datetime import datetime, timezone

# Reuse the canonical DOM extraction + assertions from the in-process proof — the UI is identical, only
# the origin serving it (the Mesh Host) and how its data arrived (HTTP feeds) differ.
from mesh_fleet.prove import _EXTRACT_JS, _assert_facts, _chromium_executable

from mesh.stack import Stack, run_stack

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_UI_HTML = os.path.join(_REPO_ROOT, "examples", "mesh_fleet", "mesh-ui.html")
_SCREENSHOT = os.path.join(_REPO_ROOT, "mesh-host-proof.png")


def _start_stack_in_background() -> tuple[Stack, asyncio.AbstractEventLoop, threading.Thread]:
    """Bring the distributed stack up on a background event loop so its servers stay live for the browser."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    at = datetime.now(timezone.utc).replace(microsecond=0)
    future = asyncio.run_coroutine_threadsafe(
        run_stack(ui_html=_UI_HTML, generated_at=at, creates=12, lists=5), loop
    )
    stack = future.result(timeout=60)
    return stack, loop, thread


def _stop_stack(stack: Stack, loop: asyncio.AbstractEventLoop, thread: threading.Thread) -> None:
    asyncio.run_coroutine_threadsafe(stack.close(), loop).result(timeout=30)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=10)
    loop.close()


def drive_and_assert(ui_url: str, screenshot: str) -> dict:
    """Load the host-served UI in headless Chromium, assert the live DOM, and screenshot it."""
    from playwright.sync_api import sync_playwright

    executable = _chromium_executable()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=executable, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1280, "height": 1600})
        page.goto(ui_url, wait_until="networkidle")
        page.wait_for_selector("article.svc", timeout=15000)
        page.wait_for_selector("#topology-table tbody tr", state="attached", timeout=15000)
        facts = page.evaluate(_EXTRACT_JS)
        toggle = page.query_selector('button[data-sec-toggle="topology-content"]')
        if toggle:
            toggle.click()
            page.wait_for_timeout(400)
        page.screenshot(path=screenshot, full_page=True)
        browser.close()

    _assert_facts(facts)
    return facts


def main() -> None:
    stack, loop, thread = _start_stack_in_background()
    print(f"Distributed mesh up. Host UI served at {stack.ui_url}")
    try:
        facts = drive_and_assert(stack.ui_url, _SCREENSHOT)
    finally:
        _stop_stack(stack, loop, thread)

    print("Host-served mesh UI rendered the live multi-process Python fleet:")
    for svc in facts["services"]:
        print(f"  - {svc['name']}: {', '.join(svc['badges'])}")
    print("Edges (collector-derived from cross-process trace parentage):")
    for edge in facts["edges"]:
        print(f"  - {edge['client']} -> {edge['server']} [{edge['source']}]")
    print(f"Screenshot: {_SCREENSHOT}")


if __name__ == "__main__":
    main()
