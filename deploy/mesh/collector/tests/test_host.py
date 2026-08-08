"""The Mesh Host wiring — the collector's feeds + queries served over HTTP, and config loading.

Exercises the deployable host in-memory (no container, no AWS): a POST to an ingest route lands in the
collector and a GET to a query route reads it back; a poll sweep folds a fake fleet in; and the fleet
config parses from inline JSON.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from benzene.mesh import (
    CallableServiceSource,
    JsonFileCollectorStore,
    MeshCollector,
    MeshPoller,
)

from collector.config import MeshConfig, load_config
from collector.host import build_mesh_host, run_poll_loop
from collector.main import _artifact_writer
from collector.static import StaticUiApp


async def _drive(app, path: str) -> tuple[int, bytes]:
    """Drive one GET through an ASGI app; return (status, body)."""
    sent: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message_: dict) -> None:
        sent.append(message_)

    scope = {"type": "http", "method": "GET", "path": path, "headers": [], "query_string": b""}
    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")["body"]
    return start["status"], body


def test_ingest_and_query_over_http() -> None:
    host = build_mesh_host(sources=[])

    # A service registers by POSTing its descriptor to the ingest route...
    register = asyncio.run(
        host.app.handle(
            "POST",
            "/mesh/register",
            body=json.dumps({"service": "orders", "topics": [{"id": "orders:place"}]}),
        )
    )
    assert register.status_code == 200

    # ...and it shows up in the fleet query.
    fleet = asyncio.run(host.app.handle("GET", "/mesh/fleet"))
    assert fleet.status_code == 200
    assert "orders" in {s["service"] for s in json.loads(fleet.body)["services"]}

    # A per-topic query reads the path parameter (topic ids contain ':').
    topic = asyncio.run(host.app.handle("GET", "/mesh/topic/orders:place"))
    assert json.loads(topic.body)["providers"] == ["orders"]


def test_health_surface_is_served_for_the_load_balancer() -> None:
    host = build_mesh_host(sources=[])
    health = asyncio.run(host.app.handle("GET", "/benzene/health"))
    assert health.status_code == 200
    assert json.loads(health.body)["isHealthy"] is True


def test_poll_loop_folds_a_source_then_stops() -> None:
    async def spec() -> dict:
        return {"service": "inventory", "topics": [{"id": "inventory:reserve"}]}

    async def health() -> dict:
        return {"isHealthy": True, "healthChecks": {}}

    collector = MeshCollector()
    poller = MeshPoller(collector, [CallableServiceSource("inventory", spec=spec, health=health)])

    async def drive() -> None:
        task = asyncio.create_task(run_poll_loop(poller, interval_seconds=0.01))
        for _ in range(100):  # let at least one sweep land
            if collector.query_fleet({})["services"]:
                break
            await asyncio.sleep(0.01)
        task.cancel()

    asyncio.run(drive())
    assert "inventory" in {s["service"] for s in collector.query_fleet({})["services"]}


def test_config_parses_inline_services() -> None:
    config = load_config(
        {
            "MESH_SERVICES": json.dumps(
                {
                    "pollIntervalSeconds": 15,
                    "services": [{"name": "orders", "baseUrl": "https://orders.svc"}],
                }
            )
        }
    )
    assert config.poll_interval_seconds == 15
    assert [s.name for s in config.sources] == ["orders"]
    assert config.store is None  # no MESH_STORE_PATH → the catalog stays in-memory


def test_config_builds_a_file_store_when_a_store_path_is_set(tmp_path: Path) -> None:
    config = load_config(
        {
            "MESH_SERVICES": json.dumps({"services": []}),
            "MESH_STORE_PATH": str(tmp_path / "state.json"),
        }
    )
    assert isinstance(config.store, JsonFileCollectorStore)
    assert config.store.path == tmp_path / "state.json"


def test_host_backed_by_a_store_survives_a_rebuild(tmp_path: Path) -> None:
    store = JsonFileCollectorStore(tmp_path / "state.json")

    # A service registers against a host whose collector persists through the store...
    first = build_mesh_host(sources=[], collector=MeshCollector(store=store))
    register = asyncio.run(
        first.app.handle(
            "POST",
            "/mesh/register",
            body=json.dumps({"service": "orders", "topics": [{"id": "orders:place"}]}),
        )
    )
    assert register.status_code == 200

    # ...and a fresh host (a redeployed task) rehydrates that service from the same store.
    second = build_mesh_host(sources=[], collector=MeshCollector(store=store))
    fleet = asyncio.run(second.app.handle("GET", "/mesh/fleet"))
    assert "orders" in {s["service"] for s in json.loads(fleet.body)["services"]}


# --- mesh-ui: static serving + artifact publishing ------------------------------------------


async def _inner_marker(scope, receive, send) -> None:  # noqa: ANN001 - ASGI stand-in
    await send({"type": "http.response.start", "status": 299, "headers": []})
    await send({"type": "http.response.body", "body": b"inner"})


def test_static_ui_serves_page_and_artifacts_and_delegates_the_rest(tmp_path: Path) -> None:
    ui = tmp_path / "mesh-ui.html"
    ui.write_text("<html>MESH UI</html>", encoding="utf-8")
    arts = tmp_path / "arts"
    arts.mkdir()
    (arts / "manifest.json").write_text('{"services": []}', encoding="utf-8")
    app = StaticUiApp(_inner_marker, ui_html=ui, artifacts_dir=arts)

    status, body = asyncio.run(_drive(app, "/mesh-ui/"))
    assert status == 200 and b"MESH UI" in body  # the vendored page

    status, body = asyncio.run(_drive(app, "/mesh-ui/manifest.json"))
    assert status == 200 and json.loads(body) == {"services": []}  # a generated artifact

    assert asyncio.run(_drive(app, "/mesh-ui/missing.json"))[0] == 404
    assert asyncio.run(_drive(app, "/mesh-ui/../secret.json"))[0] == 404  # no path traversal

    status, body = asyncio.run(_drive(app, "/mesh/fleet"))  # not under the mount -> inner app
    assert status == 299 and body == b"inner"


def test_artifact_writer_publishes_manifest_from_the_catalog(tmp_path: Path) -> None:
    host = build_mesh_host(sources=[])
    asyncio.run(
        host.app.handle(
            "POST",
            "/mesh/register",
            body=json.dumps({"service": "orders", "topics": [{"id": "order:create"}]}),
        )
    )
    config = MeshConfig(sources=[], poll_interval_seconds=1.0, artifacts_dir=str(tmp_path))
    _artifact_writer(host, config)()  # the after-sweep callback the poll loop runs

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert "orders" in {s["name"] for s in manifest["services"]}
    assert (tmp_path / "services" / "orders.json").exists()
