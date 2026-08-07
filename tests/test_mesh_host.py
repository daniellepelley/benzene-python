"""The Mesh Host: the networked collector round-trip (ingest → query over the wire envelope) + UI serving.

These exercise the host as a real ASGI service — every call goes through :meth:`MeshHost.__call__` (a
built scope/receive/send), so the collector is reached exactly the way a remote service reaches it: by
POSTing a wire envelope to ``/benzene/invoke``. The static half (``/`` → the mesh UI, ``/manifest.json``
→ the freshly-emitted artifact) is asserted the same way. No pytest-asyncio: each test drives an async
scenario through ``asyncio.run`` (the repo's convention).
"""

from __future__ import annotations

import asyncio
import json
import os

from benzene.core import Registry
from benzene.http import HttpReply, InvokeMessageSender
from benzene.mesh import Heartbeat, MeshFeedSender, ServiceDescriptor, ServiceInfo, TraceEvent
from benzene.mesh.aggregator import MeshServiceRegistry
from benzene.mesh.host import MeshHost, MeshHostConfig
from benzene.results import Result

_UI_HTML = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "mesh_fleet",
    "mesh-ui.html",
)


async def asgi_request(
    app: MeshHost,
    method: str,
    path: str,
    *,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Drive one HTTP request through an ASGI app, collecting the response (status, headers, body)."""
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": [[k.encode(), v.encode()] for k, v in (headers or {}).items()],
    }
    received = {"done": False}

    async def receive() -> dict:
        if received["done"]:
            return {"type": "http.disconnect"}
        received["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body_out = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    resp_headers = {k.decode(): v.decode() for k, v in start["headers"]}
    return start["status"], resp_headers, body_out


def _host(tmp_path) -> MeshHost:
    config = MeshHostConfig(
        registry=MeshServiceRegistry(),
        out_dir=str(tmp_path),
        ui_html=_UI_HTML,
        poll_interval_seconds=0.05,
    )
    return MeshHost(config)


def _sender_over(app: MeshHost) -> InvokeMessageSender:
    """An InvokeMessageSender whose transport POSTs the envelope through the host's ASGI surface."""

    async def transport(url: str, headers: dict[str, str], body: str) -> HttpReply:
        status, _hdrs, out = await asgi_request(
            app, "POST", "/benzene/invoke", body=body.encode(), headers=headers
        )
        return HttpReply(status, out.decode())

    return InvokeMessageSender("http://host/benzene/invoke", transport=transport)


def _orders_descriptor() -> ServiceDescriptor:
    registry = Registry()

    async def create(_request: dict) -> Result:
        return Result.ok({})

    registry.register("orders:create", create)
    return ServiceDescriptor.derive(registry, ServiceInfo("orders", instance_id="orders-1"))


def test_feed_pushed_over_the_envelope_is_ingested_and_queryable(tmp_path) -> None:
    async def scenario() -> None:
        host = _host(tmp_path)
        feeds = MeshFeedSender(_sender_over(host))

        register = await feeds.register(_orders_descriptor())
        beat = await feeds.publish_heartbeat(
            Heartbeat(
                service="orders", sent_at="2026-08-07T00:00:00Z", instance_id="orders-1", is_healthy=True
            )
        )
        traces = await feeds.publish_traces(
            [
                TraceEvent(
                    trace_id="a" * 32, span_id="b" * 16, service="orders",
                    topic="orders:create", status="ok",
                )
            ]
        )
        assert register.status == "ok" and register.payload == {"accepted": 1}
        assert beat.status == "ok"
        assert traces.status == "ok" and traces.payload == {"accepted": 1}

        # Query the collector back over the same envelope surface — proof the ingest actually landed.
        sender = _sender_over(host)
        fleet = await sender.send_message("benzene:mesh:query:fleet", {})
        assert fleet.status == "ok"
        assert [s["service"] for s in fleet.payload["services"]] == ["orders"]
        assert fleet.payload["services"][0]["health"] == "healthy"

        service = await sender.send_message("benzene:mesh:query:service", {"service": "orders"})
        assert service.status == "ok"
        assert service.payload["invocations"] == 1

    asyncio.run(scenario())


def test_query_errors_round_trip_as_wire_statuses(tmp_path) -> None:
    async def scenario() -> None:
        sender = _sender_over(_host(tmp_path))
        missing = await sender.send_message("benzene:mesh:query:service", {"service": "ghost"})
        assert missing.status == "not-found"
        bad = await sender.send_message("benzene:mesh:register", {})
        assert bad.status == "bad-request"

    asyncio.run(scenario())


def test_host_serves_the_ui_at_root_and_emitted_artifacts(tmp_path) -> None:
    async def scenario() -> None:
        host = _host(tmp_path)
        # No services registered → an empty-but-valid fleet; run_once still emits every artifact.
        manifest = await host.run_once()
        assert manifest["services"] == []

        status, headers, body = await asgi_request(host, "GET", "/")
        assert status == 200 and headers["content-type"].startswith("text/html")
        assert b"mesh" in body.lower()

        status, headers, body = await asgi_request(host, "GET", "/manifest.json")
        assert status == 200 and headers["content-type"] == "application/json"
        assert json.loads(body)["services"] == []

        status, _headers, _body = await asgi_request(host, "GET", "/does-not-exist.json")
        assert status == 404

    asyncio.run(scenario())


def test_static_serving_refuses_path_traversal(tmp_path) -> None:
    async def scenario() -> None:
        host = _host(tmp_path)
        status, _headers, _body = await asgi_request(host, "GET", "/../../etc/passwd")
        assert status == 404

    asyncio.run(scenario())


def test_collector_health_and_spec_surfaces(tmp_path) -> None:
    async def scenario() -> None:
        host = _host(tmp_path)
        status, _headers, body = await asgi_request(host, "GET", "/benzene/health")
        assert status == 200 and json.loads(body)["isHealthy"] is True

        status, _headers, body = await asgi_request(host, "GET", "/benzene/spec")
        assert status == 200
        topic_ids = {t["id"] for t in json.loads(body)["topics"]}
        assert "benzene:mesh:register" in topic_ids and "benzene:mesh:query:fleet" in topic_ids

    asyncio.run(scenario())
