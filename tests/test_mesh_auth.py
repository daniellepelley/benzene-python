"""The optional shared-secret guarding the collector's ingest feeds (the simple feed-auth option).

Three levels are covered, all through ``asyncio.run`` (the repo's no-pytest-asyncio convention):

1. **the sender** — :class:`~benzene.mesh.MeshFeedSender` attaches :data:`~benzene.mesh.MESH_KEY_HEADER`
   to every feed when a key is set, and attaches nothing when it is not (today's open behaviour);
2. **the collector service** — :func:`~benzene.mesh.host.collector_service_app` with a key rejects an
   ingest feed whose envelope carries the wrong / no key with ``unauthorized`` (HTTP 200 with the domain
   status inside the envelope, the ``/benzene/invoke`` contract), accepts the matching key, and leaves the
   ``benzene:mesh:query:*`` read models open; with **no** key it accepts an unauthenticated feed;
3. **the host, end to end** — a real :class:`~benzene.mesh.host.MeshHost` driven over its ASGI surface:
   a matching-key ``register`` lands in the collector (the fleet query then lists the service), a
   wrong-key ``register`` does not, and a keyless host ingests a keyless feed.

Deeper auth (IAM SigV4 / mTLS / API Gateway authorizers) is explicitly out of scope here — this pins the
simple shared-secret option only.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from benzene.mesh import (
    MESH_KEY_HEADER,
    Heartbeat,
    MeshCollector,
    MeshFeedSender,
    ServiceDescriptor,
    ServiceInfo,
)
from benzene.mesh.collector import REGISTER_TOPIC
from benzene.mesh.host import MeshHost, MeshHostConfig, collector_service_app
from benzene.mesh.registry import MeshServiceRegistry
from benzene.results import Result, Status


class _CapturingSender:
    """A fake :class:`~benzene.core.MessageSender` recording every ``send_message`` call's headers."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any, dict[str, str] | None]] = []

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        self.calls.append((topic, message, headers))
        return Result.ok({"accepted": 1})


def _descriptor() -> ServiceDescriptor:
    from benzene.core import Registry

    return ServiceDescriptor.derive(Registry(), ServiceInfo("orders", instance_id="orders-1"))


def _envelope(topic: str, body: dict[str, Any], *, key: str | None = None) -> str:
    headers = {MESH_KEY_HEADER: key} if key is not None else {}
    return json.dumps({"topic": topic, "headers": headers, "body": json.dumps(body)})


# --- 1. the sender ------------------------------------------------------------------------------
def test_feed_sender_attaches_key_when_set() -> None:
    sender = _CapturingSender()
    feeds = MeshFeedSender(sender, key="s3cret")

    asyncio.run(feeds.register(_descriptor()))
    asyncio.run(
        feeds.publish_heartbeat(Heartbeat(service="orders", sent_at="2026-01-01T00:00:00Z"))
    )

    assert [c[0] for c in sender.calls] == [REGISTER_TOPIC, "benzene:mesh:heartbeat"]
    for _topic, _message, headers in sender.calls:
        assert headers == {MESH_KEY_HEADER: "s3cret"}


def test_feed_sender_attaches_nothing_without_a_key() -> None:
    sender = _CapturingSender()
    feeds = MeshFeedSender(sender)  # no key → today's open behaviour

    asyncio.run(feeds.register(_descriptor()))

    assert sender.calls[0][2] is None


# --- 2. the collector service -------------------------------------------------------------------
def _invoke(app: Any, topic: str, body: dict[str, Any], *, key: str | None = None) -> dict[str, Any]:
    response = asyncio.run(
        app.handle("POST", "/benzene/invoke", "", {}, _envelope(topic, body, key=key))
    )
    assert response.status_code == 200  # /benzene/invoke always answers 200 for a processed message
    return json.loads(response.body)


def test_collector_with_key_rejects_missing_and_wrong_key() -> None:
    app = collector_service_app(MeshCollector(), key="s3cret")

    missing = _invoke(app, REGISTER_TOPIC, {"service": "orders"})
    assert missing["statusCode"] == Status.UNAUTHORIZED

    wrong = _invoke(app, REGISTER_TOPIC, {"service": "orders"}, key="nope")
    assert wrong["statusCode"] == Status.UNAUTHORIZED


def test_collector_with_key_accepts_matching_key() -> None:
    collector = MeshCollector()
    app = collector_service_app(collector, key="s3cret")

    accepted = _invoke(app, REGISTER_TOPIC, {"service": "orders"}, key="s3cret")
    assert accepted["statusCode"] == Status.OK

    fleet = _invoke(app, "benzene:mesh:query:fleet", {}, key="s3cret")
    assert {s["service"] for s in json.loads(fleet["body"])["services"]} == {"orders"}


def test_collector_query_read_models_stay_open() -> None:
    # The read models expose no write path; a dashboard polls them same-origin without the key.
    app = collector_service_app(MeshCollector(), key="s3cret")
    fleet = _invoke(app, "benzene:mesh:query:fleet", {})  # no key attached
    assert fleet["statusCode"] == Status.OK


def test_collector_without_key_is_open() -> None:
    app = collector_service_app(MeshCollector())  # default: no key configured
    accepted = _invoke(app, REGISTER_TOPIC, {"service": "orders"})  # keyless feed
    assert accepted["statusCode"] == Status.OK


# --- 3. the host, end to end ----------------------------------------------------------------------
async def _asgi_invoke(app: MeshHost, envelope: str) -> dict[str, Any]:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/benzene/invoke",
        "query_string": b"",
        "headers": [],
    }
    received = {"done": False}

    async def receive() -> dict:
        if received["done"]:
            return {"type": "http.disconnect"}
        received["done"] = True
        return {"type": "http.request", "body": envelope.encode("utf-8"), "more_body": False}

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    await app(scope, receive, send)
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return json.loads(body.decode("utf-8"))


def _host(tmp_path: Any, *, key: str | None) -> MeshHost:
    return MeshHost(
        MeshHostConfig(registry=MeshServiceRegistry(()), out_dir=str(tmp_path), mesh_key=key)
    )


def test_host_ingests_only_with_the_matching_key(tmp_path: Any) -> None:
    host = _host(tmp_path, key="s3cret")

    rejected = asyncio.run(_asgi_invoke(host, _envelope(REGISTER_TOPIC, {"service": "orders"})))
    assert rejected["statusCode"] == Status.UNAUTHORIZED
    assert host.collector.query_fleet({})["services"] == []  # nothing ingested

    accepted = asyncio.run(
        _asgi_invoke(host, _envelope(REGISTER_TOPIC, {"service": "orders"}, key="s3cret"))
    )
    assert accepted["statusCode"] == Status.OK
    assert {s["service"] for s in host.collector.query_fleet({})["services"]} == {"orders"}


def test_keyless_host_ingests_a_keyless_feed(tmp_path: Any) -> None:
    host = _host(tmp_path, key=None)
    accepted = asyncio.run(_asgi_invoke(host, _envelope(REGISTER_TOPIC, {"service": "orders"})))
    assert accepted["statusCode"] == Status.OK
    assert {s["service"] for s in host.collector.query_fleet({})["services"]} == {"orders"}
