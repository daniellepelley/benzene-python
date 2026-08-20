"""The mesh poller — pull a fleet's /benzene/spec + /benzene/health into the collector.

Stands up two real in-memory Benzene HTTP services (each with `StandardPaths`), points the poller at
them through a fake GET that routes to the in-memory apps (no sockets), and asserts the collector's
fleet view reflects both — then that a down service degrades to a failed `PollResult` without breaking
the sweep.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urlsplit

import pytest
from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    Registry,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import (
    CallableServiceSource,
    HttpServiceSource,
    MeshCollector,
    MeshPoller,
    OutboundRegistry,
)
from benzene.results import Result

from ._async import run


@dataclass
class Ping:
    n: int = 0


def _service(
    name: str, topic: str, *, healthy: bool = True, produces: tuple[str, ...] = ()
) -> BenzeneHttpApp:
    async def handler(_request: Ping) -> Result:
        return Result.ok({"pong": True})

    router = HttpRouter().register("POST", f"/{name}", topic, handler, request_type=Ping)
    registry = Registry.from_definitions(router)
    outbound = OutboundRegistry()
    for produced in produces:
        outbound.register(produced, request_type=Ping)
    return BenzeneHttpApp(
        router,
        application=BenzeneMessageApplication(registry),
        standard_paths=StandardPaths(
            health=HealthChecks().add("core", lambda: healthy),
            spec=ServiceSpec.derive(registry, service=name, produces=outbound),
        ),
    )


def _fleet_fetch(apps: dict[str, BenzeneHttpApp]):
    """A fake HttpGet routing http://<host>/path to the in-memory app registered under <host>."""

    async def fetch(url: str) -> tuple[int, str]:
        parts = urlsplit(url)
        assert parts.hostname is not None
        app = apps[parts.hostname]
        response = await app.handle("GET", parts.path)
        return response.status_code, response.body

    return fetch


def test_poller_folds_a_fleet_into_the_collector() -> None:
    apps = {
        "orders": _service("orders", "orders:place"),
        "inventory": _service("inventory", "inventory:reserve"),
    }
    fetch = _fleet_fetch(apps)
    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("orders", "http://orders", fetch=fetch),
            HttpServiceSource("inventory", "http://inventory", fetch=fetch),
        ],
    )

    results = asyncio.run(poller.poll_once())
    assert all(r.ok for r in results)

    fleet = collector.query_fleet({})
    by_name = {s["service"]: s for s in fleet["services"]}
    assert set(by_name) == {"orders", "inventory"}
    assert by_name["orders"]["health"] == "healthy"
    assert by_name["orders"]["topics"] == 1  # one provided topic, from the spec
    assert "orders:place" in {t["topic"] for t in fleet["topics"]}


def test_a_pulled_spec_carries_declared_producers_into_the_graph() -> None:
    # The pull path's half of the declared graph (mesh.md §2, §2.3): a handler registration makes a
    # service that topic's CONSUMER, so without `produces` on the interrogated /benzene/spec document
    # every topic in a pull-based mesh shows consumers and no provider at all. The producing service
    # declares its outbound topics on its ServiceSpec; the poller carries them into the collector.
    apps = {
        "orders": _service("orders", "orders:place", produces=("inventory:reserve",)),
        "inventory": _service("inventory", "inventory:reserve"),
    }
    fetch = _fleet_fetch(apps)
    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("orders", "http://orders", fetch=fetch),
            HttpServiceSource("inventory", "http://inventory", fetch=fetch),
        ],
    )

    asyncio.run(poller.poll_once())

    reserve = collector.query_topic({"topic": "inventory:reserve"})
    assert reserve["providers"] == ["orders"]  # declared by orders' outbound registry
    assert reserve["consumers"] == ["inventory"]  # derived from inventory's handler registration
    # orders' own inbound topic has a consumer and (correctly) no declared provider in this fleet.
    place = collector.query_topic({"topic": "orders:place"})
    assert place["providers"] == []
    assert place["consumers"] == ["orders"]


def test_poller_reports_an_unhealthy_service() -> None:
    apps = {"orders": _service("orders", "orders:place", healthy=False)}
    collector = MeshCollector()
    poller = MeshPoller(
        collector, [HttpServiceSource("orders", "http://orders", fetch=_fleet_fetch(apps))]
    )

    asyncio.run(poller.poll_once())
    fleet = collector.query_fleet({})
    assert fleet["services"][0]["health"] == "unhealthy"  # 503 aggregate read as unhealthy


def test_a_down_service_is_a_failed_result_not_a_broken_sweep() -> None:
    apps = {"orders": _service("orders", "orders:place")}

    async def flaky_fetch(url: str) -> tuple[int, str]:
        if "down" in url:
            raise ConnectionError("connection refused")
        return await _fleet_fetch(apps)(url)

    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("orders", "http://orders", fetch=flaky_fetch),
            HttpServiceSource("down", "http://down", fetch=flaky_fetch),
        ],
    )

    results = {r.service: r for r in asyncio.run(poller.poll_once())}
    assert results["orders"].ok is True
    assert results["down"].ok is False and "refused" in (results["down"].error or "")
    # the healthy service still made it into the fleet despite its neighbour being down
    assert {s["service"] for s in collector.query_fleet({})["services"]} == {"orders"}


def test_poll_hash_is_stable_and_drifts_with_the_contract() -> None:
    calls = {"n": 0}

    async def spec() -> dict:
        calls["n"] += 1
        topics = [{"id": "t:one"}] if calls["n"] < 3 else [{"id": "t:one"}, {"id": "t:two"}]
        return {"service": "svc", "topics": topics}

    async def health() -> dict:
        return {"isHealthy": True, "healthChecks": {}}

    collector = MeshCollector()
    poller = MeshPoller(collector, [CallableServiceSource("svc", spec=spec, health=health)])

    asyncio.run(poller.poll_once())
    first = collector.query_service({"service": "svc"})
    asyncio.run(poller.poll_once())
    second = collector.query_service({"service": "svc"})
    # same contract two polls running -> the instance's hash still matches the service's
    assert second["instances"][0]["hashMatches"] is True
    asyncio.run(poller.poll_once())  # third poll: topics changed -> a new hash is registered
    assert first is not second  # (distinct query snapshots)


def test_a_malformed_service_response_is_a_failed_result_not_a_broken_sweep() -> None:
    # A service that answers 200 but with junk (non-JSON, or a JSON array instead of an object) must
    # be a failed PollResult, never an exception out of the sweep — the rest of the fleet still folds.
    async def non_json(url: str) -> tuple[int, str]:
        return 200, "this is not json"

    async def json_array(url: str) -> tuple[int, str]:
        return 200, "[1, 2, 3]"

    collector = MeshCollector()
    poller = MeshPoller(
        collector,
        [
            HttpServiceSource("garbage", "http://garbage", fetch=non_json),
            HttpServiceSource("wrongshape", "http://wrongshape", fetch=json_array),
        ],
    )
    results = asyncio.run(poller.poll_once())
    assert {r.service: r.ok for r in results} == {"garbage": False, "wrongshape": False}
    assert all(r.error for r in results)  # each carries a reason
    assert collector.query_fleet({})["services"] == []  # nothing malformed leaked into the catalog


def test_stdlib_get_returns_status_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    # The default (zero-dependency) GET reads status + body off a normal urllib response.
    from benzene.mesh import poller as poller_module

    class _FakeResponse:
        status = 200

        def read(self) -> bytes:
            return b'{"service": "orders"}'

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    monkeypatch.setattr(poller_module.urllib.request, "urlopen", lambda *a, **k: _FakeResponse())
    get = poller_module._stdlib_get()
    status, body = run(get("http://orders/benzene/spec"))
    assert status == 200
    assert body == '{"service": "orders"}'


def test_stdlib_get_reads_the_body_off_an_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 503 from the health surface still carries the aggregate body — the default GET returns it as
    # data (code, body) rather than letting the HTTPError propagate and fail the sweep.
    import io
    import urllib.error

    from benzene.mesh import poller as poller_module

    def _raise(*_a: object, **_k: object) -> None:
        raise urllib.error.HTTPError(
            url="http://orders/benzene/health",
            code=503,
            msg="Service Unavailable",
            hdrs=None,  # type: ignore[arg-type]
            fp=io.BytesIO(b'{"isHealthy": false}'),
        )

    monkeypatch.setattr(poller_module.urllib.request, "urlopen", _raise)
    get = poller_module._stdlib_get()
    status, body = run(get("http://orders/benzene/health"))
    assert status == 503
    assert body == '{"isHealthy": false}'


# --- pulling a service this port did not build ----------------------------------------------------

#: A .NET/Go/TypeScript service's /benzene/spec: the Contract Document (contract-document.md), the
#: format R5 names. Literal rather than produced here, so the test still fails if this port's own
#: emitter drifts away from the format.
_FOREIGN_CONTRACT_DOCUMENT = {
    "openapi": "3.0.1",
    "info": {"title": "payments", "description": "", "version": "2.1.0"},
    "messageEndpoint": "/benzene/invoke",
    "requests": [
        {
            "topic": "payments:capture",
            "request": {"$ref": "#/components/schemas/CapturePayment"},
            "response": {},
        },
        {"topic": "benzene:spec", "reserved": True, "request": {}, "response": {}},
    ],
    "events": [{"topic": "payment:captured", "message": {"type": "object"}}],
    "components": {
        "schemas": {
            "CapturePayment": {
                "type": "object",
                "properties": {"orderId": {"type": "string"}},
                "required": ["orderId"],
            }
        }
    },
}


def test_a_contract_document_service_folds_into_the_collector_like_any_other() -> None:
    # Reading only the native {service, topics} shape meant a polled foreign service landed in the
    # fleet as an empty catalogue: present, contributing no topics and no graph edges at all.
    collector = _polled(_FOREIGN_CONTRACT_DOCUMENT)

    fleet = collector.query_fleet({})
    assert [s["service"] for s in fleet["services"]] == ["payments"]  # info.title names the service
    # requests[] are what it consumes and events[] what it produces — the graph edges a pull-based
    # mesh exists to draw.
    capture = collector.query_topic({"topic": "payments:capture"})
    assert capture["consumers"] == ["payments"]
    captured = collector.query_topic({"topic": "payment:captured"})
    assert captured["providers"] == ["payments"]
    # The reserved framework topic stays out of the graph, as it does for a native document, so the
    # same service does not look different depending on which shape it happened to serve.
    assert "benzene:spec" not in {t["topic"] for t in fleet["topics"]}


def test_a_polled_contract_documents_refs_are_resolved_into_real_schemas() -> None:
    # A $ref into a catalogue the collector never sees would compare as "the schema changed" the
    # moment a producer renamed a class, so the reference is resolved on the way in.
    collector = _polled(_FOREIGN_CONTRACT_DOCUMENT)
    specs = collector.snapshot()["services"][0]["topicSpecs"]
    assert specs["payments:capture"]["requestSchema"] == {
        "type": "object",
        "properties": {"orderId": {"type": "string"}},
        "required": ["orderId"],
    }


def _polled(document: dict) -> MeshCollector:
    """One poll sweep of a single service answering ``document`` at /benzene/spec."""
    collector = MeshCollector()
    source = CallableServiceSource(
        "payments",
        spec=_returning(document),
        health=_returning({"isHealthy": True, "healthChecks": {}}),
    )
    assert all(result.ok for result in run(MeshPoller(collector, [source]).poll_once()))
    return collector


def _returning(document: dict):
    async def fetch() -> dict:
        return document

    return fetch
