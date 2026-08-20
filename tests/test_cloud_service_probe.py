"""The Cloud Service Profile live-probe checker — audit a service over HTTP, tri-state per R1–R8.

Routes the probe's HTTP calls at a real in-memory ``BenzeneHttpApp`` (no sockets): a fully-provisioned
service audits clean with R6/R7/R8 the by-design ``inconclusive`` trio, a bare service is positively
found missing its surfaces, and an unreachable host degrades to inconclusive rather than a false
failure.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from urllib.parse import urlsplit

import pytest
from benzene.core import (
    BenzeneMessageApplication,
    HealthChecks,
    MiddlewarePipeline,
    Registry,
    ServiceSpec,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.mesh import (
    ServiceDescriptor,
    ServiceInfo,
    Verdict,
    mesh_interception,
    probe_cloud_service,
)
from benzene.results import Result


@dataclass
class Order:
    sku: str = ""


async def _place(_request: Order) -> Result:
    return Result.created({})


def _client(app: BenzeneHttpApp):
    """An HttpCall that routes the probe's requests to an in-memory app (by path, no socket)."""

    async def call(method: str, url: str, body: str | None) -> tuple[int, str]:
        response = await app.handle(method, urlsplit(url).path, "", {}, body or "")
        return response.status_code, response.body

    return call


def _profiled_app() -> BenzeneHttpApp:
    """A fully-provisioned service: an app topic, mesh descriptor, and the /benzene/* surfaces."""
    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    registry = Registry.from_definitions(router)
    descriptor = ServiceDescriptor.derive(registry, ServiceInfo(service="orders"))
    application = BenzeneMessageApplication(
        registry, MiddlewarePipeline([mesh_interception(descriptor)])
    )
    return BenzeneHttpApp(
        router,
        application=application,
        standard_paths=StandardPaths(
            health=HealthChecks().add("core", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )


def test_a_fully_provisioned_service_audits_clean() -> None:
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(_profiled_app())))
    assert report.is_clean
    assert report.not_satisfied == []
    # The observable requirements are satisfied outright...
    for satisfied in ("R1", "R2", "R3", "R4", "R5", "R7"):
        assert report.verdict(satisfied) is Verdict.SATISFIED, satisfied
    # ...and the structurally-unobservable trio stays inconclusive by design.
    assert report.inconclusive == ["R6", "R8"]


def test_a_bare_service_is_found_missing_its_surfaces() -> None:
    # Only an application route — no standard_paths, no mesh interception.
    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    app = BenzeneHttpApp(router)
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(app)))

    assert report.verdict("R3") is Verdict.NOT_SATISFIED  # /benzene/health 404s
    assert report.verdict("R5") is Verdict.NOT_SATISFIED  # /benzene/spec 404s
    assert report.verdict("R4") is Verdict.NOT_SATISFIED  # /benzene/invoke 404s
    assert "R3" in report.not_satisfied
    assert not report.is_clean


def test_an_unreachable_service_is_inconclusive_not_a_failure() -> None:
    async def dead(_method: str, _url: str, _body: str | None) -> tuple[int, str]:
        raise ConnectionError("connection refused")

    report = asyncio.run(probe_cloud_service("http://down", http=dead))
    # Nothing was reachable, so nothing is positively "not satisfied" — a black-box probe must not
    # claim non-conformance it cannot prove.
    assert report.not_satisfied == []
    assert report.verdict("R1") is Verdict.INCONCLUSIVE
    assert report.verdict("R5") is Verdict.INCONCLUSIVE


def test_a_non_default_prefix_makes_r7_inconclusive() -> None:
    report = asyncio.run(
        probe_cloud_service("http://svc", prefix="/_internal", http=_client(_profiled_app()))
    )
    assert report.verdict("R7") is Verdict.INCONCLUSIVE
    assert "non-default prefix" in _reason(report, "R7")


def test_report_payload_round_trips() -> None:
    report = asyncio.run(probe_cloud_service("http://svc", http=_client(_profiled_app())))
    payload = report.to_payload()
    assert payload["baseUrl"] == "http://svc"
    ids = [r["id"] for r in payload["requirements"]]
    assert ids == ["R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8"]
    assert all(r["reason"] for r in payload["requirements"])


def _reason(report, requirement_id: str) -> str:
    return next(p.reason for p in report.probes if p.id == requirement_id)


# --- the CLI (`python -m benzene.mesh.probe <url>`) ---------------------------------------------


def _cli_http(monkeypatch: pytest.MonkeyPatch, app: BenzeneHttpApp) -> None:
    """Point the CLI's default HTTP call at an in-memory app — no sockets, no real URL."""
    from benzene.mesh import probe as probe_module

    monkeypatch.setattr(probe_module, "_stdlib_http", lambda **_kwargs: _client(app))


def test_cli_exits_zero_and_prints_a_clean_report(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from benzene.mesh.probe import _main

    _cli_http(monkeypatch, _profiled_app())
    exit_code = _main(["http://svc"])

    out = capsys.readouterr().out
    assert exit_code == 0  # a clean audit is a zero exit — the CI gate a deployment can rely on
    assert "CLEAN" in out
    assert "R1: satisfied" in out


def test_cli_exits_nonzero_and_reports_the_failures_for_a_bare_service(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from benzene.mesh.probe import _main

    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    _cli_http(monkeypatch, BenzeneHttpApp(router))
    exit_code = _main(["http://svc", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1  # positively-unmet requirements fail the audit
    verdicts = {r["id"]: r["verdict"] for r in payload["requirements"]}
    assert verdicts["R3"] == "not-satisfied"  # /benzene/health is missing
    assert payload["baseUrl"] == "http://svc"
