"""Health checks: the reserved ``benzene:healthcheck`` endpoint and the aggregate."""

from __future__ import annotations

import asyncio
import json

from benzene.core import (
    HEALTH_TOPIC,
    BenzeneMessageApplication,
    HealthCheckResult,
    HealthChecks,
    MiddlewarePipeline,
    Registry,
    health_interception,
)


def _app(checks: HealthChecks) -> BenzeneMessageApplication:
    pipeline = MiddlewarePipeline().use(health_interception(checks))
    return BenzeneMessageApplication(Registry(), pipeline)


def _hit(app: BenzeneMessageApplication, topic: str = HEALTH_TOPIC) -> dict:
    return asyncio.run(app.handle({"topic": topic, "headers": {}, "body": ""}))


def test_no_checks_is_healthy() -> None:
    response = _hit(_app(HealthChecks()))
    assert response["statusCode"] == "ok"
    assert json.loads(response["body"]) == {"isHealthy": True, "healthChecks": {}}


def test_all_healthy_returns_ok_with_the_aggregate() -> None:
    checks = HealthChecks().add("db", lambda: True).add("cache", lambda: HealthCheckResult.healthy("warm"))
    response = _hit(_app(checks))
    assert response["statusCode"] == "ok"
    body = json.loads(response["body"])
    assert body["isHealthy"] is True
    assert body["healthChecks"]["db"] == {"isHealthy": True}
    assert body["healthChecks"]["cache"] == {"isHealthy": True, "detail": "warm"}


def test_an_unhealthy_check_returns_service_unavailable_naming_it() -> None:
    checks = HealthChecks().add("db", lambda: True).add("queue", lambda: HealthCheckResult.unhealthy("timeout"))
    response = _hit(_app(checks))
    assert response["statusCode"] == "service-unavailable"
    assert "queue" in json.loads(response["body"])["detail"]


def test_a_raising_check_is_unhealthy_not_a_crash() -> None:
    def boom() -> bool:
        raise RuntimeError("down")

    response = _hit(_app(HealthChecks().add("boom", boom)))
    assert response["statusCode"] == "service-unavailable"


def test_async_checks_are_awaited() -> None:
    async def ping() -> HealthCheckResult:
        await asyncio.sleep(0)
        return HealthCheckResult.healthy()

    response = _hit(_app(HealthChecks().add("ping", ping)))
    assert response["statusCode"] == "ok"


def test_interception_ignores_version_and_passes_other_topics_through() -> None:
    checks = HealthChecks()
    app = _hit(_app(checks), topic=HEALTH_TOPIC)  # version ignored (none supplied)
    assert app["statusCode"] == "ok"

    # a non-health topic falls through to the (empty) router -> not-found
    other = _hit(_app(checks), topic="orders:place")
    assert other["statusCode"] == "not-found"


def test_report_payload_matches_the_heartbeat_health_shape() -> None:
    checks = HealthChecks().add("db", lambda: True)
    report = asyncio.run(checks.run())
    payload = report.to_payload()
    # exactly the {isHealthy, healthChecks} shape the mesh Heartbeat carries
    assert set(payload) == {"isHealthy", "healthChecks"}
    assert payload["healthChecks"]["db"]["isHealthy"] is True
