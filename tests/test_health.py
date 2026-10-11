"""Health checks: the reserved ``benzene:healthcheck`` topic and the health response (wire §5).

The topic answers ``ok`` whenever the checks ran; ``isHealthy`` carries the verdict, and each check
reports a ``status`` of ``ok``, ``warning`` or ``failed``.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from benzene.core import (
    HEALTH_TOPIC,
    BenzeneMessageApplication,
    DuplicateHealthCheckError,
    HealthCheckResult,
    HealthChecks,
    HealthStatus,
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
    assert body["healthChecks"]["db"] == {"status": "ok"}
    assert body["healthChecks"]["cache"] == {"status": "ok", "detail": "warm"}


def test_an_unhealthy_service_is_still_a_successful_health_query() -> None:
    checks = HealthChecks().add("db", lambda: True).add("queue", lambda: HealthCheckResult.unhealthy("timeout"))
    response = _hit(_app(checks))

    # The checks ran, so the query succeeded: status ok, the verdict in isHealthy (wire §5). The body
    # is the report, per check, never a problem document - which check failed and why is the point.
    assert response["statusCode"] == "ok"
    assert response["isSuccessful"] is True
    body = json.loads(response["body"])
    assert body["isHealthy"] is False
    assert body["healthChecks"]["db"] == {"status": "ok"}
    assert body["healthChecks"]["queue"] == {"status": "failed", "detail": "timeout"}


def test_a_warning_is_reported_without_flipping_is_healthy() -> None:
    checks = HealthChecks().add("disk", lambda: HealthCheckResult.warning("85% full"))
    body = json.loads(_hit(_app(checks))["body"])
    assert body["isHealthy"] is True
    assert body["healthChecks"]["disk"] == {"status": HealthStatus.WARNING, "detail": "85% full"}


def test_type_and_data_are_written_verbatim() -> None:
    checks = HealthChecks().add(
        "Database",
        lambda: HealthCheckResult.healthy(type="Database", data={"CanConnect": True}),
    )
    body = json.loads(_hit(_app(checks))["body"])
    assert body["healthChecks"]["Database"] == {
        "status": "ok", "type": "Database", "data": {"CanConnect": True}
    }


def test_a_raising_check_is_unhealthy_not_a_crash() -> None:
    def boom() -> bool:
        raise RuntimeError("down")

    response = _hit(_app(HealthChecks().add("boom", boom)))
    assert response["statusCode"] == "ok"
    body = json.loads(response["body"])
    assert body["isHealthy"] is False
    assert body["healthChecks"]["boom"]["status"] == "failed"


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


def test_duplicate_check_name_is_a_startup_error() -> None:
    checks = HealthChecks().add("db", lambda: True)
    with pytest.raises(DuplicateHealthCheckError):
        checks.add("db", lambda: False)  # same name — a startup error, not a silent overwrite


def test_report_payload_matches_the_heartbeat_health_shape() -> None:
    checks = HealthChecks().add("db", lambda: True)
    report = asyncio.run(checks.run())
    payload = report.to_payload()
    # exactly the {isHealthy, healthChecks} shape the mesh Heartbeat carries
    assert set(payload) == {"isHealthy", "healthChecks"}
    assert payload["healthChecks"]["db"] == {"status": "ok"}
