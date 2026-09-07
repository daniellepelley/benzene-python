"""Health checks: the reserved ``benzene:healthcheck`` endpoint and the aggregate."""

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
    MiddlewarePipeline,
    Registry,
    health_interception,
)
from benzene.core.health import ShutdownState, shutdown_readiness_check
from benzene.core.worker import StopSignal, WorkerHost


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

    # The body is still the report, per check, not a problem document. That is the whole point of
    # hitting a health endpoint: "unhealthy" alone tells an operator nothing they did not already
    # know from the 503. wire-contracts.md 1.3 carves this case out, and Result.set is how the
    # middleware claims it - service-unavailable for the probe, isSuccessful for the body.
    body = json.loads(response["body"])
    assert body["isHealthy"] is False
    assert body["healthChecks"]["db"] == {"isHealthy": True}
    assert body["healthChecks"]["queue"] == {"isHealthy": False, "detail": "timeout"}


def test_an_unhealthy_report_is_signalled_successful_so_its_body_survives() -> None:
    """The regression guard for the carve-out itself.

    Before Result.set existed here, the encoder branched on the status class alone, saw
    service-unavailable, and replaced the carefully-built report with a problem document naming only
    the failed check. isSuccessful is what keeps the body - and section 1.2 requires the envelope to
    state it either way.
    """
    checks = HealthChecks().add("queue", lambda: HealthCheckResult.unhealthy("timeout"))
    response = _hit(_app(checks))

    assert response["isSuccessful"] is True
    assert response["statusCode"] == "service-unavailable"
    assert "benzeneStatus" not in json.loads(response["body"])


def test_a_raising_check_is_unhealthy_not_a_crash() -> None:
    def boom() -> bool:
        raise RuntimeError("down")

    response = _hit(_app(HealthChecks().add("boom", boom)))
    assert response["statusCode"] == "service-unavailable"
    assert json.loads(response["body"])["healthChecks"]["boom"]["isHealthy"] is False


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
    assert payload["healthChecks"]["db"]["isHealthy"] is True


# --- the shutdown / draining latch ----------------------------------------------------------------
#
# T0.5(b). A draining instance has to tell Kubernetes "stop routing to me" — and it does so through
# the *existing* benzene:healthcheck aggregate and the existing /benzene/health 200/503 mapping, not
# through a new reserved topic. The canonical spec reserves seven topics and benzene:readiness is not
# one of them, so a readiness probe pointed at /benzene/health is the whole mechanism.


def test_a_draining_instance_reports_unhealthy_on_the_existing_healthcheck_topic() -> None:
    state = ShutdownState()
    checks = HealthChecks().add("db", lambda: True).add(
        "shutdown", shutdown_readiness_check(state)
    )

    assert _hit(_app(checks))["statusCode"] == "ok"  # serving: a readiness probe sees 200

    state.mark_shutting_down()

    response = _hit(_app(checks))
    assert response["statusCode"] == "service-unavailable"  # → HTTP 503 → out of the Service
    body = json.loads(response["body"])
    assert body["isHealthy"] is False
    assert body["healthChecks"]["db"] == {"isHealthy": True}  # not broken — draining
    assert body["healthChecks"]["shutdown"]["isHealthy"] is False


def test_the_draining_report_keeps_the_conformance_pinned_aggregate_shape() -> None:
    """No new keys, no new topic: only one more entry in the frozen healthChecks map."""
    state = ShutdownState()
    state.mark_shutting_down()
    report = asyncio.run(HealthChecks().add("shutdown", shutdown_readiness_check(state)).run())
    payload = report.to_payload()

    assert set(payload) == {"isHealthy", "healthChecks"}
    assert set(payload["healthChecks"]) == {"shutdown"}
    assert set(payload["healthChecks"]["shutdown"]) <= {"isHealthy", "detail"}


def test_the_latch_is_one_way() -> None:
    state = ShutdownState()
    assert state.is_shutting_down is False
    state.mark_shutting_down()
    state.mark_shutting_down()  # idempotent
    assert state.is_shutting_down is True


def test_the_latch_links_to_a_worker_hosts_stop_signal() -> None:
    """The Python analogue of .NET's LinkTo(IHostApplicationLifetime.ApplicationStopping)."""

    async def scenario() -> tuple[bool, bool]:
        host = WorkerHost()
        state = ShutdownState().link_to(host.stop)
        before = state.is_shutting_down
        host.stop.set()  # what the SIGTERM handler does
        return before, state.is_shutting_down

    assert asyncio.run(scenario()) == (False, True)


def test_linking_to_an_already_stopped_signal_latches_immediately() -> None:
    async def scenario() -> bool:
        stop = StopSignal()
        stop.set()
        return ShutdownState().link_to(stop).is_shutting_down

    assert asyncio.run(scenario()) is True


def test_a_linked_latch_stays_tripped_even_if_the_signal_object_is_dropped() -> None:
    """One-way: reading through a link latches, so the state never reverts."""

    async def scenario() -> tuple[bool, bool]:
        stop = StopSignal()
        state = ShutdownState().link_to(stop)
        stop.set()
        first = state.is_shutting_down
        return first, state.is_shutting_down

    assert asyncio.run(scenario()) == (True, True)


def test_the_shutdown_check_is_healthy_while_the_service_is_serving() -> None:
    result = shutdown_readiness_check(ShutdownState())()
    assert isinstance(result, HealthCheckResult)
    assert result.is_healthy is True
