"""Health checks — the reserved ``benzene:healthcheck`` endpoint (core-concepts §, health checks).

A service intercepts the reserved topic ``benzene:healthcheck`` (plus an app-chosen alias), runs its
registered checks, and answers with the standard result: status ``ok`` and the health aggregate when
everything is healthy, ``service-unavailable`` (naming the failed checks) otherwise. Interception is
by topic id, version ignored — exactly like the mesh endpoint.

The aggregate shape (``{"isHealthy": ..., "healthChecks": {name: {"isHealthy": ...}}}``) is the one
the mesh :class:`~benzene.mesh.Heartbeat` reports, so a service runs its checks once and both the
health endpoint and the heartbeat feed reuse the result.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, Union

from benzene.results import Result, Status

from .context import Context
from .pipeline import Middleware, Next

#: The reserved topic id a service intercepts to report its health.
HEALTH_TOPIC = "benzene:healthcheck"


@dataclass(frozen=True)
class HealthCheckResult:
    """One check's outcome: healthy or not, with an optional human-readable detail."""

    is_healthy: bool
    detail: str | None = None

    @classmethod
    def healthy(cls, detail: str | None = None) -> "HealthCheckResult":
        return cls(True, detail)

    @classmethod
    def unhealthy(cls, detail: str | None = None) -> "HealthCheckResult":
        return cls(False, detail)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"isHealthy": self.is_healthy}
        if self.detail is not None:
            payload["detail"] = self.detail
        return payload


#: A health check: a callable returning a :class:`HealthCheckResult` (or a bool), sync or async.
HealthCheck = Callable[[], Union[HealthCheckResult, bool, Awaitable[Union[HealthCheckResult, bool]]]]


@dataclass(frozen=True)
class HealthReport:
    """The aggregate of every check: healthy iff all checks are (an empty set of checks is healthy)."""

    is_healthy: bool
    checks: dict[str, HealthCheckResult]

    def to_payload(self) -> dict[str, Any]:
        return {
            "isHealthy": self.is_healthy,
            "healthChecks": {name: result.to_payload() for name, result in self.checks.items()},
        }

    @property
    def unhealthy_checks(self) -> list[str]:
        return [name for name, result in self.checks.items() if not result.is_healthy]


class HealthChecks:
    """A named registry of health checks. ``add`` a check, ``run`` them into a :class:`HealthReport`."""

    def __init__(self) -> None:
        self._checks: dict[str, HealthCheck] = {}

    def add(self, name: str, check: HealthCheck) -> "HealthChecks":
        self._checks[name] = check
        return self

    async def run(self) -> HealthReport:
        """Run every check (a raising check counts as unhealthy) and aggregate the results."""
        results: dict[str, HealthCheckResult] = {}
        for name, check in self._checks.items():
            try:
                outcome = check()
                if inspect.isawaitable(outcome):
                    outcome = await outcome
                results[name] = _coerce(outcome)
            except Exception as exc:  # a check must never break the health endpoint
                results[name] = HealthCheckResult.unhealthy(f"raised {type(exc).__name__}: {exc}")
        overall = all(result.is_healthy for result in results.values())
        return HealthReport(overall, results)


def _coerce(outcome: HealthCheckResult | bool) -> HealthCheckResult:
    if isinstance(outcome, HealthCheckResult):
        return outcome
    return HealthCheckResult(bool(outcome))


def health_interception(checks: HealthChecks, *, aliases: Iterable[str] = ()) -> Middleware:
    """Middleware that answers ``benzene:healthcheck`` (and any ``aliases``) with the health aggregate.

    Healthy → status ``ok`` with the aggregate payload; unhealthy → ``service-unavailable`` naming the
    failed checks. Interception is by topic id, version ignored. Install it before the message router.
    """
    topics = {HEALTH_TOPIC, *aliases}

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if context.topic in topics:
            report = await checks.run()
            if report.is_healthy:
                context.result = Result.ok(report.to_payload())
            else:
                context.result = Result(
                    Status.SERVICE_UNAVAILABLE,
                    report.to_payload(),
                    (f"unhealthy checks: {', '.join(report.unhealthy_checks)}",),
                )
            return  # short-circuit: the reserved topic never reaches the router
        await next()

    return middleware
