"""Health checks — the reserved ``benzene:healthcheck`` endpoint (core-concepts §, health checks).

A service intercepts the reserved topic ``benzene:healthcheck`` (plus an app-chosen alias), runs its
registered checks, and answers with the standard result: status ``ok`` and the health aggregate when
everything is healthy, ``service-unavailable`` (naming the failed checks) otherwise. Interception is
by topic id, version ignored — exactly like the mesh endpoint.

The aggregate shape (``{"isHealthy": ..., "healthChecks": {name: {"isHealthy": ...}}}``) is the one
the mesh :class:`~benzene.mesh.Heartbeat` reports, so a service runs its checks once and both the
health endpoint and the heartbeat feed reuse the result.

This is also where *readiness* lives, because the spec reserves no separate readiness topic and this
port will not invent one. :class:`ShutdownState` plus :func:`shutdown_readiness_check` make a
draining instance report unhealthy on the **existing** ``benzene:healthcheck`` aggregate, which the
HTTP host already maps to 503 at ``GET /benzene/health`` — so a Kubernetes ``readinessProbe`` pointed
there stops routing to a pod the moment its drain begins, with no new surface at all.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from benzene.results import Result, Status

from .context import Context
from .pipeline import Middleware, Next

#: The reserved topic id a service intercepts to report its health.
HEALTH_TOPIC = "benzene:healthcheck"


class DuplicateHealthCheckError(Exception):
    """Raised when two health checks are registered under the same name.

    Mirrors :class:`~benzene.core.DuplicateHandlerError`: a duplicate registration is a *startup*
    error, not a silently-dropped check — a health endpoint that quietly lost a check would lie.
    """


@dataclass(frozen=True)
class HealthCheckResult:
    """One check's outcome: healthy or not, with an optional human-readable detail."""

    is_healthy: bool
    detail: str | None = None

    @classmethod
    def healthy(cls, detail: str | None = None) -> HealthCheckResult:
        return cls(True, detail)

    @classmethod
    def unhealthy(cls, detail: str | None = None) -> HealthCheckResult:
        return cls(False, detail)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"isHealthy": self.is_healthy}
        if self.detail is not None:
            payload["detail"] = self.detail
        return payload


#: A health check: a callable returning a :class:`HealthCheckResult` (or a bool), sync or async.
HealthCheck = Callable[[], HealthCheckResult | bool | Awaitable[HealthCheckResult | bool]]


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

    def add(self, name: str, check: HealthCheck) -> HealthChecks:
        if name in self._checks:
            raise DuplicateHealthCheckError(
                f"A health check named {name!r} is already registered. Each check needs a distinct "
                "name — rename one, or remove the duplicate registration."
            )
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


class ShutdownSignal(Protocol):
    """Anything that can say "we have been asked to wind down" — structurally, just ``is_set()``.

    :class:`~benzene.core.worker.StopSignal` satisfies it, and so do :class:`asyncio.Event` and
    :class:`threading.Event`, so a service embedded in some other host can link the latch to whatever
    that host already uses without adopting Benzene's hosting layer.
    """

    def is_set(self) -> bool: ...


class ShutdownState:
    """A one-way "this instance is draining" latch, read by :func:`shutdown_readiness_check`.

    The Python analogue of .NET's ``ShutdownState``/``LinkTo(ApplicationStopping)``. Trip it — or
    :meth:`link_to` the signal that gets tripped for you — and the health aggregate turns unhealthy,
    which the HTTP host serves as a 503 on the existing ``/benzene/health`` path. That is how a pod
    tells Kubernetes *stop sending me new work* while its in-flight work finishes::

        host = WorkerHost()                                   # catches SIGTERM, sets host.stop
        checks.add("shutdown", shutdown_readiness_check(ShutdownState().link_to(host.stop)))

    It never reverts: an instance that has begun draining is on its way out, and a health endpoint
    that flapped back to 200 would invite the Service to route to it again.

    :meth:`link_to` reads the signal *at probe time* rather than subscribing to it, so it needs no
    running event loop, no background task and no cleanup — it can be wired at import time, before
    the loop exists, which is where health checks are usually registered. Reading through a tripped
    link latches, so the state stays tripped even if the signal object is later discarded.
    """

    def __init__(self) -> None:
        self._is_shutting_down = False
        self._linked: list[ShutdownSignal] = []

    def mark_shutting_down(self) -> None:
        """Begin draining. Idempotent, and one-way — nothing puts this instance back in rotation."""
        self._is_shutting_down = True

    @property
    def is_shutting_down(self) -> bool:
        """True once draining has begun, here or on any linked signal."""
        if not self._is_shutting_down and any(signal.is_set() for signal in self._linked):
            self._is_shutting_down = True
        return self._is_shutting_down

    def link_to(self, signal: ShutdownSignal) -> ShutdownState:
        """Trip this latch whenever ``signal`` is set — pass the ``stop`` of a :class:`WorkerHost`.

        Returns ``self``, so it reads as one expression in a registration. An already-set signal
        latches immediately.
        """
        self._linked.append(signal)
        return self


def shutdown_readiness_check(state: ShutdownState) -> HealthCheck:
    """A check that goes unhealthy once ``state`` says the instance is draining.

    Register it on the health checks a **readiness** probe reads. A draining instance is not broken,
    so do not point a *liveness* probe at a set of checks containing this one: Kubernetes would
    restart the pod in the middle of its own drain, which is precisely the abandoned in-flight work
    the drain exists to avoid. With only one health surface in this port, that means a liveness probe
    should not target ``/benzene/health`` once this check is registered on it.
    """

    def check() -> HealthCheckResult:
        if state.is_shutting_down:
            return HealthCheckResult.unhealthy(
                "shutting down: draining in-flight work, stop routing new requests here"
            )
        return HealthCheckResult.healthy()

    return check


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
                # service-unavailable so an HTTP probe sees a 503 and a load balancer drains this
                # instance, but explicitly successful so the response still carries the report - which
                # check failed and why is the entire value of hitting a health endpoint. Without the
                # override the encoder would replace the report with a problem document naming only
                # the failed checks (wire-contracts.md 1.3's carve-out; the same call .NET's
                # HealthCheckProcessor makes).
                context.result = Result.set(
                    Status.SERVICE_UNAVAILABLE, report.to_payload(), successful=True
                )
            return  # short-circuit: the reserved topic never reaches the router
        await next()

    return middleware
