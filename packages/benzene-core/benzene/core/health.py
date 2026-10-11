"""Health checks — the reserved ``benzene:healthcheck`` topic (wire-contracts §5, §7).

A service intercepts the reserved topic ``benzene:healthcheck`` (plus an app-chosen alias), runs its
registered checks, and answers with the health response. The result's status is ``ok`` whenever the
checks ran — an unhealthy service is still a *successful health query* — and the payload's
``isHealthy`` carries the verdict. A transport with a status of its own maps the verdict onto it: the
HTTP binding's ``GET /benzene/health`` answers 503 when ``isHealthy`` is false. Interception is by
topic id, version ignored — exactly like the mesh endpoint.

The response shape (``{"isHealthy": ..., "healthChecks": {name: {"status": "ok"|"warning"|"failed",
...}}}``) is also what the mesh :class:`~benzene.mesh.Heartbeat` reports, so a service runs its checks
once and both the health endpoint and the heartbeat feed reuse the result.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from benzene.results import Result

from .context import Context
from .pipeline import Middleware, Next

#: The reserved topic id a service intercepts to report its health.
HEALTH_TOPIC = "benzene:healthcheck"

#: The attribute :func:`health_interception` marks its middleware with, so a composition root's
#: pipeline can be read for it (the profile's R3) without running it.
_HEALTH_INTERCEPTION_MARK = "__benzene_health_interception__"


class HealthStatus:
    """A check's status (wire-contracts §5) — its own vocabulary, distinct from the result statuses.

    Plain string constants, like :class:`~benzene.results.Status`: ``isHealthy`` is true iff no check
    reports ``failed``, so a ``warning`` is visible without draining the instance.
    """

    OK = "ok"
    WARNING = "warning"
    FAILED = "failed"


class DuplicateHealthCheckError(Exception):
    """Raised when two health checks are registered under the same name.

    Mirrors :class:`~benzene.core.DuplicateHandlerError`: a duplicate registration is a *startup*
    error, not a silently-dropped check — a health endpoint that quietly lost a check would lie.
    """


@dataclass(frozen=True)
class HealthCheckResult:
    """One check's outcome: a :class:`HealthStatus`, plus optional detail for whoever reads the report.

    Build one with :meth:`healthy`, :meth:`warning` or :meth:`unhealthy`; a check may also just return
    a ``bool`` (``True`` is ``ok``, ``False`` is ``failed``). ``type`` names the kind of dependency
    (``"Database"``) and ``data`` is a free-form diagnostic bag written verbatim (wire-contracts §5).
    ``detail`` is a human-readable line this port adds; readers ignore what they don't know.
    """

    status: str = HealthStatus.OK
    detail: str | None = None
    type: str | None = None
    data: Mapping[str, Any] | None = None

    @property
    def is_healthy(self) -> bool:
        """Whether this check keeps the service healthy: anything but ``failed`` (a warning does)."""
        return self.status != HealthStatus.FAILED

    @classmethod
    def healthy(
        cls, detail: str | None = None, *, type: str | None = None, data: Mapping[str, Any] | None = None
    ) -> HealthCheckResult:
        return cls(HealthStatus.OK, detail, type, data)

    @classmethod
    def warning(
        cls, detail: str | None = None, *, type: str | None = None, data: Mapping[str, Any] | None = None
    ) -> HealthCheckResult:
        """Degraded but serving — reported, and does not flip ``isHealthy``."""
        return cls(HealthStatus.WARNING, detail, type, data)

    @classmethod
    def unhealthy(
        cls, detail: str | None = None, *, type: str | None = None, data: Mapping[str, Any] | None = None
    ) -> HealthCheckResult:
        return cls(HealthStatus.FAILED, detail, type, data)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status}
        if self.type is not None:
            payload["type"] = self.type
        if self.data is not None:
            payload["data"] = dict(self.data)  # keys verbatim — no naming policy (wire-contracts §5)
        if self.detail is not None:
            payload["detail"] = self.detail
        return payload


#: A health check: a callable returning a :class:`HealthCheckResult` (or a bool), sync or async.
HealthCheck = Callable[[], HealthCheckResult | bool | Awaitable[HealthCheckResult | bool]]


@dataclass(frozen=True)
class HealthReport:
    """The aggregate of every check: healthy iff no check failed (an empty set of checks is healthy)."""

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
        overall = all(result.is_healthy for result in results.values())  # a warning is healthy
        return HealthReport(overall, results)


def _coerce(outcome: HealthCheckResult | bool) -> HealthCheckResult:
    if isinstance(outcome, HealthCheckResult):
        return outcome
    return HealthCheckResult.healthy() if outcome else HealthCheckResult.unhealthy()


def health_interception(checks: HealthChecks, *, aliases: Iterable[str] = ()) -> Middleware:
    """Middleware that answers ``benzene:healthcheck`` (and any ``aliases``) with the health response.

    Always status ``ok`` once the checks ran, with the report as payload; ``isHealthy`` in it carries
    the verdict (wire-contracts §5). A transport maps the verdict where it has a convention for one —
    the HTTP ``/benzene/health`` route answers 503 — so a queue or a direct invoke asking for health
    gets the report as an ordinary success. Interception is by topic id, version ignored. Install it
    before the message router.
    """
    topics = {HEALTH_TOPIC, *aliases}

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        if context.topic in topics:
            report = await checks.run()
            context.result = Result.ok(report.to_payload())
            return  # short-circuit: the reserved topic never reaches the router
        await next()

    setattr(middleware, _HEALTH_INTERCEPTION_MARK, True)
    return middleware


def intercepts_health(middleware: Middleware) -> bool:
    """Whether ``middleware`` is a :func:`health_interception` — answers ``benzene:healthcheck``."""
    return getattr(middleware, _HEALTH_INTERCEPTION_MARK, False) is True
