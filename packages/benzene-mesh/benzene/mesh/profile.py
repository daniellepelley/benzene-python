"""The Cloud Service Profile self-check — a wiring-time self-assessment against R1–R8.

The [Cloud Service Profile](cloud-service-profile.md) names eight requirements (R1–R8) a service must
satisfy to be a first-class fleet citizen. This module projects a composition root's
:class:`~benzene.core.AppDefinition` into a :class:`CloudServiceProfileReport` — a **build-time /
wiring-time** self-assessment of what the setup actually provisioned. Its :meth:`~CloudServiceProfileReport.to_profile`
output is the descriptor's optional ``profile`` field (mesh.md §2): ``{"name": "cloud-service",
"missing": [...]}``, with ``missing`` omitted when fully conformant. Any tool that can reach the
reserved ``benzene:mesh`` topic can then ask a running service whether it claims the profile and, if
not, exactly which requirements its own wiring knows it is missing.

This is a self-assessment of *provisioning*, not a runtime probe: it reflects what the composition
root wired, and — like ``degraded`` — it never participates in the ``descriptorHash`` (mesh.md §2.2)
and never changes because of runtime degradation (an unreachable collector does not make a conformant
service stop claiming the profile). It is the counterpart of the external live-probe checker, which
audits a *deployed* service over HTTP from the outside.

Most requirements are read straight off the ``AppDefinition`` (the registry and the ``standard_paths``
declaration). Two are structurally invisible to pipeline inspection and are taken as explicit inputs —
exactly the pair the profile spec (§5) singles out as unobservable from a single service's surfaces:
**R6**'s register/heartbeat/trace *sending* (wired into the host loop, not the request pipeline) and
**R8**'s outbound ``traceparent`` propagation (a property of the outbound clients). A composition root
that wired them says so; the default is the honest "not provisioned".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from benzene.core import AppDefinition

#: The profile's identifier, as carried in the descriptor's ``profile.name`` (cloud-service-profile.md).
PROFILE_NAME = "cloud-service"

#: The default well-known path prefix (design-principles §5.2); R7 wants the surfaces here.
_DEFAULT_PREFIX = "/benzene"

#: The requirement ids, in order.
REQUIREMENT_IDS = ("R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8")


@dataclass(frozen=True)
class RequirementCheck:
    """One requirement's verdict: whether the wiring satisfies it, and why."""

    id: str
    satisfied: bool
    reason: str


@dataclass(frozen=True)
class CloudServiceProfileReport:
    """A service's self-assessment against the Cloud Service Profile's R1–R8.

    Build it with :func:`evaluate_cloud_service_profile`; read :attr:`missing` /
    :attr:`is_conformant`, or hand :meth:`to_profile` to a :class:`~benzene.mesh.ServiceInfo` so the
    verdict rides on the service descriptor.
    """

    checks: tuple[RequirementCheck, ...]
    name: str = PROFILE_NAME

    def __post_init__(self) -> None:
        if len(self.checks) != len(REQUIREMENT_IDS):
            raise ValueError(f"expected {len(REQUIREMENT_IDS)} checks, got {len(self.checks)}")

    @property
    def missing(self) -> list[str]:
        """The requirement ids the wiring does not satisfy, in R-order (empty when conformant)."""
        return [check.id for check in self.checks if not check.satisfied]

    @property
    def is_conformant(self) -> bool:
        """True when every requirement is satisfied."""
        return not self.missing

    def reason(self, requirement_id: str) -> str:
        """The explanation recorded for one requirement id (raises ``KeyError`` if unknown)."""
        for check in self.checks:
            if check.id == requirement_id:
                return check.reason
        raise KeyError(requirement_id)

    def to_profile(self) -> dict[str, Any]:
        """The descriptor ``profile`` field (mesh.md §2): ``{"name", "missing"?}``.

        ``missing`` is omitted — not emitted empty — when the service is fully conformant, matching the
        wire contract. Feed this to :class:`~benzene.mesh.ServiceInfo`'s ``profile`` argument.
        """
        profile: dict[str, Any] = {"name": self.name}
        missing = self.missing
        if missing:
            profile["missing"] = missing
        return profile


def _application_topics(definition: AppDefinition) -> list[str]:
    """Every topic the definition serves, from the registry (or, failing that, the HTTP router)."""
    source = definition.registry
    if source is None:
        source = definition.router
    if source is None:
        return []
    return [d.topic for d in source.definitions()]


def _is_reserved(topic: str) -> bool:
    return topic.startswith("benzene:")


def evaluate_cloud_service_profile(
    definition: AppDefinition,
    *,
    mesh_feeds: bool = False,
    trace_propagation: bool = False,
) -> CloudServiceProfileReport:
    """Assess an :class:`~benzene.core.AppDefinition` against the Cloud Service Profile (R1–R8).

    ``mesh_feeds`` — set True when the composition root provisioned the service-side mesh feeds
    (``benzene:mesh`` descriptor + register + heartbeat + trace sending, mesh.md §§1–4). These are
    sent from the host loop / outbound clients, not the request pipeline, so they cannot be read off
    the definition (R6).

    ``trace_propagation`` — set True when the service joins inbound W3C trace context and its outbound
    Benzene clients forward ``traceparent`` (R8). This is a property of the outbound clients, again
    invisible to the definition.
    """
    paths = (
        definition.standard_paths
    )  # duck-typed benzene.http.StandardPaths (Any on the definition)
    has_http = paths is not None
    topics = _application_topics(definition)
    app_topics = [t for t in topics if not _is_reserved(t)]

    checks: list[RequirementCheck] = []

    # R1 — a hosted middleware pipeline behind at least one binding. At wiring time this is "there is a
    # runnable app to host": a registry (or an HTTP router whose handlers become one).
    runnable = definition.registry is not None or definition.router is not None
    checks.append(
        RequirementCheck(
            "R1",
            runnable,
            "registry/router present — a pipeline is hostable"
            if runnable
            else "no registry or router: nothing for a transport to host",
        )
    )

    # R2 — application topics served through the registry.
    checks.append(
        RequirementCheck(
            "R2",
            bool(app_topics),
            f"{len(app_topics)} application topic(s) registered"
            if app_topics
            else "no non-reserved topics registered",
        )
    )

    # R3 — health checks. The observable signal is the /benzene/health aggregate being provisioned.
    r3 = has_http and getattr(paths, "health", None) is not None
    checks.append(
        RequirementCheck(
            "R3",
            r3,
            "health aggregate provisioned"
            if r3
            else "no HealthChecks on standard_paths (benzene:healthcheck / /benzene/health)",
        )
    )

    # R4 — wire-envelope invocability. A Benzene app is always envelope-invocable over its transport;
    # where there is an HTTP surface, the envelope endpoint must default to /benzene/invoke (on unless
    # explicitly disabled).
    r4 = runnable and (not has_http or bool(getattr(paths, "invoke", False)))
    checks.append(
        RequirementCheck(
            "R4",
            r4,
            "envelope-invocable" + (" (/benzene/invoke enabled)" if has_http and r4 else "")
            if r4
            else "the HTTP invoke endpoint is disabled",
        )
    )

    # R5 — derived spec exposed (the /benzene/spec source is wired).
    r5 = has_http and getattr(paths, "spec", None) is not None
    checks.append(
        RequirementCheck(
            "R5",
            r5,
            "derived spec provisioned"
            if r5
            else "no ServiceSpec on standard_paths (/benzene/spec)",
        )
    )

    # R6 — mesh service-side feeds. Not readable from the pipeline; declared by the composition root.
    checks.append(
        RequirementCheck(
            "R6",
            mesh_feeds,
            "mesh feeds declared provisioned"
            if mesh_feeds
            else "mesh feeds not declared (pass mesh_feeds=True when register/heartbeat/traces are wired)",
        )
    )

    # R7 — the well-known surfaces default to the /benzene/ prefix. A relocated (or absent) prefix
    # accepts the documented degradation for those surfaces.
    prefix = getattr(paths, "prefix", None) if has_http else None
    r7 = prefix == _DEFAULT_PREFIX
    checks.append(
        RequirementCheck(
            "R7",
            r7,
            "standard paths under the default /benzene/ prefix"
            if r7
            else (
                f"standard paths relocated to {prefix!r}"
                if has_http
                else "no standard_paths declared: the well-known surfaces are absent"
            ),
        )
    )

    # R8 — trace context propagation. A property of the outbound clients; declared by the root.
    checks.append(
        RequirementCheck(
            "R8",
            trace_propagation,
            "trace propagation declared"
            if trace_propagation
            else "trace propagation not declared (pass trace_propagation=True when clients forward traceparent)",
        )
    )

    return CloudServiceProfileReport(checks=tuple(checks))
