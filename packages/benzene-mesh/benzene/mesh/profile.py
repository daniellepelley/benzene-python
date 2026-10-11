"""The Cloud Service Profile self-check — a wiring-time self-assessment against R1–R5 and R7.

The [Cloud Service Profile](cloud-service-profile.md) names six requirements (R1–R5, R7) a service must
satisfy so any tool can pull its operational surfaces from it with no collector. This module projects a
composition root's :class:`~benzene.core.AppDefinition` into a :class:`CloudServiceProfileReport` — a **build-time /
wiring-time** self-assessment of what the setup actually provisioned. Its :meth:`~CloudServiceProfileReport.to_profile`
output is the Contract Document's optional ``profile`` field (contract-document.md §1):
``{"name": "cloud-service", "missing": [...]}``, with ``missing`` omitted when fully conformant. A
service declares it on its :class:`~benzene.core.ContractInfo` (:func:`declare_cloud_service_profile`),
so ``benzene:spec`` serves it and the register body carries it (mesh.md §2), and the fleet can see
whether each service claims the profile and, if not, exactly which requirements its own wiring knows it
is missing.

The report also grades the separate **meshed-service** claim (mesh.md §7.1): **M1** (registration,
heartbeats and traces — the profile's former R6) and **M2** (W3C trace context joined inbound and
forwarded outbound — the former R8). Those are reported on :attr:`CloudServiceProfileReport.mesh`, never
in ``missing``/``profile.missing``: a service that does not join a mesh still claims the profile in full.
The ids R6 and R8 are retired and not reused.

This is a self-assessment of *provisioning*, not a runtime probe: it reflects what the composition
root wired, and — like ``degraded`` — it is outside both document hashes (contract-document §6) and
never changes because of runtime degradation (an unreachable collector does not make a conformant
service stop claiming the profile). It is the counterpart of the external live-probe checker, which
audits a *deployed* service over HTTP from the outside.

The profile's requirements are read straight off the ``AppDefinition`` (the registry, the
``standard_paths`` declaration and the ``contract``). The two meshed-service requirements are invisible to
pipeline inspection and are taken as explicit inputs: **M1**'s registration/heartbeat/trace *sending*
(wired into the host loop, not the request pipeline) and **M2**'s outbound ``traceparent`` propagation (a
property of the outbound clients). A composition root that wired them says so; the default is the honest
"not provisioned".
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from benzene.core import AppDefinition, intercepts_health

#: The profile's identifier, as carried in the document's ``profile.name`` (cloud-service-profile.md).
PROFILE_NAME = "cloud-service"

#: The default well-known path prefix (design-principles §5.2); R7 wants the surfaces here.
_DEFAULT_PREFIX = "/benzene"

#: The profile's requirement ids, in order (cloud-service-profile.md §2). R6 and R8 are retired.
REQUIREMENT_IDS = ("R1", "R2", "R3", "R4", "R5", "R7")

#: The meshed-service claim's own requirement ids (mesh.md §7.1) — never part of ``profile.missing``.
MESH_REQUIREMENT_IDS = ("M1", "M2")


@dataclass(frozen=True)
class RequirementCheck:
    """One requirement's verdict: whether the wiring satisfies it, and why."""

    id: str
    satisfied: bool
    reason: str


@dataclass(frozen=True)
class CloudServiceProfileReport:
    """A service's self-assessment against the Cloud Service Profile (R1–R5, R7), plus the separate
    meshed-service claim (M1, M2) on :attr:`mesh`.

    Build it with :func:`evaluate_cloud_service_profile`; read :attr:`missing` /
    :attr:`is_conformant` for the profile and :attr:`mesh_missing` / :attr:`is_meshed` for the mesh
    claim, or let :func:`declare_cloud_service_profile` put :meth:`to_profile` on the Contract Document
    so the verdict rides on ``benzene:spec`` and on the registration.
    """

    checks: tuple[RequirementCheck, ...]
    mesh: tuple[RequirementCheck, ...]
    name: str = PROFILE_NAME

    def __post_init__(self) -> None:
        ids = tuple(check.id for check in self.checks)
        if ids != REQUIREMENT_IDS:
            raise ValueError(f"expected checks for {REQUIREMENT_IDS}, got {ids}")
        mesh_ids = tuple(check.id for check in self.mesh)
        if mesh_ids != MESH_REQUIREMENT_IDS:
            raise ValueError(f"expected mesh checks for {MESH_REQUIREMENT_IDS}, got {mesh_ids}")

    @property
    def missing(self) -> list[str]:
        """The profile ids (R1–R5, R7) the wiring does not satisfy, in order (empty when conformant).

        Never contains M1/M2: the meshed-service claim is reported on :attr:`mesh_missing`."""
        return [check.id for check in self.checks if not check.satisfied]

    @property
    def is_conformant(self) -> bool:
        """True when every profile requirement (R1–R5, R7) is satisfied."""
        return not self.missing

    @property
    def mesh_missing(self) -> list[str]:
        """The meshed-service ids (M1, M2) the wiring does not satisfy (mesh.md §7.1)."""
        return [check.id for check in self.mesh if not check.satisfied]

    @property
    def is_meshed(self) -> bool:
        """True when the service can claim to be meshed: the profile in full, plus M1 and M2."""
        return self.is_conformant and not self.mesh_missing

    def reason(self, requirement_id: str) -> str:
        """The explanation recorded for one requirement id — R or M (raises ``KeyError`` if unknown)."""
        for check in (*self.checks, *self.mesh):
            if check.id == requirement_id:
                return check.reason
        raise KeyError(requirement_id)

    def to_profile(self) -> dict[str, Any]:
        """The document's ``profile`` field (contract-document §1): ``{"name", "missing"?}``.

        ``missing`` is omitted — not emitted empty — when the service is fully conformant, matching the
        wire contract. :func:`declare_cloud_service_profile` sets it as ``ContractInfo.profile``.
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
    """Assess an :class:`~benzene.core.AppDefinition` against the Cloud Service Profile (R1–R5, R7)
    and the meshed-service claim (M1, M2, mesh.md §7.1).

    The profile is read entirely off the definition. The two keyword inputs feed only the mesh claim
    (:attr:`CloudServiceProfileReport.mesh`), never ``missing``:

    ``mesh_feeds`` — set True when the composition root provisioned M1's three service-side mesh feeds:
    registration (its body the R5 Contract Document), heartbeats and traces (mesh.md §§2–5). These are
    sent from the host loop / outbound clients, not the request pipeline, so they cannot be read off
    the definition. M1 also needs a document to register — what a ``contract`` provisions — so it is
    unmet without one.

    ``trace_propagation`` — set True when the service joins inbound W3C trace context and its outbound
    Benzene clients forward ``traceparent`` (M2). This is a property of the outbound clients, again
    invisible to the definition.
    """
    paths = (
        definition.standard_paths
    )  # duck-typed benzene.http.StandardPaths (Any on the definition)
    has_http = paths is not None
    # Any HTTP surface — the standard paths, or an application router whose host serves HTTP.
    has_http_surface = has_http or definition.router is not None
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

    # R3 — health checks. The service intercepts benzene:healthcheck in the pipeline (a
    # health_interception the startup installed, or the HealthChecks on standard_paths, which
    # application_from installs as one), and — only where there is an HTTP surface — exposes the
    # aggregate at /benzene/health too. A message-only service needs only the topic.
    http_health = getattr(paths, "health", None) is not None
    intercepted = http_health or any(intercepts_health(m) for m in definition.middleware)
    health_route = not has_http_surface or http_health
    r3 = intercepted and health_route
    checks.append(
        RequirementCheck(
            "R3",
            r3,
            "health checks provisioned: the pipeline answers benzene:healthcheck"
            + (", and /benzene/health serves the aggregate" if has_http_surface else "")
            if r3
            else (
                "nothing intercepts benzene:healthcheck (install health_interception(checks), or "
                "declare StandardPaths(health=checks) where there is an HTTP surface)"
                if not intercepted
                else "the service has an HTTP surface but /benzene/health is not provisioned "
                "(standard_paths=StandardPaths(health=checks))"
            ),
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

    # R5 — the Contract Document is derived and exposed: a declared ContractInfo makes application_from
    # intercept the reserved benzene:spec topic in the pipeline, answering it wherever the transport
    # carries a reply (a one-way binding cannot answer, and that never costs R5), and — where the
    # service has an HTTP surface — /benzene/spec must route to it too.
    has_document = definition.contract is not None
    spec_route = not has_http_surface or bool(getattr(paths, "spec", False))
    r5 = has_document and spec_route
    checks.append(
        RequirementCheck(
            "R5",
            r5,
            "derived spec provisioned: the pipeline answers benzene:spec with the Contract Document "
            "wherever the transport carries a reply"
            + (", and /benzene/spec routes to it" if has_http_surface else "")
            if r5
            else (
                "no ContractInfo on the AppDefinition: benzene:spec has no derived document to answer "
                "with (declare AppDefinition(contract=ContractInfo(...)))"
                if not has_document
                else "the service has an HTTP surface but /benzene/spec is not provisioned "
                "(standard_paths=StandardPaths(spec=True))"
            ),
        )
    )

    # R7 — the well-known HTTP surfaces the service exposes default to the /benzene/ prefix. A service
    # with no HTTP surface exposes none, so meets R7 trivially; a relocated (or absent) prefix on an
    # HTTP service accepts the documented degradation for those surfaces.
    prefix = getattr(paths, "prefix", None) if has_http else None
    r7 = not has_http_surface or prefix == _DEFAULT_PREFIX
    checks.append(
        RequirementCheck(
            "R7",
            r7,
            (
                "standard paths under the default /benzene/ prefix"
                if has_http_surface
                else "no HTTP surface: there are no well-known HTTP surfaces to place"
            )
            if r7
            else (
                f"standard paths relocated to {prefix!r}"
                if has_http
                else "the service has an HTTP surface but no standard_paths: the well-known "
                "surfaces are absent"
            ),
        )
    )

    # The meshed-service claim (mesh.md §7.1), reported apart from the profile.
    # M1 — the three service-side feeds. Their sending is not readable from the pipeline, so the
    # composition root declares it; the registration's body is the Contract Document, which is.
    m1 = mesh_feeds and has_document
    m2 = trace_propagation
    mesh = (
        RequirementCheck(
            "M1",
            m1,
            "registration (the Contract Document), heartbeats and traces declared provisioned"
            if m1
            else (
                "mesh feeds declared, but there is no Contract Document to register "
                "(declare AppDefinition(contract=ContractInfo(...)))"
                if mesh_feeds
                else "mesh feeds not declared (pass mesh_feeds=True when registration/heartbeats/"
                "traces are wired)"
            ),
        ),
        # M2 — trace context propagation. A property of the outbound clients; declared by the root.
        RequirementCheck(
            "M2",
            m2,
            "trace propagation declared"
            if m2
            else "trace propagation not declared (pass trace_propagation=True when clients forward traceparent)",
        ),
    )

    return CloudServiceProfileReport(checks=tuple(checks), mesh=mesh)


def declare_cloud_service_profile(definition: AppDefinition) -> AppDefinition:
    """``definition`` with its profile self-check on its Contract Document (contract-document §1).

    Evaluates :func:`evaluate_cloud_service_profile` (R1–R5, R7 — read entirely off the definition, so
    there is nothing to declare; the meshed-service M1/M2 never reach ``profile.missing``) and sets
    the result as the ``profile`` of the
    definition's :class:`~benzene.core.ContractInfo`, so ``benzene:spec`` serves it and the mesh
    register body (:func:`~benzene.mesh.registration_document`) carries it — one document, both
    surfaces. It is outside both hashes. A definition without a ``contract`` has no document to carry
    a profile and is returned unchanged.
    """
    if definition.contract is None:
        return definition
    report = evaluate_cloud_service_profile(definition)
    return replace(definition, contract=replace(definition.contract, profile=report.to_profile()))
