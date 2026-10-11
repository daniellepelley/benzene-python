"""Self-check a service against the Cloud Service Profile and carry the verdict on its Contract Document.

The [Cloud Service Profile](../../docs/cloud-service-profile.md) names six requirements (R1–R5, R7) a
service must satisfy so any tool can pull its operational surfaces from it; being **meshed** adds M1
(registration, heartbeats, traces) and M2 (trace propagation), mesh.md §7.1. A composition root can
grade its *own* wiring with :func:`~benzene.mesh.evaluate_cloud_service_profile`, and
:func:`~benzene.mesh.declare_cloud_service_profile` puts the result on the Contract Document's
``profile`` field — so ``benzene:spec`` serves it, and the registration a mesh collector receives
(the same document, mesh.md §2) carries it: the fleet can see whether a running service claims the
profile and, if not, which requirements it is missing.

This dogfoods that flow end to end: a fully-wired service (registry + the well-known ``/benzene/*``
surfaces via :class:`~benzene.http.StandardPaths`, plus the mesh feeds and trace propagation a meshed
service provisions) grades itself conformant and meshed, and its document carries ``profile: {"name":
"cloud-service"}`` — with no ``missing`` — without disturbing either hash.
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import AppDefinition, ContractDocument, ContractInfo, HealthChecks, Registry
from benzene.http import StandardPaths
from benzene.mesh import (
    CloudServiceProfileReport,
    declare_cloud_service_profile,
    evaluate_cloud_service_profile,
    registration_document,
)
from benzene.results import Result


@dataclass
class PlaceOrder:
    sku: str = ""
    quantity: int = 1


async def _place(_request: PlaceOrder) -> Result:
    return Result.created({})


def build_profiled_definition() -> AppDefinition:
    """A composition root that provisions every profile surface a single service can wire itself."""
    registry = Registry().register("orders:place", _place)  # request_type inferred from _place
    return AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(health=HealthChecks().add("order-store", lambda: True)),
        # the self-description: benzene:spec + /benzene/spec, and the mesh register body
        contract=ContractInfo("orders", service_version="1.0.0", version_scheme="semver"),
    )


def self_check() -> CloudServiceProfileReport:
    """Grade the wiring against R1–R5/R7, and the mesh claim M1/M2 (its feeds + propagation declared)."""
    return evaluate_cloud_service_profile(
        build_profiled_definition(), mesh_feeds=True, trace_propagation=True
    )


def document_with_profile() -> ContractDocument:
    """The service's Contract Document with its self-assessed profile on it (contract-document §1)."""
    definition = declare_cloud_service_profile(build_profiled_definition())
    return registration_document(definition)


def _main() -> None:
    report = self_check()
    print("conformant:", report.is_conformant, "missing:", report.missing)
    print("meshed:", report.is_meshed, "mesh missing:", report.mesh_missing)
    for check in (*report.checks, *report.mesh):
        print(f"  {check.id}: {'ok' if check.satisfied else 'MISSING'} — {check.reason}")
    print("document profile:", document_with_profile().to_payload()["profile"])


if __name__ == "__main__":
    _main()
