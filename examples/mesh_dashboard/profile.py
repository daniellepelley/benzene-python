"""Self-check a service against the Cloud Service Profile and carry the verdict on its descriptor.

The [Cloud Service Profile](../../docs/cloud-service-profile.md) names eight requirements (R1–R8) a
service must satisfy to be a first-class fleet citizen. A composition root can grade its *own* wiring
with :func:`~benzene.mesh.evaluate_cloud_service_profile` and attach the result to the descriptor's
``profile`` field, so any tool that reaches the reserved ``benzene:mesh`` topic can ask a running
service whether it claims the profile and, if not, which requirements it is missing.

This dogfoods that flow end to end: a fully-wired service (registry + the well-known ``/benzene/*``
surfaces via :class:`~benzene.http.StandardPaths`, plus the mesh feeds and trace propagation a meshed
service provisions) grades itself conformant, and its descriptor carries ``profile: {"name":
"cloud-service"}`` — with no ``missing`` — without disturbing the ``descriptorHash``.
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import AppDefinition, HealthChecks, Registry, ServiceSpec
from benzene.http import StandardPaths
from benzene.mesh import (
    CloudServiceProfileReport,
    ServiceDescriptor,
    ServiceInfo,
    evaluate_cloud_service_profile,
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
    registry = Registry().register("orders:place", _place, request_type=PlaceOrder)
    return AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(
            health=HealthChecks().add("order-store", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )


def self_check() -> CloudServiceProfileReport:
    """Grade the service's wiring against R1–R8 (mesh feeds + trace propagation are declared here)."""
    return evaluate_cloud_service_profile(
        build_profiled_definition(), mesh_feeds=True, trace_propagation=True
    )


def descriptor_with_profile() -> ServiceDescriptor:
    """Derive the service descriptor with its self-assessed profile attached (mesh.md §2)."""
    report = self_check()
    definition = build_profiled_definition()
    return ServiceDescriptor.derive(
        definition.registry,
        ServiceInfo(service="orders", service_version="1.0.0", profile=report.to_profile()),
    )


def _main() -> None:
    report = self_check()
    print("conformant:", report.is_conformant, "missing:", report.missing)
    for check in report.checks:
        print(f"  {check.id}: {'ok' if check.satisfied else 'MISSING'} — {check.reason}")
    print("descriptor profile:", descriptor_with_profile().to_payload()["profile"])


if __name__ == "__main__":
    _main()
