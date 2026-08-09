"""The Cloud Service Profile self-check — evaluate an AppDefinition against R1–R8.

Pins the mapping from what a composition root wired to the descriptor's ``profile`` field: a fully
provisioned service is conformant (profile carries only its name), a partly-wired one lists exactly the
requirement ids it is missing, and the verdict rides on the descriptor without touching its hash
(mesh.md §2.2 — ``profile`` is self-description, not contract).
"""

from __future__ import annotations

from dataclasses import dataclass

from benzene.core import AppDefinition, HealthChecks, Registry, ServiceSpec
from benzene.http import StandardPaths
from benzene.mesh import (
    PROFILE_NAME,
    REQUIREMENT_IDS,
    CloudServiceProfileReport,
    ServiceDescriptor,
    ServiceInfo,
    evaluate_cloud_service_profile,
)
from benzene.results import Result


@dataclass
class Order:
    sku: str = ""


async def _place(_request: Order) -> Result:
    return Result.ok({})


def _full_definition() -> AppDefinition:
    """A composition root that provisions every HTTP-observable surface."""
    registry = Registry().register("orders:place", _place, request_type=Order)
    return AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(
            health=HealthChecks().add("core", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )


def test_a_fully_wired_service_is_conformant() -> None:
    report = evaluate_cloud_service_profile(
        _full_definition(), mesh_feeds=True, trace_propagation=True
    )
    assert report.is_conformant
    assert report.missing == []
    # Conformant → the profile carries only its name; missing is omitted, never emitted empty.
    assert report.to_profile() == {"name": PROFILE_NAME}


def test_missing_mesh_and_propagation_are_reported() -> None:
    # HTTP surfaces are all wired, but the root didn't declare the mesh feeds or trace propagation.
    report = evaluate_cloud_service_profile(_full_definition())
    assert report.missing == ["R6", "R8"]
    assert report.to_profile() == {"name": PROFILE_NAME, "missing": ["R6", "R8"]}
    assert "mesh feeds not declared" in report.reason("R6")


def test_message_only_service_misses_the_http_surfaces() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(AppDefinition(registry=registry))
    # No standard_paths → health (R3), spec (R5), the default paths (R7) are absent; mesh/propagation
    # undeclared (R6/R8). R1 (hostable) + R2 (a topic) + R4 (envelope-invocable) still hold.
    assert report.missing == ["R3", "R5", "R6", "R7", "R8"]
    assert set(report.missing).isdisjoint({"R1", "R2", "R4"})


def test_a_relocated_prefix_only_costs_r7() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    definition = AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(
            prefix="/_internal",
            health=HealthChecks().add("core", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )
    report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
    assert report.missing == ["R7"]
    assert "/_internal" in report.reason("R7")


def test_no_application_topics_costs_r2() -> None:
    # A registry with only reserved (benzene:*) topics serves no application traffic.
    registry = Registry().register("benzene:mesh", _place)
    report = evaluate_cloud_service_profile(AppDefinition(registry=registry))
    assert "R2" in report.missing
    assert "no non-reserved topics" in report.reason("R2")


def test_disabled_invoke_endpoint_costs_r4() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    definition = AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(
            invoke=False,
            health=HealthChecks().add("core", lambda: True),
            spec=ServiceSpec.derive(registry, service="orders"),
        ),
    )
    report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
    assert report.missing == ["R4"]


def test_router_only_definition_is_hostable() -> None:
    # An HTTP-first composition root may carry only a router; its handlers are the registry.
    from benzene.http import HttpRouter

    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(AppDefinition(router=router))
    assert "R1" not in report.missing  # a router is hostable
    assert "R2" not in report.missing  # and serves an application topic


def test_profile_rides_on_the_descriptor_without_changing_its_hash() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(
        _full_definition(), mesh_feeds=True, trace_propagation=True
    )

    plain = ServiceDescriptor.derive(registry, ServiceInfo(service="orders"))
    profiled = ServiceDescriptor.derive(
        registry, ServiceInfo(service="orders", profile=report.to_profile())
    )
    # profile is self-description, excluded from the hash (mesh.md §2.2) ...
    assert plain.descriptor_hash() == profiled.descriptor_hash()
    # ... but it does appear in the emitted payload.
    assert profiled.to_payload()["profile"] == {"name": PROFILE_NAME}


def test_report_shape_is_complete() -> None:
    report = evaluate_cloud_service_profile(_full_definition())
    assert isinstance(report, CloudServiceProfileReport)
    assert tuple(check.id for check in report.checks) == REQUIREMENT_IDS
    assert all(check.reason for check in report.checks)  # every verdict explains itself
