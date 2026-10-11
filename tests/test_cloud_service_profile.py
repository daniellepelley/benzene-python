"""The Cloud Service Profile self-check — evaluate an AppDefinition against R1–R5 and R7.

Pins the mapping from what a composition root wired to the Contract Document's ``profile`` field: a
fully provisioned service is conformant (profile carries only its name), a partly-wired one lists
exactly the requirement ids it is missing, and the verdict rides on the document — served on
``benzene:spec`` and registered with a collector — without touching either hash (contract-document §1).
The meshed-service claim (M1, M2 — mesh.md §7.1, the former R6/R8) is reported separately and never
reaches ``profile.missing``.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from benzene.core import AppDefinition, ContractInfo, HealthChecks, Registry, application_from
from benzene.http import StandardPaths
from benzene.mesh import (
    MESH_REQUIREMENT_IDS,
    PROFILE_NAME,
    REQUIREMENT_IDS,
    CloudServiceProfileReport,
    declare_cloud_service_profile,
    evaluate_cloud_service_profile,
    registration_document,
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
        ),
        contract=ContractInfo("orders"),
    )


def test_a_fully_wired_service_is_conformant() -> None:
    report = evaluate_cloud_service_profile(
        _full_definition(), mesh_feeds=True, trace_propagation=True
    )
    assert report.is_conformant
    assert report.missing == []
    # Conformant → the profile carries only its name; missing is omitted, never emitted empty.
    assert report.to_profile() == {"name": PROFILE_NAME}
    # ... and, with the feeds and propagation declared, meshed too (mesh.md §7.1).
    assert report.mesh_missing == []
    assert report.is_meshed


def test_an_unmeshed_service_still_claims_the_profile_in_full() -> None:
    # HTTP surfaces are all wired, but the root didn't declare the mesh feeds or trace propagation:
    # that costs the meshed-service claim (M1, M2), never the profile.
    report = evaluate_cloud_service_profile(_full_definition())
    assert report.missing == []
    assert report.to_profile() == {"name": PROFILE_NAME}
    assert report.mesh_missing == ["M1", "M2"]
    assert not report.is_meshed
    assert "mesh feeds not declared" in report.reason("M1")
    assert "trace propagation not declared" in report.reason("M2")


def test_the_meshed_claim_requires_the_profile() -> None:
    # A meshed service is a Cloud Service plus M1 and M2 (mesh.md §7.1): M1/M2 met but a profile gap
    # (no health checks → R3) is not meshed.
    definition = _full_definition()
    definition.standard_paths.health = None
    report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
    assert report.missing == ["R3"]
    assert report.mesh_missing == []
    assert not report.is_meshed


def test_mesh_ids_never_reach_profile_missing() -> None:
    # The worst case for the mesh claim on the worst-wired service: missing stays within R1–R5/R7.
    report = evaluate_cloud_service_profile(AppDefinition())
    assert set(report.missing) <= set(REQUIREMENT_IDS)
    assert report.mesh_missing == ["M1", "M2"]
    assert "M1" not in report.to_profile()["missing"]
    assert "M2" not in report.to_profile()["missing"]


def test_message_only_service_misses_the_http_surfaces() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(AppDefinition(registry=registry))
    # No standard_paths → health (R3) and the default paths (R7) are absent, and with no ContractInfo
    # benzene:spec has nothing to answer with (R5). R1 (hostable) + R2 (a topic) + R4
    # (envelope-invocable) still hold.
    assert report.missing == ["R3", "R5", "R7"]
    assert set(report.missing).isdisjoint({"R1", "R2", "R4"})


def test_r5_is_the_spec_topic_for_a_message_only_service() -> None:
    # No HTTP surface: R5 is the pipeline answering benzene:spec with the derived document wherever the
    # transport carries a reply, which a ContractInfo provisions — there is no /benzene/spec half to
    # require, and a one-way binding that cannot answer never costs R5.
    registry = Registry().register("orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(
        AppDefinition(registry=registry, contract=ContractInfo("orders"))
    )
    assert "R5" not in report.missing
    assert "wherever the transport carries a reply" in report.reason("R5")


def test_r5_needs_the_http_route_where_there_is_an_http_surface() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    definition = AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(spec=False, health=HealthChecks().add("core", lambda: True)),
        contract=ContractInfo("orders"),
    )
    report = evaluate_cloud_service_profile(definition)
    assert report.missing == ["R5"]
    assert "/benzene/spec" in report.reason("R5")


def test_a_relocated_prefix_only_costs_r7() -> None:
    registry = Registry().register("orders:place", _place, request_type=Order)
    definition = AppDefinition(
        registry=registry,
        standard_paths=StandardPaths(
            prefix="/_internal",
            health=HealthChecks().add("core", lambda: True),
        ),
        contract=ContractInfo("orders"),
    )
    report = evaluate_cloud_service_profile(definition)
    assert report.missing == ["R7"]
    assert "/_internal" in report.reason("R7")


def test_no_application_topics_costs_r2() -> None:
    # A registry with only reserved (benzene:*) topics serves no application traffic.
    registry = Registry().register("benzene:spec", _place)
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
        ),
        contract=ContractInfo("orders"),
    )
    report = evaluate_cloud_service_profile(definition)
    assert report.missing == ["R4"]


def test_router_only_definition_is_hostable() -> None:
    # An HTTP-first composition root may carry only a router; its handlers are the registry.
    from benzene.http import HttpRouter

    router = HttpRouter().register("POST", "/orders", "orders:place", _place, request_type=Order)
    report = evaluate_cloud_service_profile(AppDefinition(router=router))
    assert "R1" not in report.missing  # a router is hostable
    assert "R2" not in report.missing  # and serves an application topic


def test_m1_needs_a_contract_document_to_register() -> None:
    # The feeds are declared, but with no ContractInfo there is no document for a registration to
    # carry (mesh.md §7.1 M1) — nor for benzene:spec to serve (R5).
    definition = _full_definition()
    definition.contract = None
    report = evaluate_cloud_service_profile(definition, mesh_feeds=True, trace_propagation=True)
    assert report.mesh_missing == ["M1"]
    assert "no Contract Document" in report.reason("M1")
    assert report.missing == ["R5"]


def test_profile_rides_on_the_document_without_changing_its_hashes() -> None:
    # A relocated prefix gives the self-check something to report (R7).
    relocated = _full_definition()
    relocated.standard_paths.prefix = "/_internal"
    plain = registration_document(relocated)
    definition = declare_cloud_service_profile(relocated)
    profiled = registration_document(definition)
    # profile is outside both hashes (contract-document §1) ...
    assert plain.registration_hash == profiled.registration_hash
    assert plain.contract_hash == profiled.contract_hash
    # ... but it is on the registered document, and on the one benzene:spec serves.
    assert profiled.to_payload()["profile"] == {"name": PROFILE_NAME, "missing": ["R7"]}
    served = asyncio.run(
        application_from(definition).handle({"topic": "benzene:spec", "headers": {}, "body": ""})
    )
    assert json.loads(served["body"]) == profiled.to_payload()


def test_declaring_a_profile_without_a_contract_changes_nothing() -> None:
    definition = AppDefinition(registry=Registry().register("orders:place", _place))
    assert declare_cloud_service_profile(definition) is definition


def test_report_shape_is_complete() -> None:
    report = evaluate_cloud_service_profile(_full_definition())
    assert isinstance(report, CloudServiceProfileReport)
    assert REQUIREMENT_IDS == ("R1", "R2", "R3", "R4", "R5", "R7")  # R6/R8 retired, not reused
    assert tuple(check.id for check in report.checks) == REQUIREMENT_IDS
    assert tuple(check.id for check in report.mesh) == MESH_REQUIREMENT_IDS == ("M1", "M2")
    # every verdict explains itself
    assert all(check.reason for check in (*report.checks, *report.mesh))
