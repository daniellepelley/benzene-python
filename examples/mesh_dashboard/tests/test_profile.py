"""The Cloud Service Profile self-check, dogfooded on a fully-wired service.

Grades a realistic composition root against R1–R8 and asserts the verdict rides on the descriptor's
``profile`` field without changing the hash — the flow a meshed service uses to advertise its
conformance to fleet tooling.
"""

from __future__ import annotations

from benzene.mesh import (
    PROFILE_NAME,
    ServiceDescriptor,
    ServiceInfo,
    evaluate_cloud_service_profile,
)

from mesh_dashboard.profile import (
    build_profiled_definition,
    descriptor_with_profile,
    self_check,
)


def test_a_fully_wired_service_grades_itself_conformant() -> None:
    report = self_check()
    assert report.is_conformant
    assert report.missing == []
    assert report.to_profile() == {"name": PROFILE_NAME}


def test_the_descriptor_carries_the_profile_without_changing_its_hash() -> None:
    profiled = descriptor_with_profile()
    payload = profiled.to_payload()
    assert payload["profile"] == {"name": PROFILE_NAME}

    # profile is self-description, excluded from the hash (mesh.md §2.2).
    plain = ServiceDescriptor.derive(
        build_profiled_definition().registry,
        ServiceInfo(service="orders", service_version="1.0.0"),
    )
    assert plain.descriptor_hash() == profiled.descriptor_hash()


def test_without_the_mesh_feeds_it_reports_them_missing() -> None:
    # The same HTTP wiring, but not meshed: R6 (feeds) + R8 (propagation) are the honest gaps.
    report = evaluate_cloud_service_profile(build_profiled_definition())
    assert report.missing == ["R6", "R8"]
