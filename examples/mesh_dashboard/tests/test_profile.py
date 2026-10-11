"""The Cloud Service Profile self-check, dogfooded on a fully-wired service.

Grades a realistic composition root against R1–R8 and asserts the verdict rides on the Contract
Document's ``profile`` field without changing either hash — the flow a meshed service uses to
advertise its conformance to fleet tooling (served on ``benzene:spec``, registered with a collector).
"""

from __future__ import annotations

from benzene.mesh import (
    PROFILE_NAME,
    evaluate_cloud_service_profile,
    registration_document,
)

from mesh_dashboard.profile import (
    build_profiled_definition,
    document_with_profile,
    self_check,
)


def test_a_fully_wired_service_grades_itself_conformant() -> None:
    report = self_check()
    assert report.is_conformant
    assert report.missing == []
    assert report.to_profile() == {"name": PROFILE_NAME}


def test_the_document_carries_the_profile_without_changing_its_hashes() -> None:
    profiled = document_with_profile()
    assert profiled.to_payload()["profile"] == {"name": PROFILE_NAME}

    # profile is outside both hashes (contract-document §1).
    plain = registration_document(build_profiled_definition())
    assert plain.registration_hash == profiled.registration_hash
    assert plain.contract_hash == profiled.contract_hash


def test_without_the_mesh_feeds_it_reports_them_missing() -> None:
    # The same HTTP wiring, but not meshed: R6 (feeds) + R8 (propagation) are the honest gaps.
    report = evaluate_cloud_service_profile(build_profiled_definition())
    assert report.missing == ["R6", "R8"]
