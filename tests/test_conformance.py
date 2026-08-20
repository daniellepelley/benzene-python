"""Runs the language-neutral conformance fixtures under pytest (one test per envelope case).

The heavy lifting lives in the dependency-free ``conformance_runner`` (so it can also be run
without pytest); this module just surfaces each fixture as a granular test.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from benzene.core import BenzeneMessageApplication, Registry

from .canonical_handlers import register_canonical
from .conformance_runner import (
    CONFORMANCE_DIR,
    envelope_case_failures,
    run_contract_document_cases,
    run_contract_hash_cases,
    run_fixture_coverage,
    run_grpc_status_mapping,
    run_http_mapping,
    run_problem_details,
    run_status_vocabulary,
)


def test_status_vocabulary_conforms() -> None:
    assert run_status_vocabulary() == []


def test_http_status_mapping_conforms() -> None:
    assert run_http_mapping() == []


def test_problem_details_cases_conform() -> None:
    """problem-details-cases.json: the registry, the envelope cases, and the HTTP signalling rules.

    Required for the Benzene Core claim (registry + envelopeCases) and for the HTTP binding this
    port ships (httpRules). The fixture was vendored here with nothing reading it.
    """
    assert run_problem_details() == []


def test_grpc_status_mapping_conforms() -> None:
    """grpc-status-mapping.json, from the runner rather than only from tests/test_grpc.py.

    The mapping is dependency-free, so there was never a reason for the runner to be the one place
    that skipped it — and while it did, CI printed CONFORMANCE PASSED having not checked it.
    """
    assert run_grpc_status_mapping() == []


def test_every_vendored_fixture_is_claimed_by_a_runner() -> None:
    """A fixture nobody opens is byte-perfect, green, and checking nothing.

    The drift check guards each fixture's bytes against canonical and guards that a canonical
    fixture is not missing from the snapshot; neither notices a vendored fixture that is never
    loaded. Two mesh fixtures sat in exactly that state.
    """
    assert run_fixture_coverage() == []


def test_contract_document_cases_conform() -> None:
    assert run_contract_document_cases() == []


def test_contract_hash_cases_conform() -> None:
    assert run_contract_hash_cases() == []


def _envelope_cases() -> list:
    data = json.loads((CONFORMANCE_DIR / "envelope-cases.json").read_text())
    return data["cases"]


@pytest.mark.parametrize("case", _envelope_cases(), ids=lambda c: c["name"])
def test_envelope_case(case: dict) -> None:
    """One test per case, but the assertions come from the shared checker.

    This used to re-implement the envelope case format, and its copy had fallen behind: it never
    checked ``isSuccessful`` or ``bodyExclude``. Delegating means a case format the runner learns is
    a case format this sees too.
    """
    app = BenzeneMessageApplication(register_canonical(Registry()))
    response = asyncio.run(app.handle(case["request"]))

    assert envelope_case_failures(response, case["expected"], case["name"]) == []
