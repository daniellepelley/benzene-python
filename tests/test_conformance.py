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
    _is_subset,
    run_contract_document_cases,
    run_contract_hash_cases,
    run_http_mapping,
    run_status_vocabulary,
)


def test_status_vocabulary_conforms() -> None:
    assert run_status_vocabulary() == []


def test_http_status_mapping_conforms() -> None:
    assert run_http_mapping() == []


def test_contract_document_cases_conform() -> None:
    assert run_contract_document_cases() == []


def test_contract_hash_cases_conform() -> None:
    assert run_contract_hash_cases() == []


def _envelope_cases() -> list:
    data = json.loads((CONFORMANCE_DIR / "envelope-cases.json").read_text())
    return data["cases"]


@pytest.mark.parametrize("case", _envelope_cases(), ids=lambda c: c["name"])
def test_envelope_case(case: dict) -> None:
    app = BenzeneMessageApplication(register_canonical(Registry()))
    response = asyncio.run(app.handle(case["request"]))
    expected = case["expected"]

    assert response["statusCode"] == expected["statusCode"]
    if "headers" in expected:
        assert _is_subset(expected["headers"], response["headers"])
    if "body" in expected:
        actual = json.loads(response["body"]) if response["body"] else {}
        assert _is_subset(expected["body"], actual)
