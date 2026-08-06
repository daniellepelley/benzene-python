"""Benzene ↔ gRPC status mapping — the language-neutral fixture plus the trailer rule."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from benzene.grpc import BENZENE_STATUS_TRAILER, from_grpc, to_grpc

_FIXTURE = json.loads(
    (Path(__file__).resolve().parent.parent / "conformance" / "grpc-status-mapping.json").read_text()
)


@pytest.mark.parametrize(
    "case", _FIXTURE["forward"], ids=lambda c: f"{c['from']}->{c['to']}"
)
def test_forward_maps_benzene_status_to_grpc_code(case: dict) -> None:
    # "<unknown>" in the fixture stands for any status outside the vocabulary -> Internal.
    status = "some-app-extension" if case["from"] == "<unknown>" else case["from"]
    assert to_grpc(status) == case["to"]


@pytest.mark.parametrize(
    "case", _FIXTURE["reverse"], ids=lambda c: f"{c['from']}->{c['to']}"
)
def test_reverse_maps_grpc_code_to_benzene_status(case: dict) -> None:
    assert from_grpc(case["from"]) == case["to"]


def test_unknown_grpc_code_is_unexpected_error() -> None:
    assert from_grpc("SomeFutureCode") == "unexpected-error"


def test_benzene_status_trailer_wins_verbatim() -> None:
    # created and ok both map forward to OK; the trailer preserves the exact status on the way back.
    assert to_grpc("created") == "OK"
    assert from_grpc("OK", trailer="created") == "created"
    assert from_grpc("Internal", trailer="conflict") == "conflict"  # even across a failure code
    assert BENZENE_STATUS_TRAILER == "benzene-status"
