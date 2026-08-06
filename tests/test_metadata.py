"""Transport metadata: topic + header resolution from a native metadata dictionary (wire-contracts §2)."""

from __future__ import annotations

import json

import pytest
from benzene.core import DEFAULT_TOPIC_KEY, MetadataKeys, read_message_metadata, resolve_version

from .conformance_runner import CONFORMANCE_DIR, run_transport_metadata


def test_transport_metadata_conforms() -> None:
    assert run_transport_metadata() == []


def _cases() -> list:
    data = json.loads((CONFORMANCE_DIR / "transport-metadata-cases.json").read_text())
    metadata = [(c, None) for c in data.get("metadataCases", [])]
    override = [(c, c["metadataKeys"]["topic"]) for c in data.get("overrideCases", [])]
    return metadata + override


@pytest.mark.parametrize("case,topic_key", _cases(), ids=lambda v: v["name"] if isinstance(v, dict) else v)
def test_metadata_case(case: dict, topic_key: str | None) -> None:
    keys = MetadataKeys() if topic_key is None else MetadataKeys(topic=topic_key)
    topic, headers = read_message_metadata(case["metadata"], keys)
    expected = case["expected"]
    if "topic" in expected:
        assert topic == expected["topic"]
    for key, value in expected.get("headers", {}).items():
        assert headers[key] == value
    for key in expected.get("headersExclude", []):
        assert key.lower() not in headers
    if "version" in expected:
        assert resolve_version(headers) == expected["version"]


def test_defaults_and_case_insensitivity() -> None:
    assert DEFAULT_TOPIC_KEY == "topic"
    topic, headers = read_message_metadata({"Topic": "orders:place", "X-Tenant": "acme"})
    assert topic == "orders:place"
    assert headers == {"x-tenant": "acme"}  # keys lower-cased, topic consumed


def test_non_reserved_key_does_not_route() -> None:
    topic, headers = read_message_metadata({"benzene-topic": "orders:place"})
    assert topic == ""  # only the configured key routes
    assert headers == {"benzene-topic": "orders:place"}
