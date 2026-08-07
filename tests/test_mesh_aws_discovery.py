"""Unit tests for the AWS **Lambda discovery source** — tag-filtered self-discovery, with a fake client.

Drives :class:`~benzene.mesh.aws.AwsLambdaDiscoveryProvider` off a hand-written fake
:class:`~benzene.mesh.aws.LambdaClient` (no ``boto3``, no ``moto``): a fake account of tagged/untagged
Lambda functions is enumerated (paginated ``list_functions``) and tag-read (``list_tags``), and only the
``benzene``-tagged functions become :class:`~benzene.mesh.MeshServiceEntry` records — carrying the HTTP
base URL and the ``benzene:mesh-path`` prefix override off their tags.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from benzene.mesh.aws import (
    AwsLambdaDiscoveryProvider,
    Boto3LambdaClient,
    MeshDiscoveryFilter,
)


class _FakeLambda:
    """A fake Lambda control plane: canned function pages + per-ARN tag maps, recording every call."""

    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        tags: Mapping[str, Mapping[str, str]],
    ) -> None:
        self._pages = pages
        self._tags = tags
        self.list_functions_markers: list[str | None] = []
        self.list_tags_resources: list[str] = []

    def list_functions(self, *, marker: str | None = None) -> Mapping[str, Any]:
        self.list_functions_markers.append(marker)
        index = 0 if marker is None else int(marker)
        response: dict[str, Any] = {"Functions": self._pages[index]}
        if index + 1 < len(self._pages):
            response["NextMarker"] = str(index + 1)
        return response

    def list_tags(self, *, resource: str) -> Mapping[str, Any]:
        self.list_tags_resources.append(resource)
        return {"Tags": dict(self._tags.get(resource, {}))}


def _account() -> _FakeLambda:
    """orders + payments carry the benzene tag (orders relocated its mesh prefix); billing does not."""
    functions = [
        {"FunctionName": "orders", "FunctionArn": "arn:orders"},
        {"FunctionName": "billing", "FunctionArn": "arn:billing"},
        {"FunctionName": "payments", "FunctionArn": "arn:payments"},
    ]
    tags = {
        "arn:orders": {
            "benzene": "true",
            "benzene:mesh-url": "https://orders.example.com",
            "benzene:mesh-path": "/internal/benzene",
        },
        "arn:billing": {"team": "finance"},  # untagged for the mesh → excluded
        "arn:payments": {"benzene": "true", "benzene:mesh-url": "https://payments.example.com"},
    }
    return _FakeLambda([functions], tags)


def test_only_benzene_tagged_functions_become_entries() -> None:
    registry = AwsLambdaDiscoveryProvider(_account()).discover()
    assert [e.name for e in registry.services] == ["orders", "payments"]  # billing excluded, source order


def test_mesh_url_tag_becomes_the_http_base_url() -> None:
    registry = AwsLambdaDiscoveryProvider(_account()).discover()
    payments = next(e for e in registry.services if e.name == "payments")
    assert payments.base_url == "https://payments.example.com"
    # Default prefix → the profile's well-known /benzene/spec path off that base.
    assert payments.resolved_spec_url() == "https://payments.example.com/benzene/spec"
    assert payments.resolved_health_url() == "https://payments.example.com/benzene/health"


def test_mesh_path_tag_overrides_the_descriptor_prefix() -> None:
    registry = AwsLambdaDiscoveryProvider(_account()).discover()
    orders = next(e for e in registry.services if e.name == "orders")
    assert orders.prefix == "/internal/benzene"
    assert orders.resolved_spec_url() == "https://orders.example.com/internal/benzene/spec"


def test_pagination_marker_is_followed() -> None:
    client = _FakeLambda(
        pages=[
            [{"FunctionName": "a", "FunctionArn": "arn:a"}],
            [{"FunctionName": "b", "FunctionArn": "arn:b"}],
        ],
        tags={
            "arn:a": {"benzene": "true", "benzene:mesh-url": "https://a"},
            "arn:b": {"benzene": "true", "benzene:mesh-url": "https://b"},
        },
    )
    registry = AwsLambdaDiscoveryProvider(client).discover()
    assert [e.name for e in registry.services] == ["a", "b"]
    # Page one (marker None) then page two (marker "1"), and every function's tags were read once.
    assert client.list_functions_markers == [None, "1"]
    assert sorted(client.list_tags_resources) == ["arn:a", "arn:b"]


def test_valued_filter_matches_the_tag_value_exactly() -> None:
    client = _FakeLambda(
        pages=[
            [
                {"FunctionName": "prod", "FunctionArn": "arn:prod"},
                {"FunctionName": "staging", "FunctionArn": "arn:staging"},
            ]
        ],
        tags={
            "arn:prod": {"benzene": "true", "benzene:mesh-env": "prod", "benzene:mesh-url": "https://p"},
            "arn:staging": {"benzene": "true", "benzene:mesh-env": "staging", "benzene:mesh-url": "https://s"},
        },
    )
    filt = MeshDiscoveryFilter(required_tags={"benzene": None, "benzene:mesh-env": "prod"})
    registry = AwsLambdaDiscoveryProvider(client, filter=filt).discover()
    assert [e.name for e in registry.services] == ["prod"]


def test_empty_account_discovers_nothing() -> None:
    registry = AwsLambdaDiscoveryProvider(_FakeLambda(pages=[[]], tags={})).discover()
    assert registry.services == []


def test_mesh_tagged_function_without_a_url_still_appears_but_is_unreachable() -> None:
    client = _FakeLambda(
        pages=[[{"FunctionName": "ghost", "FunctionArn": "arn:ghost"}]],
        tags={"arn:ghost": {"benzene": "true"}},  # marked for the mesh, but no HTTP base URL
    )
    registry = AwsLambdaDiscoveryProvider(client).discover()
    ghost = registry.services[0]
    assert ghost.name == "ghost" and ghost.base_url is None
    # With no base URL the aggregator cannot reach it — resolving a spec URL is an explicit error, not a
    # silent bad URL, which is the honest "discovered but unreachable" state.
    try:
        ghost.resolved_spec_url()
    except ValueError as exc:
        assert "ghost" in str(exc)
    else:  # pragma: no cover - the assertion above must raise
        raise AssertionError("expected resolved_spec_url() to raise without a base_url")


def test_boto3_client_is_lazy_and_needs_no_boto3_to_construct() -> None:
    # Constructing the real adapter must not import boto3 (that only happens on first call), so the [aws]
    # extra stays truly optional — mirrors the X-Ray/CloudWatch Boto3*Client seams.
    adapter = Boto3LambdaClient(client=None)
    assert isinstance(adapter, Boto3LambdaClient)
