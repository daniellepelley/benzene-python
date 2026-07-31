"""Handler-version selection over the inbound version-header fallback list (versioning.md §2).

A message carries its version in one of an ordered list of headers — ``benzene-version`` (canonical),
then ``version``, then ``x-version`` — so a peer in any language reaches the right versioned handler.
Absent from all of them, the unversioned handler serves the request.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from benzene.core import (
    VERSION_HEADER_NAMES,
    BenzeneMessageApplication,
    Registry,
    resolve_version,
)
from benzene.results import Result


def _app() -> BenzeneMessageApplication:
    async def v1(_request: dict) -> Result:
        return Result.ok({"handler": "v1"})

    async def v2(_request: dict) -> Result:
        return Result.ok({"handler": "v2"})

    registry = Registry()
    registry.register("orders:get", v1)  # unversioned (version "")
    registry.register("orders:get", v2, version="v2")
    return BenzeneMessageApplication(registry)


def _run(headers: dict[str, str]) -> str:
    response = asyncio.run(
        _app().handle({"topic": "orders:get", "headers": headers, "body": "{}"})
    )
    return json.loads(response["body"])["handler"]


def test_default_header_names_are_the_spec_list() -> None:
    assert VERSION_HEADER_NAMES == ("benzene-version", "version", "x-version")


@pytest.mark.parametrize("header", ["benzene-version", "version", "x-version"])
def test_any_fallback_header_selects_the_versioned_handler(header: str) -> None:
    assert _run({header: "v2"}) == "v2"


def test_absent_version_selects_the_unversioned_handler() -> None:
    assert _run({}) == "v1"


def test_canonical_header_wins_over_fallbacks() -> None:
    # benzene-version is first in the list, so it takes precedence over `version` / `x-version`.
    assert _run({"benzene-version": "v2", "version": "", "x-version": ""}) == "v2"


def test_resolve_version_is_order_sensitive() -> None:
    assert resolve_version({"x-version": "v2"}) == "v2"
    assert resolve_version({"version": "a", "x-version": "b"}) == "a"
    assert resolve_version({}) == ""
