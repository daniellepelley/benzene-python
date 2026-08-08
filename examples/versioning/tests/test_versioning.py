"""Dogfooded tests for the versioning example — Mechanism A, handler-version dispatch.

The topic ``order:create`` has a V1 and a V2 handler; the incoming ``benzene-version`` metadata
selects which one runs. When no version is signalled the router falls back to the highest registered
version (V2), so V2 is the topic's default handler. Driven through the transport-neutral front door
(``InMemoryBenzeneHost`` from ``benzene.testing``), version travelling as a header — never in the
body — exactly as a real transport delivers it.
"""

from __future__ import annotations

import asyncio
import json

from benzene.core import VERSION_HEADER
from benzene.testing import InMemoryBenzeneHost

from versioning import ORDER_CREATE_TOPIC, V1, V2, ProcessedLog, build_versioning_app


def make_host():
    log = ProcessedLog()
    host = InMemoryBenzeneHost(build_versioning_app(log))
    return host, log


def test_v1_header_routes_to_the_v1_handler() -> None:
    host, log = make_host()

    response = asyncio.run(
        host.send_message(
            ORDER_CREATE_TOPIC,
            body={"customer_name": "Jo Bloggs", "quantity": 3},
            headers={VERSION_HEADER: V1},
        )
    )

    assert response["statusCode"] == "ok"
    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v1"
    assert any(e.startswith("order:create v1") for e in log.entries)
    # The version the request declared is echoed back on the response (wire-contracts §2.1).
    assert response["headers"][VERSION_HEADER] == V1


def test_v2_header_routes_to_the_v2_handler() -> None:
    host, log = make_host()

    response = asyncio.run(
        host.send_message(
            ORDER_CREATE_TOPIC,
            body={"first_name": "Jo", "last_name": "Bloggs", "quantity": 3, "currency": "GBP"},
            headers={VERSION_HEADER: V2},
        )
    )

    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v2"
    assert accepted["currency"] == "GBP"
    assert any(e.startswith("order:create v2") for e in log.entries)


def test_no_version_falls_back_to_the_highest_version_handler_v2() -> None:
    host, log = make_host()

    # No version signalled: highest_version picks the natural-max registered version, "v2".
    response = asyncio.run(
        host.send_message(
            ORDER_CREATE_TOPIC,
            body={"first_name": "Jo", "last_name": "Bloggs", "quantity": 1, "currency": "USD"},
        )
    )

    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v2"
    assert any(e.startswith("order:create v2") for e in log.entries)


def test_fallback_version_header_selects_the_handler() -> None:
    host, log = make_host()

    # A peer may send the version under the fallback name "version"; resolve_version still finds it,
    # so v1 routes to the V1 handler (versioning.md §2).
    response = asyncio.run(
        host.send_message(
            ORDER_CREATE_TOPIC,
            body={"customer_name": "Fallback", "quantity": 2},
            headers={"version": V1},
        )
    )

    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v1"
    assert any(e.startswith("order:create v1") for e in log.entries)
