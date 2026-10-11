"""Dogfooded tests for the versioning example — Mechanism A, handler-version dispatch.

The topic ``order:create`` has a V1 and a V2 handler; the incoming ``benzene-version`` metadata
selects which one runs. V1 is declared the topic's default, so a message with no version reaches it.
Driven through the transport-neutral front door
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


def test_no_version_goes_to_the_declared_default_v1() -> None:
    host, log = make_host()

    # No version signalled: the topic's declared default handler (V1) runs.
    response = asyncio.run(
        host.send_message(ORDER_CREATE_TOPIC, body={"customer_name": "Jo Bloggs", "quantity": 1})
    )

    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v1"
    assert any(e.startswith("order:create v1") for e in log.entries)


def test_a_plain_version_header_is_not_a_version() -> None:
    host, log = make_host()

    # A producer's own "version" header (an SDK or API version) is an ordinary header: it selects
    # nothing, so the message reaches the default handler, not V2 (wire-contracts §2).
    response = asyncio.run(
        host.send_message(
            ORDER_CREATE_TOPIC,
            body={"customer_name": "Plain", "quantity": 2},
            headers={"version": V2},
        )
    )

    accepted = json.loads(response["body"])
    assert accepted["handledBy"] == "create_order_v1"


def test_an_unknown_version_is_not_found() -> None:
    host, _ = make_host()

    response = asyncio.run(
        host.send_message(ORDER_CREATE_TOPIC, body={}, headers={VERSION_HEADER: "v9"})
    )

    assert response["statusCode"] == "not-found"  # no nearest/highest fallback
