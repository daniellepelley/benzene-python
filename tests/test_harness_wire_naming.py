"""The test-host body builders serialize through the real wire policy (camelCase), not ``asdict``.

A passing in-memory test must reflect real cross-language traffic: a .NET/Go/TS peer sends a
dataclass field ``order_id`` as ``orderId`` on the wire (``encode_body``), so the test builders must
too — otherwise a raw-dict handler passes its in-memory test and fails against a real peer (the exact
interop break the port exists to prevent). These pin every builder to ``encode_body``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from benzene.core import encode_body
from benzene.testing import MessageBuilder


@dataclass
class PlaceOrder:
    order_id: str
    customer_name: str


def test_in_memory_message_builder_uses_camelcase_wire_naming() -> None:
    envelope = MessageBuilder("orders:place").with_body(PlaceOrder("A1", "Ada")).build()
    body = json.loads(envelope["body"])
    assert body == {"orderId": "A1", "customerName": "Ada"}  # wire policy, not snake_case asdict
    assert envelope["body"] == encode_body(PlaceOrder("A1", "Ada"))


def test_in_memory_builder_passes_a_string_body_through_unchanged() -> None:
    envelope = MessageBuilder("t").with_body('{"already": "encoded"}').build()
    assert envelope["body"] == '{"already": "encoded"}'


def test_raw_dict_bodies_keep_their_keys() -> None:
    # to_jsonable camelCases dataclass *fields* only — a raw dict is the developer's exact bytes.
    envelope = MessageBuilder("t").with_body({"order_id": "A1"}).build()
    assert json.loads(envelope["body"]) == {"order_id": "A1"}


def test_every_cloud_request_builder_uses_camelcase_wire_naming() -> None:
    """AWS / GCP / Azure native builders serialize a dataclass body the same way the live transport does."""
    order = PlaceOrder("A1", "Ada")
    expected = {"orderId": "A1", "customerName": "Ada"}

    aws = pytest.importorskip("benzene.aws.testing")
    assert (
        json.loads(aws.ApiGatewayRequestBuilder("POST", "/orders").with_body(order).build()["body"])
        == expected
    )

    gcp = pytest.importorskip("benzene.gcp.testing")
    assert (
        json.loads(
            gcp.HttpRequestBuilder("POST", "/orders").with_body(order).get_data(as_text=True)
        )
        == expected
    )

    azure = pytest.importorskip("benzene.azure.testing")
    msg = azure.service_bus_message("orders:place", order)
    assert json.loads(msg.get_body().decode("utf-8")) == expected


def test_http_test_host_uses_camelcase_wire_naming() -> None:
    """The in-memory HTTP host encodes a dataclass body the way a real HTTP peer sends it."""
    from benzene.core import message
    from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint
    from benzene.http.testing import HttpTestHost
    from benzene.results import Result

    @http_endpoint("POST", "/orders")
    @message("orders:place")
    async def place(request: dict) -> Result:  # a raw-dict handler sees the literal wire keys
        return Result.ok(request)

    host = HttpTestHost(BenzeneHttpApp(HttpRouter().add(place)))
    response = host.send_http("POST", "/orders", body=PlaceOrder("A1", "Ada"))
    assert json.loads(response.body) == {"orderId": "A1", "customerName": "Ada"}


def test_http_test_host_passes_a_string_body_through_unchanged() -> None:
    from benzene.http.testing import _body

    assert _body('{"already": "encoded"}') == '{"already": "encoded"}'
    assert _body(PlaceOrder("A1", "Ada")) == encode_body(PlaceOrder("A1", "Ada"))
