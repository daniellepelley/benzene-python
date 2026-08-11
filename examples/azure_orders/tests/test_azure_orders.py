"""Dogfooded, in-memory tests for the Azure orders example.

Drives the real Azure Functions bindings via ``benzene.azure.testing`` and fakes only the outbound
edge. Covers HTTP, Service Bus, Event Hub, and Event Grid (native + CloudEvents 1.0) ingress,
ingress → handler → egress, and the failure/retry rule.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from benzene.azure.testing import event_grid_cloud_event, event_grid_event
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def make_host():
    """Boot the real app from OrdersStartUp, fake only the edges, specialize to Azure.

    Identical to the GCP and AWS suites except the single ``.build_azure()`` call and the ``send_*``
    shape — the test-champion consistency law in action.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_azure()
    return host, service, sender, seen


def test_http_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["sku"] == "ABC"
    assert sender.last_topic == ORDER_CREATED_TOPIC  # ingress -> handler -> egress
    assert sender.last_message.id == order["id"]
    assert order["id"] in service.orders


def test_http_get_order_round_trip() -> None:
    host, _, _, _ = make_host()
    created = json.loads(host.send_http("POST", "/orders", body={"sku": "XYZ"}).body)
    response = host.send_http("GET", f"/orders/{created['id']}")
    assert response.status_code == 200
    assert json.loads(response.body)["id"] == created["id"]


def test_service_bus_order_created_is_handled() -> None:
    host, _, _, seen = make_host()
    host.send_service_bus(ORDER_CREATED_TOPIC, {"id": "ord-sb", "sku": "ABC"})
    assert seen == ["ord-sb"]


def test_event_hub_batch_is_handled_per_event() -> None:
    host, _, _, seen = make_host()
    from benzene.azure.testing import event_hub_event

    host.send_event_hub_batch(
        [
            event_hub_event(ORDER_CREATED_TOPIC, {"id": "e1", "sku": "A"}),
            event_hub_event(ORDER_CREATED_TOPIC, {"id": "e2", "sku": "B"}),
        ]
    )
    assert seen == ["e1", "e2"]


@pytest.mark.parametrize(
    "make_event",
    [event_grid_event, event_grid_cloud_event],
    ids=["native", "cloudevents"],
)
def test_event_grid_order_created_is_handled(
    make_event: Callable[[str, Any], dict[str, Any]],
) -> None:
    """Event Grid delivers to the same subscriber under *both* schemas it supports.

    The event type (native ``eventType`` / CloudEvents ``type``) is the Benzene topic, so an event
    typed ``orders:created`` lands on the shared subscriber whether it arrives in the native Event
    Grid schema or the CloudEvents 1.0 schema — the marquee feature of the binding.
    """
    host, _, _, seen = make_host()
    host.send_event_grid(make_event(ORDER_CREATED_TOPIC, {"id": "ord-eg", "sku": "ABC"}))
    assert seen == ["ord-eg"]


def test_service_bus_unroutable_topic_raises_for_retry() -> None:
    host, _, _, _ = make_host()
    with pytest.raises(RuntimeError):
        host.send_service_bus("orders:unknown", {})
