"""Dogfooded, in-memory tests for the six aws_lambda_mesh domain services.

Boots the real app from ``ServiceStartUp`` (the same composition root ``service/host.py`` deploys)
through the shared AWS test harness (``create_test_host(...).build_aws()``), faking only the outbound
edge (``FakeMessageSender``) — proving ingress -> handler -> egress over the *native* AWS event shapes
(API Gateway, SQS, SNS, EventBridge) each service actually receives in deployment, plus the two reserved
topics (``benzene:mesh`` / ``benzene:healthcheck``) the mesh's direct-invoke interrogation depends on.
No cloud, no network: the same test the CI gate runs.
"""

from __future__ import annotations

import json

from benzene.core import HEALTH_TOPIC, MessageSender
from benzene.mesh import MESH_TOPIC
from benzene.testing import FakeMessageSender, create_test_host

from aws_lambda_mesh.service.domain import (
    ORDER_CREATE_TOPIC,
    ORDER_PLACED_TOPIC,
    PAYMENT_CAPTURED_TOPIC,
    PAYMENTS_CAPTURE_TOPIC,
    SHIPMENT_DISPATCHED_TOPIC,
    SHIPPING_BOOK_TOPIC,
)
from aws_lambda_mesh.service.startup import ServiceStartUp


def make_host(service_name: str):
    sender = FakeMessageSender()

    def overrides(services):
        services.add_instance(MessageSender, sender)

    host = create_test_host(ServiceStartUp(service_name)).with_services(overrides).build_aws()
    return host, sender


# --- orders: API Gateway ingress -> SQS + SNS egress -----------------------------------------------


def test_orders_create_order_sends_payments_capture_and_order_placed() -> None:
    host, sender = make_host("orders")

    response = host.send_http("POST", "/orders", body={"orderId": "order-1"})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["status"] == "created"
    assert order["orderId"] == "order-1"
    topics = [sent.topic for sent in sender.sent]
    assert topics == [PAYMENTS_CAPTURE_TOPIC, ORDER_PLACED_TOPIC]
    assert all(sent.message.order_id == "order-1" for sent in sender.sent)


def test_orders_mints_an_order_id_when_none_given() -> None:
    host, _ = make_host("orders")
    response = host.send_http("POST", "/orders", body={})
    assert json.loads(response.body)["orderId"]


# --- payments: SQS ingress -> SQS + EventBridge egress ----------------------------------------------


def test_payments_capture_sends_shipping_book_and_payment_captured() -> None:
    host, sender = make_host("payments")

    response = host.send_sqs(PAYMENTS_CAPTURE_TOPIC, {"orderId": "order-2"})

    assert response.batch_item_failures == []
    topics = [sent.topic for sent in sender.sent]
    assert topics == [SHIPPING_BOOK_TOPIC, PAYMENT_CAPTURED_TOPIC]
    assert all(sent.message.order_id == "order-2" for sent in sender.sent)


# --- shipping: SQS ingress -> EventBridge egress, terminal in this direction ------------------------


def test_shipping_book_sends_shipment_dispatched() -> None:
    host, sender = make_host("shipping")

    response = host.send_sqs(SHIPPING_BOOK_TOPIC, {"orderId": "order-3"})

    assert response.batch_item_failures == []
    assert len(sender.sent) == 1
    assert sender.sent[0].topic == SHIPMENT_DISPATCHED_TOPIC
    assert sender.sent[0].message.order_id == "order-3"


# --- terminal consumers: SNS + EventBridge ingress, no egress ---------------------------------------


def test_inventory_consumes_order_placed_sns_and_shipment_dispatched_eventbridge() -> None:
    host, sender = make_host("inventory")

    host.send_sns(ORDER_PLACED_TOPIC, {"orderId": "order-4"})
    host.send_eventbridge(SHIPMENT_DISPATCHED_TOPIC, {"orderId": "order-5"})

    assert not sender.sent  # a pure consumer sends nothing downstream


def test_notifications_consumes_all_three_fan_out_topics() -> None:
    host, sender = make_host("notifications")

    host.send_sns(ORDER_PLACED_TOPIC, {"orderId": "order-6"})
    host.send_eventbridge(PAYMENT_CAPTURED_TOPIC, {"orderId": "order-6"})
    host.send_eventbridge(SHIPMENT_DISPATCHED_TOPIC, {"orderId": "order-6"})

    assert not sender.sent


def test_analytics_consumes_payment_captured_and_shipment_dispatched() -> None:
    host, sender = make_host("analytics")

    host.send_eventbridge(PAYMENT_CAPTURED_TOPIC, {"orderId": "order-7"})
    host.send_eventbridge(SHIPMENT_DISPATCHED_TOPIC, {"orderId": "order-7"})

    assert not sender.sent


# --- the mesh's own interrogation surfaces: direct invoke on the two reserved topics ----------------


def test_benzene_mesh_topic_answers_a_direct_invoke_with_the_service_descriptor() -> None:
    host, _ = make_host("shipping")

    result = host.send_invoke(MESH_TOPIC)

    assert result.is_successful
    assert result.payload["service"] == "shipping"
    assert {t["id"] for t in result.payload["topics"]} == {SHIPPING_BOOK_TOPIC}


def test_benzene_healthcheck_topic_answers_a_direct_invoke_healthy() -> None:
    host, _ = make_host("analytics")

    result = host.send_invoke(HEALTH_TOPIC)

    assert result.is_successful
    assert result.payload["isHealthy"] is True
    assert "analytics-self" in result.payload["healthChecks"]


def test_http_standard_paths_also_answer_health_and_spec() -> None:
    """Every service is also fronted by API Gateway (deploy/main.tf) — prove the R3/R5 HTTP surfaces
    answer too, not only the direct-invoke path the mesh actually uses."""
    host, _ = make_host("orders")

    health = host.send_http("GET", "/benzene/health")
    assert health.status_code == 200
    assert json.loads(health.body)["isHealthy"] is True

    spec = host.send_http("GET", "/benzene/spec")
    assert spec.status_code == 200
    assert {t["id"] for t in json.loads(spec.body)["topics"]} == {ORDER_CREATE_TOPIC}
