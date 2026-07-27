"""Dogfooded, in-memory tests for the AWS orders example.

Drives the real Lambda bindings via ``benzene.aws.testing`` and fakes only the outbound edge.
Covers all three event sources (API Gateway, SQS, SNS), ingress → handler → egress, and the SQS
partial-batch-failure protocol.
"""

from __future__ import annotations

import json

from benzene.aws.testing import AwsLambdaTestHost, SqsEventBuilder
from benzene.testing import FakeMessageSender

from aws_orders import build_aws_orders_app
from orders_domain import ORDER_CREATED_TOPIC, OrderService


def make_host() -> tuple[AwsLambdaTestHost, OrderService, FakeMessageSender, list[str]]:
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []
    app = build_aws_orders_app(service, sender, seen)
    return AwsLambdaTestHost(app), service, sender, seen


def test_api_gateway_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["sku"] == "ABC"
    assert sender.last_topic == ORDER_CREATED_TOPIC        # ingress -> handler -> egress
    assert sender.last_message.id == order["id"]
    assert order["id"] in service.orders


def test_api_gateway_get_order_round_trip() -> None:
    host, _, _, _ = make_host()
    created = json.loads(host.send_http("POST", "/orders", body={"sku": "XYZ"}).body)
    response = host.send_http("GET", f"/orders/{created['id']}")
    assert response.status_code == 200
    assert json.loads(response.body)["id"] == created["id"]


def test_sqs_order_created_is_handled() -> None:
    host, _, _, seen = make_host()
    result = host.send_sqs(ORDER_CREATED_TOPIC, {"id": "ord-sqs", "sku": "ABC"})
    assert result == {"batchItemFailures": []}
    assert seen == ["ord-sqs"]


def test_sns_order_created_is_handled() -> None:
    host, _, _, seen = make_host()
    host.send_sns(ORDER_CREATED_TOPIC, {"id": "ord-sns", "sku": "ABC"})
    assert seen == ["ord-sns"]


def test_sqs_partial_batch_failure_reports_only_failed_record() -> None:
    host, _, _, seen = make_host()
    event = (
        SqsEventBuilder()
        .with_message(ORDER_CREATED_TOPIC, {"id": "ok-1", "sku": "A"}, message_id="m1")
        .with_message("orders:unknown", {}, message_id="m2")     # no handler -> not-found -> fails
        .build()
    )
    result = host.send_sqs_event(event)
    assert result == {"batchItemFailures": [{"itemIdentifier": "m2"}]}
    assert seen == ["ok-1"]                                       # the good record still processed
