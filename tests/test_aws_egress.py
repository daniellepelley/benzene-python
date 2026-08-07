"""AWS outbound clients (SNS, SQS) — the egress wire contract, the mirror of the inbound decode.

Each sender forwards the Benzene topic + headers onto the native message-attribute channel (so
correlation/trace propagation survives the hop) and serializes the body through the shared wire
policy. The harness's ``FakeMessageSender`` bypasses these publish paths, so they need direct cover:
the native call shape, the topic/header tagging, and the ``except -> service-unavailable`` mapping.
Each client takes an injected fake, so this is credential-free — no boto3.
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import TOPIC_ATTRIBUTE, SnsMessageSender, SqsMessageSender
from benzene.core import encode_body
from benzene.results import Status, is_successful


class _FakeSns:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"MessageId": "m1"}


class _FakeSqs:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def send_message(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"MessageId": "m1"}


def test_sns_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeSns()
    result = asyncio.run(
        SnsMessageSender("arn:topic", client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["TopicArn"] == "arn:topic"
    assert call["Message"] == encode_body({"id": "1"})  # the shared wire policy, not asdict
    attrs = call["MessageAttributes"]
    assert attrs[TOPIC_ATTRIBUTE] == {"DataType": "String", "StringValue": "orders:created"}
    assert attrs["traceparent"] == {"DataType": "String", "StringValue": "tp"}


def test_sqs_sender_tags_topic_propagates_headers_and_serializes_body() -> None:
    fake = _FakeSqs()
    result = asyncio.run(
        SqsMessageSender("q-url", client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["QueueUrl"] == "q-url"
    assert call["MessageBody"] == encode_body({"id": "1"})
    attrs = call["MessageAttributes"]
    assert attrs[TOPIC_ATTRIBUTE]["StringValue"] == "orders:created"
    assert attrs["traceparent"]["StringValue"] == "tp"


def test_sns_sender_maps_a_publish_failure_to_service_unavailable() -> None:
    class Boom:
        def publish(self, **kwargs):
            raise RuntimeError("sns down")

    result = asyncio.run(SnsMessageSender("arn", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "sns down" in " ".join(result.errors)


def test_sqs_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def send_message(self, **kwargs):
            raise RuntimeError("sqs down")

    result = asyncio.run(SqsMessageSender("q", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "sqs down" in " ".join(result.errors)
