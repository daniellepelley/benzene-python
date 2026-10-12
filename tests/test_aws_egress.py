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
    assert "sns down" in " ".join(result.messages)


def test_sqs_sender_maps_a_send_failure_to_service_unavailable() -> None:
    class Boom:
        def send_message(self, **kwargs):
            raise RuntimeError("sqs down")

    result = asyncio.run(SqsMessageSender("q", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "sqs down" in " ".join(result.messages)


def test_lambda_sender_maps_a_client_deadline_to_timeout_and_a_dead_endpoint_to_service_unavailable() -> None:
    # transport-bindings §1 (spec 1.1): a call that fails before any reply yields a Result - timeout
    # when the client's own deadline elapsed, otherwise service-unavailable. botocore's exceptions
    # are recognised by name (boto3 is optional, so they cannot be imported here).
    class ReadTimeoutError(Exception):
        pass

    class EndpointConnectionError(Exception):
        pass

    def client_raising(error: Exception):
        class Client:
            def invoke(self, **kwargs):
                raise error

        return Client()

    from benzene.aws import LambdaMessageSender

    timed_out = asyncio.run(
        LambdaMessageSender("fn", client=client_raising(ReadTimeoutError("read timed out")))
        .send_message("t", {})
    )
    assert timed_out.status == Status.TIMEOUT
    unreachable = asyncio.run(
        LambdaMessageSender("fn", client=client_raising(EndpointConnectionError("no endpoint")))
        .send_message("t", {})
    )
    assert unreachable.status == Status.SERVICE_UNAVAILABLE
    assert unreachable.messages == ("no endpoint",)
