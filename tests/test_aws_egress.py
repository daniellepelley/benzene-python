"""AWS outbound clients (SNS, SQS) — the egress wire contract, the mirror of the inbound decode.

Each sender forwards the Benzene topic + headers onto the native message-attribute channel (so
correlation/trace propagation survives the hop) and serializes the body through the shared wire
policy. The harness's ``FakeMessageSender`` bypasses these publish paths, so they need direct cover:
the native call shape, the topic/header tagging, and the ``except -> service-unavailable`` mapping.
Each client takes an injected fake, so this is credential-free — no boto3.
"""

from __future__ import annotations

import asyncio
import sys

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import (
    TOPIC_ATTRIBUTE,
    EventBridgeMessageSender,
    KinesisMessageSender,
    LambdaMessageSender,
    SnsMessageSender,
    SqsMessageSender,
)
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


# --- a missing SDK is a deployment error, not a message outcome (D1) ----------------------------


@pytest.mark.parametrize(
    ("name", "make_sender"),
    [
        ("SnsMessageSender", lambda: SnsMessageSender("arn:topic")),
        ("SqsMessageSender", lambda: SqsMessageSender("q-url")),
        ("EventBridgeMessageSender", lambda: EventBridgeMessageSender("bus")),
        ("KinesisMessageSender", lambda: KinesisMessageSender("stream")),
        ("LambdaMessageSender", lambda: LambdaMessageSender("fn")),
    ],
)
def test_a_missing_boto3_raises_a_teaching_import_error_out_of_send_message(
    monkeypatch: pytest.MonkeyPatch, name: str, make_sender
) -> None:
    # Without the guard the lazy ``import boto3`` is swallowed by the sender's ``except Exception``
    # mapper and every publish quietly becomes service-unavailable — which retry middleware and
    # circuit breakers then hammer. A missing extra must escape as a teaching ImportError instead.
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ImportError) as excinfo:
        asyncio.run(make_sender().send_message("orders:created", {"id": "1"}))
    message = str(excinfo.value)
    assert name in message
    assert "boto3" in message
    assert "pip install benzene-aws[boto3]" in message
