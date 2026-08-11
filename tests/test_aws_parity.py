"""AWS transport-parity bindings (roadmap item 19) — the sources beyond API Gateway/SQS/SNS.

Covers the native decoders (S3, EventBridge, DynamoDB Streams, Kinesis, Kafka/MSK) lifting the right
topic/headers/body from realistic fake events; the host dispatching one invocation per record and
reporting failures per each source's Lambda contract (partial-batch for DynamoDB/Kinesis, raise for
the channel-less S3/EventBridge/Kafka); and the two new outbound clients forwarding topic + headers
and mapping a client fault to ``service-unavailable``. In-memory only — no boto3, no network.
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import (
    AwsLambdaApp,
    EventBridgeMessageSender,
    KinesisMessageSender,
    dynamodb_record_envelope,
    event_source,
    eventbridge_envelope,
    kafka_record_envelope,
    kafka_records,
    kinesis_record_envelope,
    s3_record_envelope,
)
from benzene.aws.testing import (
    AwsLambdaTestHost,
    DynamoDbStreamBuilder,
    KafkaLambdaEventBuilder,
    KinesisEventBuilder,
)
from benzene.core import MessageHandlingError, Registry
from benzene.results import Result, Status, is_successful


# --- decoders: each lifts the right topic/headers/body --------------------------------------
def test_event_source_classifies_the_new_shapes() -> None:
    assert event_source({"detail-type": "orders.created", "detail": {}}) == "eventbridge"
    assert event_source({"eventSource": "aws:kafka", "records": {}}) == "kafka"
    assert event_source({"records": {"t-0": []}}) == "kafka"
    assert event_source({"Records": [{"eventSource": "aws:s3"}]}) == "s3"
    assert event_source({"Records": [{"eventSource": "aws:dynamodb"}]}) == "dynamodb"
    assert event_source({"Records": [{"eventSource": "aws:kinesis"}]}) == "kinesis"
    assert event_source({"nope": True}) is None


def test_s3_decoder_uses_convention_topic_and_projects_bucket_key_eventname() -> None:
    record = {
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "s3": {"bucket": {"name": "photos"}, "object": {"key": "a/b.jpg"}},
    }
    envelope = s3_record_envelope(record, topic="s3:uploaded")
    assert envelope["topic"] == "s3:uploaded"
    assert envelope["headers"] == {}
    assert json.loads(envelope["body"]) == {
        "bucket": "photos",
        "key": "a/b.jpg",
        "eventName": "ObjectCreated:Put",
    }


def test_eventbridge_decoder_topic_from_detail_type_body_is_detail() -> None:
    event = {"detail-type": "orders.created", "source": "shop", "detail": {"id": "o1"}}
    envelope = eventbridge_envelope(event)
    assert envelope["topic"] == "orders.created"
    assert envelope["headers"] == {}
    assert json.loads(envelope["body"]) == {"id": "o1"}


def test_eventbridge_decoder_falls_back_to_default_topic() -> None:
    envelope = eventbridge_envelope({"detail": {"x": 1}}, default_topic="bus:event")
    assert envelope["topic"] == "bus:event"


def test_dynamodb_decoder_derives_topic_from_eventname_and_carries_the_projection() -> None:
    record = {
        "eventSource": "aws:dynamodb",
        "eventName": "INSERT",
        "dynamodb": {"Keys": {"id": {"S": "1"}}, "NewImage": {"name": {"S": "ada"}}},
    }
    envelope = dynamodb_record_envelope(record)
    assert envelope["topic"] == "dynamodb:insert"
    assert json.loads(envelope["body"]) == {
        "Keys": {"id": {"S": "1"}},
        "NewImage": {"name": {"S": "ada"}},
    }


def test_dynamodb_decoder_honours_an_injected_topic_override() -> None:
    record = {"eventName": "MODIFY", "dynamodb": {"Keys": {}}}
    assert dynamodb_record_envelope(record, topic="orders:changed")["topic"] == "orders:changed"


def test_kinesis_decoder_base64_decodes_the_payload_as_the_body() -> None:
    event = KinesisEventBuilder().with_record({"id": "k1"}).build()
    envelope = kinesis_record_envelope(event["Records"][0], topic="kinesis:orders")
    assert envelope["topic"] == "kinesis:orders"
    assert json.loads(envelope["body"]) == {"id": "k1"}


def test_kafka_decoder_lifts_topic_header_and_decodes_headers_and_value() -> None:
    event = (
        KafkaLambdaEventBuilder()
        .with_message("orders", {"id": "1"}, topic="orders:created", headers={"traceparent": "tp"})
        .build()
    )
    envelope = kafka_record_envelope(kafka_records(event)[0])
    assert envelope["topic"] == "orders:created"
    assert envelope["headers"] == {"traceparent": "tp"}
    assert json.loads(envelope["body"]) == {"id": "1"}


def test_kafka_decoder_falls_back_to_the_kafka_topic_when_no_topic_header() -> None:
    event = KafkaLambdaEventBuilder().with_message("payments", {"amount": 5}).build()
    assert kafka_record_envelope(kafka_records(event)[0])["topic"] == "payments"


# --- host dispatch: one invocation per record + correct failure reporting -------------------
def _recording_host(topic: str, ok: bool = True, **kwargs) -> tuple[AwsLambdaTestHost, list]:
    seen: list = []

    async def handler(request: object) -> Result:
        seen.append(request)
        return Result.ok() if ok else Result.not_found("gone")

    app = AwsLambdaApp(registry=Registry().register(topic, handler), **kwargs)
    return AwsLambdaTestHost(app), seen


def test_s3_runs_one_invocation_per_record_and_raises_on_failure() -> None:
    host, seen = _recording_host("s3:object-created")
    host.send_s3("bucket", "key.txt")
    assert len(seen) == 1

    failing, _ = _recording_host("s3:object-created", ok=False)
    with pytest.raises(MessageHandlingError):
        failing.send_s3("bucket", "key.txt")


def test_eventbridge_single_invocation_and_raises_on_failure() -> None:
    host, seen = _recording_host("orders.created")
    host.send_eventbridge("orders.created", {"id": "o1"})
    assert seen == [{"id": "o1"}]

    failing, _ = _recording_host("orders.created", ok=False)
    with pytest.raises(MessageHandlingError):
        failing.send_eventbridge("orders.created", {"id": "o1"})


def test_kafka_runs_one_invocation_per_record_and_raises_on_failure() -> None:
    host, seen = _recording_host("orders:created")
    host.send_kafka("orders", {"id": "1"}, topic="orders:created")
    assert seen == [{"id": "1"}]

    failing, _ = _recording_host("orders:created", ok=False)
    with pytest.raises(MessageHandlingError):
        failing.send_kafka("orders", {"id": "1"}, topic="orders:created")


def test_dynamodb_reports_only_failed_records_via_partial_batch() -> None:
    seen: list = []

    async def handler(request: dict) -> Result:
        seen.append(request)
        # Fail the MODIFY record only, keyed by its sequence number below.
        return Result.not_found("nope") if request["Keys"]["id"]["S"] == "bad" else Result.ok()

    app = AwsLambdaApp(registry=Registry().register("dynamodb:insert", handler))
    event = (
        DynamoDbStreamBuilder()
        .with_record("INSERT", {"id": {"S": "ok"}}, sequence_number="seq-ok")
        .with_record("INSERT", {"id": {"S": "bad"}}, sequence_number="seq-bad")
        .build()
    )
    response = app.handle(event)
    assert response == {"batchItemFailures": [{"itemIdentifier": "seq-bad"}]}
    assert len(seen) == 2  # both records were invoked; only one is reported for reprocessing


def test_kinesis_reports_only_failed_records_via_partial_batch() -> None:
    async def handler(request: dict) -> Result:
        return Result.not_found("nope") if request["id"] == "bad" else Result.ok()

    app = AwsLambdaApp(registry=Registry().register("kinesis:record", handler))
    event = (
        KinesisEventBuilder()
        .with_record({"id": "ok"}, sequence_number="seq-ok")
        .with_record({"id": "bad"}, sequence_number="seq-bad")
        .build()
    )
    response = app.handle(event)
    assert response == {"batchItemFailures": [{"itemIdentifier": "seq-bad"}]}


def test_configured_conventions_reach_the_decoders() -> None:
    seen: list = []

    async def handler(request: object) -> Result:
        seen.append(request)
        return Result.ok()

    app = AwsLambdaApp(
        registry=Registry().register("s3:custom", handler),
        s3_topic="s3:custom",
    )
    AwsLambdaTestHost(app).send_s3("b", "k")
    assert len(seen) == 1  # routed only because the host's s3_topic matched the registration


# --- outbound clients: forward topic + headers; map faults to service-unavailable -----------
class _FakeEvents:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_events(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"FailedEntryCount": 0}


class _FakeKinesis:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def put_record(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"SequenceNumber": "1"}


def test_eventbridge_sender_embeds_topic_headers_and_body_in_the_detail() -> None:
    fake = _FakeEvents()
    result = asyncio.run(
        EventBridgeMessageSender("bus", source="shop", client=fake).send_message(
            "orders:created", {"id": "1"}, headers={"traceparent": "tp"}
        )
    )
    assert is_successful(result.status)
    entry = fake.calls[0]["Entries"][0]
    assert entry["EventBusName"] == "bus"
    assert entry["Source"] == "shop"
    assert entry["DetailType"] == "orders:created"  # topic round-trips as the DetailType
    assert json.loads(entry["Detail"]) == {
        "topic": "orders:created",
        "headers": {"traceparent": "tp"},
        "body": {"id": "1"},
    }


def test_eventbridge_sender_maps_a_put_failure_to_service_unavailable() -> None:
    class Boom:
        def put_events(self, **kwargs):
            raise RuntimeError("eventbridge down")

    result = asyncio.run(EventBridgeMessageSender("bus", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "eventbridge down" in " ".join(result.errors)


def test_kinesis_sender_embeds_the_envelope_and_keys_the_shard() -> None:
    fake = _FakeKinesis()
    result = asyncio.run(
        KinesisMessageSender(
            "stream", partition_key_header="partition-key", client=fake
        ).send_message("orders:created", {"id": "1"}, headers={"partition-key": "cust-9"})
    )
    assert is_successful(result.status)
    call = fake.calls[0]
    assert call["StreamName"] == "stream"
    assert call["PartitionKey"] == "cust-9"  # taken from the configured header
    assert json.loads(call["Data"]) == {
        "topic": "orders:created",
        "headers": {"partition-key": "cust-9"},
        "body": {"id": "1"},
    }


def test_kinesis_sender_partition_key_defaults_to_the_topic() -> None:
    fake = _FakeKinesis()
    asyncio.run(
        KinesisMessageSender("stream", client=fake).send_message("orders:created", {"id": "1"})
    )
    assert fake.calls[0]["PartitionKey"] == "orders:created"


def test_kinesis_sender_maps_a_put_failure_to_service_unavailable() -> None:
    class Boom:
        def put_record(self, **kwargs):
            raise RuntimeError("kinesis down")

    result = asyncio.run(KinesisMessageSender("stream", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "kinesis down" in " ".join(result.errors)
