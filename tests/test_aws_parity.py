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
    LambdaMessageSender,
    dynamodb_record_envelope,
    event_source,
    eventbridge_envelope,
    invoke_envelope,
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


# --- direct invoke: one AWS Lambda calling another via lambda.invoke() -----------------------


def test_event_source_classifies_a_bare_envelope_as_invoke() -> None:
    assert event_source({"topic": "orders:place", "headers": {}, "body": "{}"}) == "invoke"
    assert event_source({"topic": ""}) == "invoke"  # still recognised; the router rejects it deeper
    # Every other source's own marker is checked first, so none of them is mistaken for an invoke.
    assert event_source({"detail-type": "orders.created", "detail": {}}) == "eventbridge"


def test_invoke_envelope_normalizes_absent_fields() -> None:
    assert invoke_envelope({"topic": "t"}) == {"topic": "t", "headers": {}, "body": ""}
    assert invoke_envelope({}) == {"topic": "", "headers": {}, "body": ""}


def test_invoke_is_synchronous_like_api_gateway_and_returns_the_response_envelope() -> None:
    async def place(request: dict) -> Result:
        return Result.created({"id": "o1", "sku": request["sku"]})

    app = AwsLambdaApp(registry=Registry().register("orders:place", place))
    host = AwsLambdaTestHost(app)

    result = host.send_invoke("orders:place", {"sku": "ABC"})
    assert result.status == Status.CREATED
    assert result.payload == {"id": "o1", "sku": "ABC"}


def test_invoke_failure_decodes_status_and_errors_not_a_raise() -> None:
    async def place(_request: dict) -> Result:
        return Result.bad_request("sku is required")

    app = AwsLambdaApp(registry=Registry().register("orders:place", place))
    result = AwsLambdaTestHost(app).send_invoke("orders:place", {})
    assert result.status == Status.BAD_REQUEST
    assert result.errors == ("sku is required",)


class _FakeLambdaPayload:
    """A stand-in for the ``StreamingBody`` boto3 hands back as ``response["Payload"]``."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeLambdaClient:
    """A boto3 ``lambda`` client stand-in whose ``invoke`` replays a scripted response."""

    def __init__(self, response: dict) -> None:
        self._response = response
        self.calls: list[dict] = []

    def invoke(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return self._response


def _invoke_response(payload: dict, *, function_error: str | None = None) -> dict:
    response = {"StatusCode": 200, "Payload": _FakeLambdaPayload(json.dumps(payload).encode())}
    if function_error:
        response["FunctionError"] = function_error
    return response


def test_lambda_sender_decodes_a_successful_response_envelope() -> None:
    envelope = {
        "statusCode": Status.CREATED,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"id": "o1"}),
    }
    fake = _FakeLambdaClient(_invoke_response(envelope))
    result = asyncio.run(
        LambdaMessageSender("target-fn", client=fake).send_message(
            "orders:place", {"sku": "ABC"}, headers={"traceparent": "tp"}
        )
    )
    assert result.status == Status.CREATED
    assert result.payload == {"id": "o1"}

    call = fake.calls[0]
    assert call["FunctionName"] == "target-fn"
    assert call["InvocationType"] == "RequestResponse"
    sent = json.loads(call["Payload"].decode("utf-8"))
    assert sent == {
        "topic": "orders:place",
        "headers": {"traceparent": "tp"},
        "body": '{"sku": "ABC"}',
    }


def test_lambda_sender_decodes_a_failure_response_envelope() -> None:
    envelope = {
        "statusCode": Status.NOT_FOUND,
        "body": json.dumps({"status": "not-found", "detail": "no such order"}),
    }
    fake = _FakeLambdaClient(_invoke_response(envelope))
    result = asyncio.run(
        LambdaMessageSender("target-fn", client=fake).send_message("orders:get", {})
    )
    assert result.status == Status.NOT_FOUND
    assert result.errors == ("no such order",)


def test_lambda_sender_maps_a_function_error_to_service_unavailable() -> None:
    # The target Lambda itself faulted (an unhandled exception) — AWS's own error shape, never a
    # Benzene envelope.
    fake = _FakeLambdaClient(
        _invoke_response(
            {"errorMessage": "boom", "errorType": "ValueError"}, function_error="Unhandled"
        )
    )
    result = asyncio.run(LambdaMessageSender("target-fn", client=fake).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "boom" in " ".join(result.errors)


def test_lambda_sender_maps_an_invoke_exception_to_service_unavailable() -> None:
    class Boom:
        def invoke(self, **kwargs):
            raise RuntimeError("lambda invoke failed")

    result = asyncio.run(LambdaMessageSender("target-fn", client=Boom()).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE
    assert "lambda invoke failed" in " ".join(result.errors)


def test_lambda_sender_maps_a_non_benzene_response_to_service_unavailable_not_a_crash() -> None:
    # The target answered but isn't a Benzene function (returned a bare string, not an envelope).
    fake = _FakeLambdaClient(
        {"StatusCode": 200, "Payload": _FakeLambdaPayload(b'"not an envelope"')}
    )
    result = asyncio.run(LambdaMessageSender("target-fn", client=fake).send_message("t", {}))
    assert result.status == Status.SERVICE_UNAVAILABLE


def test_lambda_sender_event_invocation_is_accepted_without_decoding_a_response() -> None:
    fake = _FakeLambdaClient({"StatusCode": 202, "Payload": _FakeLambdaPayload(b"")})
    result = asyncio.run(
        LambdaMessageSender("target-fn", client=fake, invocation_type="Event").send_message("t", {})
    )
    assert result.status == Status.ACCEPTED
    assert fake.calls[0]["InvocationType"] == "Event"


def test_lambda_sender_passes_a_qualifier_when_given() -> None:
    fake = _FakeLambdaClient(_invoke_response({"statusCode": Status.OK, "body": ""}))
    asyncio.run(
        LambdaMessageSender("target-fn", client=fake, qualifier="live").send_message("t", {})
    )
    assert fake.calls[0]["Qualifier"] == "live"


def test_lambda_to_lambda_round_trip_with_no_aws_at_all() -> None:
    """The full story: a fake ``lambda.invoke()`` that actually dispatches to a second, independent
    ``AwsLambdaApp`` — proving a real Lambda-to-Lambda call end to end without any boto3/AWS."""

    async def get_order(request: dict) -> Result:
        return (
            Result.ok({"id": request["id"], "sku": "ABC"})
            if request["id"] == "o1"
            else Result.not_found()
        )

    target_app = AwsLambdaApp(registry=Registry().register("orders:get", get_order))

    class FakeLambdaService:
        """Stands in for AWS: routes an ``invoke()`` Payload straight to the target function."""

        def invoke(
            self, *, FunctionName: str, InvocationType: str, Payload: bytes, **_kwargs
        ) -> dict:
            event = json.loads(Payload.decode("utf-8"))
            response = target_app.handle(event)
            return {
                "StatusCode": 200,
                "Payload": _FakeLambdaPayload(json.dumps(response).encode("utf-8")),
            }

    sender = LambdaMessageSender("orders-service", client=FakeLambdaService())
    result = asyncio.run(sender.send_message("orders:get", {"id": "o1"}))
    assert result.is_successful
    assert result.payload == {"id": "o1", "sku": "ABC"}

    missing = asyncio.run(sender.send_message("orders:get", {"id": "o404"}))
    assert missing.status == Status.NOT_FOUND
