"""Native-event builders + test host for the AWS binding (mirrors .NET's ``*.TestHelpers``).

Drive an :class:`~benzene.aws.AwsLambdaApp` with the exact event shapes the bound sources deliver —
API Gateway, SQS, SNS, S3, EventBridge, DynamoDB Streams, Kinesis, and Kafka/MSK — in memory, so an
example's tests dogfood the real bindings with neither AWS nor boto3.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benzene.core import encode_body

from .app import AwsLambdaApp
from .events import TOPIC_ATTRIBUTE


def _b64(payload: Any) -> str:
    """Serialize ``payload`` through the wire policy and base64-encode it (the S3/Kinesis wire form)."""
    return base64.b64encode(encode_body(payload).encode("utf-8")).decode("ascii")


if TYPE_CHECKING:
    from benzene.core import Scope


class ApiGatewayRequestBuilder:
    """Builds an API Gateway (v1 proxy) event."""

    def __init__(self, method: str, path: str) -> None:
        self._method = method.upper()
        self._path = path
        self._headers: dict[str, str] = {}
        self._query: dict[str, str] = {}
        self._body: str | None = None

    def with_header(self, key: str, value: str) -> ApiGatewayRequestBuilder:
        self._headers[key] = value
        return self

    def with_query(self, key: str, value: str) -> ApiGatewayRequestBuilder:
        self._query[key] = value
        return self

    def with_body(self, body: Any) -> ApiGatewayRequestBuilder:
        self._body = encode_body(body)
        return self

    def build(self) -> dict[str, Any]:
        return {
            "httpMethod": self._method,
            "path": self._path,
            "headers": dict(self._headers),
            "queryStringParameters": dict(self._query) or None,
            "body": self._body,
        }


class SqsEventBuilder:
    """Builds an SQS event; add one record per message on a topic."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_message(
        self,
        topic: str,
        body: Any,
        message_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> SqsEventBuilder:
        attrs = {TOPIC_ATTRIBUTE: {"stringValue": topic, "dataType": "String"}}
        for key, value in (headers or {}).items():
            attrs[str(key)] = {"stringValue": str(value), "dataType": "String"}
        self._records.append(
            {
                "eventSource": "aws:sqs",
                "messageId": message_id or f"msg-{len(self._records) + 1}",
                "body": encode_body(body),
                "messageAttributes": attrs,
            }
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class SnsEventBuilder:
    """Builds an SNS event; add one record per message on a topic."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_message(
        self, topic: str, body: Any, headers: dict[str, str] | None = None
    ) -> SnsEventBuilder:
        attrs = {TOPIC_ATTRIBUTE: {"Type": "String", "Value": topic}}
        for key, value in (headers or {}).items():
            attrs[str(key)] = {"Type": "String", "Value": str(value)}
        self._records.append(
            {
                "EventSource": "aws:sns",
                "Sns": {"Message": encode_body(body), "MessageAttributes": attrs},
            }
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class S3EventBuilder:
    """Builds an S3 ``ObjectCreated`` notification; add one record per created object."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_object(
        self, bucket: str, key: str, event_name: str = "ObjectCreated:Put"
    ) -> S3EventBuilder:
        self._records.append(
            {
                "eventSource": "aws:s3",
                "eventName": event_name,
                "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
            }
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class EventBridgeEventBuilder:
    """Builds a (single) EventBridge event with a ``detail-type``, ``source`` and ``detail``."""

    def __init__(self, detail_type: str, source: str = "benzene") -> None:
        self._detail_type = detail_type
        self._source = source
        self._detail: Any = {}

    def with_detail(self, detail: Any) -> EventBridgeEventBuilder:
        self._detail = detail
        return self

    def build(self) -> dict[str, Any]:
        return {
            "detail-type": self._detail_type,
            "source": self._source,
            "detail": self._detail,
        }


class DynamoDbStreamBuilder:
    """Builds a DynamoDB Streams event; add one record per change."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_record(
        self,
        event_name: str,
        keys: dict[str, Any],
        new_image: dict[str, Any] | None = None,
        old_image: dict[str, Any] | None = None,
        sequence_number: str | None = None,
    ) -> DynamoDbStreamBuilder:
        dynamodb: dict[str, Any] = {
            "Keys": keys,
            "SequenceNumber": sequence_number or f"seq-{len(self._records) + 1}",
        }
        if new_image is not None:
            dynamodb["NewImage"] = new_image
        if old_image is not None:
            dynamodb["OldImage"] = old_image
        self._records.append(
            {"eventSource": "aws:dynamodb", "eventName": event_name, "dynamodb": dynamodb}
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class KinesisEventBuilder:
    """Builds a Kinesis Data Streams event; add one record per (base64-encoded) payload."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def with_record(
        self,
        body: Any,
        sequence_number: str | None = None,
        partition_key: str = "pk",
    ) -> KinesisEventBuilder:
        self._records.append(
            {
                "eventSource": "aws:kinesis",
                "kinesis": {
                    "data": _b64(body),
                    "sequenceNumber": sequence_number or f"seq-{len(self._records) + 1}",
                    "partitionKey": partition_key,
                },
            }
        )
        return self

    def build(self) -> dict[str, Any]:
        return {"Records": list(self._records)}


class KafkaLambdaEventBuilder:
    """Builds a Kafka/MSK-via-Lambda event; records are grouped by ``<topic>-<partition>``."""

    def __init__(self) -> None:
        self._records: dict[str, list[dict[str, Any]]] = {}

    def with_message(
        self,
        kafka_topic: str,
        body: Any,
        partition: int = 0,
        offset: int = 0,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> KafkaLambdaEventBuilder:
        """Add one Kafka record. ``topic`` sets the Benzene ``topic`` header (else the Kafka topic)."""
        wire_headers: list[dict[str, list[int]]] = []
        merged = dict(headers or {})
        if topic is not None:
            merged[TOPIC_ATTRIBUTE] = topic
        for key, value in merged.items():
            wire_headers.append({str(key): list(str(value).encode("utf-8"))})
        record = {
            "topic": kafka_topic,
            "partition": partition,
            "offset": offset,
            "value": base64.b64encode(encode_body(body).encode("utf-8")).decode("ascii"),
            "headers": wire_headers,
        }
        self._records.setdefault(f"{kafka_topic}-{partition}", []).append(record)
        return self

    def build(self) -> dict[str, Any]:
        return {
            "eventSource": "aws:kafka",
            "records": {k: list(v) for k, v in self._records.items()},
        }


@dataclass
class ApiGatewayResponse:
    status_code: int
    headers: dict[str, str]
    body: str


@dataclass
class SqsBatchResponse:
    """The SQS partial-batch response, as an object to assert on (mirrors the HTTP response shape).

    The wire body stays the SQS protocol's ``{"batchItemFailures": [{"itemIdentifier": ...}]}`` — this
    only wraps it so a test reads ``response.batch_item_failures == []`` (parallel to
    ``response.status_code`` on the HTTP hosts) instead of indexing a raw dict.
    """

    batch_item_failures: list[dict[str, str]]

    @property
    def item_identifiers(self) -> list[str]:
        """Just the failed message ids (``itemIdentifier``) — the common thing to assert on."""
        return [f["itemIdentifier"] for f in self.batch_item_failures if "itemIdentifier" in f]

    @classmethod
    def from_wire(cls, response: dict[str, Any]) -> SqsBatchResponse:
        return cls(batch_item_failures=list(response.get("batchItemFailures", [])))


class AwsLambdaTestHost:
    """Wraps an :class:`AwsLambdaApp` for in-memory tests of each event source."""

    #: The resolved root scope, set by ``create_test_host(...).build_aws()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: AwsLambdaApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> ApiGatewayResponse:
        builder = ApiGatewayRequestBuilder(method, path)
        for key, value in (headers or {}).items():
            builder.with_header(key, value)
        for key, value in (query or {}).items():
            builder.with_query(key, value)
        if body is not None:
            builder.with_body(body)
        result = self._app.handle(builder.build())
        assert (
            result is not None
        )  # API Gateway always yields a response envelope (never None like SNS)
        return ApiGatewayResponse(
            result["statusCode"], result.get("headers", {}), result.get("body", "")
        )

    def send_sqs(
        self,
        topic: str,
        body: Any,
        headers: dict[str, str] | None = None,
        message_id: str | None = None,
    ) -> SqsBatchResponse:
        """Send one SQS message; returns the partial-batch response (``.batch_item_failures``)."""
        event = (
            SqsEventBuilder()
            .with_message(topic, body, message_id=message_id, headers=headers)
            .build()
        )
        return self.send_sqs_event(event)

    def send_sqs_event(self, event: dict[str, Any]) -> SqsBatchResponse:
        result = self._app.handle(event)
        assert (
            result is not None
        )  # SQS always yields a partial-batch response (never None like SNS)
        return SqsBatchResponse.from_wire(result)

    def send_sns(self, topic: str, body: Any, headers: dict[str, str] | None = None) -> None:
        event = SnsEventBuilder().with_message(topic, body, headers=headers).build()
        self._app.handle(event)

    def send_s3(self, bucket: str, key: str, event_name: str = "ObjectCreated:Put") -> None:
        """Deliver one S3 object-created notification (topic resolved from the host's convention)."""
        event = S3EventBuilder().with_object(bucket, key, event_name).build()
        self._app.handle(event)

    def send_eventbridge(self, detail_type: str, detail: Any, source: str = "benzene") -> None:
        """Deliver one EventBridge event; the Benzene topic is the ``detail_type``."""
        event = EventBridgeEventBuilder(detail_type, source).with_detail(detail).build()
        self._app.handle(event)

    def send_dynamodb(
        self,
        event_name: str,
        keys: dict[str, Any],
        new_image: dict[str, Any] | None = None,
        sequence_number: str | None = None,
    ) -> SqsBatchResponse:
        """Deliver one DynamoDB Streams record; returns the partial-batch response."""
        event = (
            DynamoDbStreamBuilder()
            .with_record(event_name, keys, new_image=new_image, sequence_number=sequence_number)
            .build()
        )
        result = self._app.handle(event)
        assert result is not None  # DynamoDB always yields a partial-batch response
        return SqsBatchResponse.from_wire(result)

    def send_kinesis(self, body: Any, sequence_number: str | None = None) -> SqsBatchResponse:
        """Deliver one Kinesis record; returns the partial-batch response."""
        event = KinesisEventBuilder().with_record(body, sequence_number=sequence_number).build()
        result = self._app.handle(event)
        assert result is not None  # Kinesis always yields a partial-batch response
        return SqsBatchResponse.from_wire(result)

    def send_kafka(
        self,
        kafka_topic: str,
        body: Any,
        topic: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Deliver one Kafka/MSK record; ``topic`` overrides the Benzene topic (else the Kafka topic)."""
        event = (
            KafkaLambdaEventBuilder()
            .with_message(kafka_topic, body, topic=topic, headers=headers)
            .build()
        )
        self._app.handle(event)
