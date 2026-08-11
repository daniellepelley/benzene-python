"""AWS outbound clients (SNS, SQS, EventBridge, Kinesis) implementing the ``MessageSender`` port.

Two carrying conventions, matching what each service exposes on the wire:

- **SNS / SQS** have a native message-attribute channel, so the Benzene topic and headers ride there
  (the ``topic`` attribute plus one attribute per header) — the same shape the inbound decoders read.
- **EventBridge / Kinesis** have *no* metadata channel, so the sender embeds the whole Benzene
  envelope ``{topic, headers, body}`` inside the payload it serializes. This keeps
  correlation/trace propagation working end to end regardless of the transport.

Mirrors .NET's ``Benzene.Clients.Aws.*``. ``boto3`` is an optional dependency, imported lazily, so
the module (and its tests, which inject a fake client) load with no AWS SDK present.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from benzene.core import encode_body
from benzene.results import Result, Status

from .events import TOPIC_ATTRIBUTE


def _string_attributes(topic: str, headers: dict[str, str] | None) -> dict[str, dict[str, str]]:
    attrs = {TOPIC_ATTRIBUTE: {"DataType": "String", "StringValue": topic}}
    for key, value in (headers or {}).items():
        attrs[str(key)] = {"DataType": "String", "StringValue": str(value)}
    return attrs


class SnsMessageSender:
    """Publishes to an SNS topic ARN, Benzene topic carried in the ``topic`` message attribute."""

    def __init__(
        self,
        topic_arn: str,
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._topic_arn = topic_arn
        self._client = client
        self._serialize = serializer or encode_body

    def _sns(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("sns")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._sns().publish(
                TopicArn=self._topic_arn,
                Message=self._serialize(message),
                MessageAttributes=_string_attributes(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class SqsMessageSender:
    """Sends to an SQS queue URL, Benzene topic carried in the ``topic`` message attribute."""

    def __init__(
        self,
        queue_url: str,
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._queue_url = queue_url
        self._client = client
        self._serialize = serializer or encode_body

    def _sqs(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("sqs")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._sqs().send_message(
                QueueUrl=self._queue_url,
                MessageBody=self._serialize(message),
                MessageAttributes=_string_attributes(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


def _embedded_envelope(
    topic: str, message: Any, headers: dict[str, str] | None, serialize: Callable[[Any], str]
) -> str:
    """Serialize the Benzene envelope for a transport with no metadata channel (EventBridge, Kinesis).

    The topic and headers travel *inside* the payload as ``{"topic", "headers", "body"}`` so nothing
    is lost on a wire that carries only an opaque blob; the body is run through the shared serializer.
    """
    return serialize({"topic": topic, "headers": headers or {}, "body": message})


class EventBridgeMessageSender:
    """Publishes to an EventBridge event bus (mirrors ``Benzene.Clients.Aws.EventBridge``).

    Each ``put_events`` entry names the configured ``source`` and a ``DetailType`` (the fixed
    ``detail_type`` classifier when given, else the Benzene topic — which round-trips with the inbound
    decoder's "topic from ``detail-type``"); the ``Detail`` embeds the full Benzene envelope so topic
    and headers survive a bus that has no attribute channel.
    """

    def __init__(
        self,
        event_bus_name: str,
        source: str = "benzene",
        detail_type: str | None = None,
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._event_bus_name = event_bus_name
        self._source = source
        self._detail_type = detail_type
        self._client = client
        self._serialize = serializer or encode_body

    def _events(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("events")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._events().put_events(
                Entries=[
                    {
                        "EventBusName": self._event_bus_name,
                        "Source": self._source,
                        "DetailType": self._detail_type or topic,
                        "Detail": _embedded_envelope(topic, message, headers, self._serialize),
                    }
                ]
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class KinesisMessageSender:
    """Puts a record on a Kinesis Data Stream (mirrors ``Benzene.Clients.Aws.Kinesis``).

    Kinesis carries only an opaque ``Data`` blob and a ``PartitionKey``, so the Benzene envelope is
    embedded in ``Data``. The partition key is read from the header named ``partition_key_header``
    when present, else it falls back to the topic — so records for one topic co-locate on a shard and
    stay ordered by default.
    """

    def __init__(
        self,
        stream_name: str,
        partition_key_header: str = "partition-key",
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
    ) -> None:
        self._stream_name = stream_name
        self._partition_key_header = partition_key_header
        self._client = client
        self._serialize = serializer or encode_body

    def _kinesis(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("kinesis")
        return self._client

    def _partition_key(self, topic: str, headers: dict[str, str] | None) -> str:
        if self._partition_key_header and headers:
            value = headers.get(self._partition_key_header)
            if value:
                return value
        return topic

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        try:
            self._kinesis().put_record(
                StreamName=self._stream_name,
                Data=_embedded_envelope(topic, message, headers, self._serialize),
                PartitionKey=self._partition_key(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()
