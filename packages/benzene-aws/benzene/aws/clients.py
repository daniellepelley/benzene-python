"""AWS outbound clients (SNS, SQS, EventBridge, Kinesis, Lambda) implementing the ``MessageSender``
port.

Three carrying conventions, matching what each service exposes on the wire:

- **SNS / SQS** have a native message-attribute channel, so the Benzene topic and headers ride there
  (the ``topic`` attribute plus one attribute per header) — the same shape the inbound decoders read.
- **EventBridge / Kinesis** have *no* metadata channel, so the sender leaves the domain payload as
  the wire body and embeds headers *inside* it, under the reserved ``_benzeneHeaders`` key (mirrors
  .NET's ``Benzene.Clients.Aws.EventBridge`` — see ``docs/specification/transport-bindings.md``
  "EventBridge" for the cross-language contract). The topic travels out-of-band (EventBridge's
  ``DetailType``; Kinesis has no equivalent, so its topic is a caller-supplied convention, not part
  of the wire body). This keeps correlation/trace propagation working end to end without disturbing
  the payload shape a plain (non-Benzene) consumer of the stream/bus would see.
- **Lambda** (:class:`LambdaMessageSender`) calls another function directly — AWS's own
  request/response primitive, no broker involved — so the envelope travels as the invoke Payload
  itself and the *response* Payload decodes straight back into a :class:`~benzene.results.Result`
  (see :class:`~benzene.aws.AwsLambdaApp`'s ``"invoke"`` source, the receiving half of this).

Mirrors .NET's ``Benzene.Clients.Aws.*``. ``boto3`` is an optional dependency, imported lazily, so
the module (and its tests, which inject a fake client) load with no AWS SDK present.

Each ``send_message`` runs its blocking ``boto3`` call via :func:`asyncio.to_thread`, so an
``await sender.send_message(...)`` never blocks the event loop — the same rule the consumer loops
follow, and what keeps an outbound publish from stalling an ASGI server co-hosted in the same process.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from benzene.core import decode_response, encode_body
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
            await asyncio.to_thread(
                self._sns().publish,
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
            await asyncio.to_thread(
                self._sqs().send_message,
                QueueUrl=self._queue_url,
                MessageBody=self._serialize(message),
                MessageAttributes=_string_attributes(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


EMBEDDED_HEADERS_KEY = "_benzeneHeaders"
"""The reserved key inside a headerless-transport body that carries embedded Benzene wire headers.

Mirrors .NET's ``OutboundEventBridgeContextConverter.EmbeddedHeadersKey`` /
``EventBridgeMessageHeadersGetter.EmbeddedHeadersKey`` (wire-contracts §2, transport-bindings.md
"EventBridge"). The inbound ``eventbridge_envelope`` decoder (``events.py``) lifts it back out.
"""


def _embed_headers(
    message: Any, headers: dict[str, str] | None, serialize: Callable[[Any], str]
) -> str:
    """Serialize ``message`` for a transport with no metadata channel (EventBridge, Kinesis),
    embedding ``headers`` under :data:`EMBEDDED_HEADERS_KEY` when the serialized payload is a JSON
    object — matching .NET's ``OutboundEventBridgeContextConverter.BuildDetail``. A non-object
    payload (or no headers at all) is left exactly as the serializer produced it, so a plain
    (non-Benzene) consumer of the stream/bus sees the domain payload verbatim.
    """
    body = serialize(message)
    if not headers:
        return body
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return body
    if not isinstance(parsed, dict):
        return body
    parsed[EMBEDDED_HEADERS_KEY] = dict(headers)
    return json.dumps(parsed)


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
            await asyncio.to_thread(
                self._events().put_events,
                Entries=[
                    {
                        "EventBusName": self._event_bus_name,
                        "Source": self._source,
                        "DetailType": self._detail_type or topic,
                        "Detail": _embed_headers(message, headers, self._serialize),
                    }
                ],
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
            await asyncio.to_thread(
                self._kinesis().put_record,
                StreamName=self._stream_name,
                Data=_embed_headers(message, headers, self._serialize),
                PartitionKey=self._partition_key(topic, headers),
            )
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()


class LambdaMessageSender:
    """Invokes another AWS Lambda function directly — AWS's own ``Invoke`` API, no broker in
    between (mirrors direct Lambda-to-Lambda calls; the AWS-specific counterpart to
    :class:`~benzene.http.HttpMessageSender` / :class:`~benzene.grpc.GrpcMessageSender`, which reach
    the same *outcome* over HTTP/gRPC on platforms with no equivalent invoke primitive).

    The invoke Payload **is** the transport-neutral Benzene envelope (``{topic, headers, body}``), so
    the target needs no special wiring — *any* :class:`~benzene.aws.AwsLambdaApp` recognises this
    shape as its ``"invoke"`` source automatically, the same function it already answers API
    Gateway/SQS/SNS/etc. through.

    ``invocation_type`` selects AWS's two invoke modes:

    - ``"RequestResponse"`` (the default) — synchronous: waits for the target to run and decodes its
      response envelope back into a :class:`~benzene.results.Result`
      (:func:`~benzene.core.decode_response`) — the "call another function and get an answer" pattern.
    - ``"Event"`` — asynchronous: returns as soon as AWS accepts the invoke, before the target even
      runs; maps to :meth:`~benzene.results.Result.accepted`, matching every fire-and-forget sender
      here (no visibility into the target's own outcome).

    ``qualifier`` pins a specific version/alias (``Qualifier`` on the AWS API) when given.
    """

    def __init__(
        self,
        function_name: str,
        client: Any | None = None,
        serializer: Callable[[Any], str] | None = None,
        *,
        invocation_type: str = "RequestResponse",
        qualifier: str | None = None,
    ) -> None:
        self._function_name = function_name
        self._client = client
        self._serialize = serializer or encode_body
        self._invocation_type = invocation_type
        self._qualifier = qualifier

    def _lambda(self) -> Any:
        if self._client is None:
            import boto3  # lazy: optional dependency

            self._client = boto3.client("lambda")
        return self._client

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        envelope = {"topic": topic, "headers": headers or {}, "body": self._serialize(message)}
        kwargs: dict[str, Any] = {
            "FunctionName": self._function_name,
            "InvocationType": self._invocation_type,
            "Payload": json.dumps(envelope).encode("utf-8"),
        }
        if self._qualifier:
            kwargs["Qualifier"] = self._qualifier

        try:
            response = await asyncio.to_thread(self._lambda().invoke, **kwargs)
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))

        if self._invocation_type != "RequestResponse":
            return Result.accepted()  # queued; the target's own outcome is never visible here

        if response.get("FunctionError"):
            # The invoke itself faulted (an unhandled exception/timeout in the target, or a Payload
            # AwsLambdaApp couldn't classify) — never a Benzene envelope to decode.
            return Result.failure(Status.SERVICE_UNAVAILABLE, _invoke_error_detail(response))
        payload = _read_payload(response)
        if not isinstance(payload, dict):
            # The target answered, but not with a Benzene response envelope (it isn't a Benzene
            # function, or returned something else entirely) — a mapped failure, not a crash.
            return Result.failure(
                Status.SERVICE_UNAVAILABLE, f"Unexpected invoke response: {payload!r}"
            )
        return decode_response(payload)


def _read_payload(response: dict[str, Any]) -> Any:
    """Read and JSON-decode a Lambda invoke response's ``Payload`` stream; raw text if not JSON."""
    raw = response["Payload"].read().decode("utf-8")
    try:
        return json.loads(raw) if raw else None
    except (ValueError, TypeError):
        return raw


def _invoke_error_detail(response: dict[str, Any]) -> str:
    """The target's own error message when ``FunctionError`` is set (AWS's ``{errorMessage, ...}``
    shape for an unhandled exception), falling back to the raw payload or the error type."""
    payload = _read_payload(response)
    if isinstance(payload, dict) and payload.get("errorMessage"):
        return str(payload["errorMessage"])
    return str(payload) if payload else str(response["FunctionError"])
