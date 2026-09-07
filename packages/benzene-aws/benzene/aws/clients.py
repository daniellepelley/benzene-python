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
the module (and its tests, which inject a fake client) load with no AWS SDK present. A *missing*
boto3 raises an ImportError naming the extra (``benzene-aws[boto3]``) straight out of
``send_message`` — a forgotten extra is a deployment error, not a message outcome, so it must never
be mapped to ``service-unavailable`` for retries and circuit breakers to hammer.

Each ``send_message`` runs its blocking ``boto3`` call via :func:`asyncio.to_thread`, so an
``await sender.send_message(...)`` never blocks the event loop — the same rule the consumer loops
follow, and what keeps an outbound publish from stalling an ASGI server co-hosted in the same process.

**Batches.** SNS, SQS, EventBridge and Kinesis each also implement
:class:`~benzene.core.BatchMessageSender` — ``send_batch([(topic, message), ...])`` — over their
native N-per-call APIs, so publishing 1,000 events costs 100 round trips (or 2, on Kinesis) instead
of 1,000. Each chunks to that API's documented cap, runs a whole chunk in **one**
:func:`asyncio.to_thread` hop, and reports a per-message outcome, because all four APIs are
partial-failure APIs: an entry can fail while its neighbours in the same call succeed. Every batch
entry is built by the same helpers ``send_message`` uses, so batching changes how messages are
transmitted and never what a message *is*. :class:`LambdaMessageSender` has no batch API to wrap
(``Invoke`` takes one payload) and says so — its ``send_batch`` is a documented sequential fallback.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from typing import Any

from benzene.core import (
    BatchResult,
    FailedMessage,
    chunked,
    decode_response,
    encode_body,
    send_batch_sequentially,
)
from benzene.results import Result, Status

from .events import TOPIC_ATTRIBUTE


def _boto3(sender: str) -> Any:
    """Import ``boto3`` lazily, turning a missing optional dependency into a teaching error.

    A missing SDK is a *deployment* error, not a message outcome: swallowed by a sender's
    ``except Exception`` mapper it would become a ``service-unavailable`` result that retry
    middleware and circuit breakers then hammer forever. Surfacing it as an ImportError naming the
    exact extra fails fast and says what to install (the same guard :mod:`benzene.grpc` uses); each
    ``send_message`` re-raises it ahead of its generic mapper.
    """
    try:
        import boto3  # lazy: optional dependency
    except ImportError as exc:
        raise ImportError(
            f"{sender} requires boto3 — install it with 'pip install benzene-aws[boto3]'."
        ) from exc
    return boto3


def _string_attributes(topic: str, headers: dict[str, str] | None) -> dict[str, dict[str, str]]:
    attrs = {TOPIC_ATTRIBUTE: {"DataType": "String", "StringValue": topic}}
    for key, value in (headers or {}).items():
        attrs[str(key)] = {"DataType": "String", "StringValue": str(value)}
    return attrs


SQS_BATCH_LIMIT = 10
"""``SendMessageBatch`` accepts at most 10 entries per call (AWS SQS API reference; the cap .NET's
``SqsBatchMessageClient`` chunks to). The 256 KB total-payload limit is *not* enforced here — the
service reports an oversized batch per entry, which arrives as those entries' failures."""

SNS_BATCH_LIMIT = 10
"""``PublishBatch`` accepts at most 10 entries per call (AWS SNS API reference)."""

EVENTBRIDGE_BATCH_LIMIT = 10
"""``PutEvents`` accepts at most 10 entries per call (AWS EventBridge API reference)."""

KINESIS_BATCH_LIMIT = 500
"""``PutRecords`` accepts at most 500 records per call (AWS Kinesis API reference)."""


def _chunk_failed(sent: list[tuple[int, Any]], detail: str) -> list[FailedMessage]:
    """Fail every caller index the chunk actually carried — a whole-call error (throttle, network,
    expired credentials).

    Only *this* chunk: earlier chunks' successes are kept and later chunks are still attempted, so
    the caller resends exactly what did not land instead of duplicating what did. ``sent`` excludes
    entries already failed on the way in (an unserializable payload), so no index is reported twice.
    """
    return [FailedMessage(index, Status.SERVICE_UNAVAILABLE, detail) for index, _message in sent]


def _entry_status(sender_fault: Any) -> str:
    """AWS's ``SenderFault`` decides retryability: a caller fault will fail again identically.

    Mapping it to ``bad-request`` keeps it out of :data:`~benzene.core.DEFAULT_RETRYABLE`, so the
    retry decorator resends the service's own failures and leaves a malformed entry alone.
    """
    return Status.BAD_REQUEST if sender_fault else Status.SERVICE_UNAVAILABLE


def _positional_failures(
    chunk: list[tuple[int, Any]], entries: list[dict[str, Any]]
) -> list[FailedMessage]:
    """Map a positional per-entry response (EventBridge ``PutEvents``, Kinesis ``PutRecords``).

    Neither API echoes an id, so the i-th response entry pairs with the i-th *request* entry; the
    chunk carries the caller indices that position maps back to.
    """
    failures = []
    # strict=False: a response shorter than the request is a provider anomaly, and dropping to the
    # entries actually reported beats raising out of a send that partly succeeded.
    for (index, _message), entry in zip(chunk, entries, strict=False):
        code = entry.get("ErrorCode")
        if code:
            detail = f"{code}: {entry.get('ErrorMessage', '')}".strip()
            failures.append(FailedMessage(index, Status.SERVICE_UNAVAILABLE, detail))
    return failures


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
            self._client = _boto3("SnsMessageSender").client("sns")
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
        except ImportError:
            raise  # a missing boto3 is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Publish through SNS ``PublishBatch`` (≤10 per call), reporting per-entry outcomes.

        Each entry's ``Id`` carries its caller index, so the response's ``Failed`` list maps
        straight back to the caller's own positions.
        """
        client = self._sns()  # ImportError here: a missing boto3 is never a message outcome
        failures: list[FailedMessage] = []
        for chunk in chunked(messages, SNS_BATCH_LIMIT):
            failures.extend(await asyncio.to_thread(self._publish_chunk, client, chunk, headers))
        return BatchResult(tuple(failures))

    def _publish_chunk(
        self, client: Any, chunk: list[tuple[int, tuple[str, Any]]], headers: dict[str, str] | None
    ) -> list[FailedMessage]:
        entries, failures, sent = [], [], []
        for index, (topic, message) in chunk:
            try:
                entries.append(
                    {
                        "Id": str(index),
                        "Message": self._serialize(message),
                        "MessageAttributes": _string_attributes(topic, headers),
                    }
                )
            except Exception as ex:  # one unserializable payload is that entry's failure alone
                failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                continue
            sent.append((index, message))
        if not entries:
            return failures
        try:
            response = client.publish_batch(
                TopicArn=self._topic_arn, PublishBatchRequestEntries=entries
            )
        except Exception as ex:
            return failures + _chunk_failed(sent, str(ex))
        for failed in response.get("Failed", []):
            detail = f"{failed.get('Code')}: {failed.get('Message', '')}".strip()
            failures.append(
                FailedMessage(int(failed["Id"]), _entry_status(failed.get("SenderFault")), detail)
            )
        return failures


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
            self._client = _boto3("SqsMessageSender").client("sqs")
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
        except ImportError:
            raise  # a missing boto3 is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Send through SQS ``SendMessageBatch`` (≤10 per call), reporting per-entry outcomes.

        ``SendMessageBatch`` is the archetypal partial-failure API — it answers with ``Successful``
        *and* ``Failed`` — so each entry's ``Id`` carries its caller index and the ``Failed`` list
        becomes :class:`~benzene.core.FailedMessage` entries at those same positions.
        """
        client = self._sqs()  # ImportError here: a missing boto3 is never a message outcome
        failures: list[FailedMessage] = []
        for chunk in chunked(messages, SQS_BATCH_LIMIT):
            failures.extend(await asyncio.to_thread(self._send_chunk, client, chunk, headers))
        return BatchResult(tuple(failures))

    def _send_chunk(
        self, client: Any, chunk: list[tuple[int, tuple[str, Any]]], headers: dict[str, str] | None
    ) -> list[FailedMessage]:
        entries, failures, sent = [], [], []
        for index, (topic, message) in chunk:
            try:
                entries.append(
                    {
                        "Id": str(index),
                        "MessageBody": self._serialize(message),
                        "MessageAttributes": _string_attributes(topic, headers),
                    }
                )
            except Exception as ex:  # one unserializable payload is that entry's failure alone
                failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                continue
            sent.append((index, message))
        if not entries:
            return failures
        try:
            response = client.send_message_batch(QueueUrl=self._queue_url, Entries=entries)
        except Exception as ex:
            return failures + _chunk_failed(sent, str(ex))
        for failed in response.get("Failed", []):
            detail = f"{failed.get('Code')}: {failed.get('Message', '')}".strip()
            failures.append(
                FailedMessage(int(failed["Id"]), _entry_status(failed.get("SenderFault")), detail)
            )
        return failures


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
            self._client = _boto3("EventBridgeMessageSender").client("events")
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
        except ImportError:
            raise  # a missing boto3 is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Publish through ``PutEvents`` (≤10 entries per call), reporting per-entry outcomes.

        ``PutEvents`` has no per-entry id: the response's ``Entries`` list is **positional**, so
        response entry *i* belongs to request entry *i* of that chunk, which the chunk's caller
        indices then map back to the caller's own list.
        """
        client = self._events()  # ImportError here: a missing boto3 is never a message outcome
        failures: list[FailedMessage] = []
        for chunk in chunked(messages, EVENTBRIDGE_BATCH_LIMIT):
            failures.extend(await asyncio.to_thread(self._put_chunk, client, chunk, headers))
        return BatchResult(tuple(failures))

    def _put_chunk(
        self, client: Any, chunk: list[tuple[int, tuple[str, Any]]], headers: dict[str, str] | None
    ) -> list[FailedMessage]:
        entries, failures, sent = [], [], []
        for index, (topic, message) in chunk:
            try:
                entries.append(self._entry(topic, message, headers))
            except Exception as ex:  # one unserializable payload is that entry's failure alone
                failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                continue
            sent.append((index, message))
        if not entries:
            return failures
        try:
            response = client.put_events(Entries=entries)
        except Exception as ex:
            return failures + _chunk_failed(sent, str(ex))
        if not response.get("FailedEntryCount"):
            return failures
        return failures + _positional_failures(sent, response.get("Entries", []))

    def _entry(self, topic: str, message: Any, headers: dict[str, str] | None) -> dict[str, Any]:
        """One ``PutEvents`` entry — the single-send body verbatim, so both paths agree on the wire."""
        return {
            "EventBusName": self._event_bus_name,
            "Source": self._source,
            "DetailType": self._detail_type or topic,
            "Detail": _embed_headers(message, headers, self._serialize),
        }


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
            self._client = _boto3("KinesisMessageSender").client("kinesis")
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
        except ImportError:
            raise  # a missing boto3 is a deployment error, never a service-unavailable result
        except Exception as ex:
            return Result.failure(Status.SERVICE_UNAVAILABLE, str(ex))
        return Result.ok()

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """Put through ``PutRecords`` (≤500 records per call), reporting per-record outcomes.

        Like ``PutEvents`` the response is **positional**, and ``FailedRecordCount`` gates the walk.
        Records keep the caller's order within a call, and each record's partition key is chosen
        exactly as ``send_message`` chooses it, so a keyed stream's per-shard ordering is unchanged.
        """
        client = self._kinesis()  # ImportError here: a missing boto3 is never a message outcome
        failures: list[FailedMessage] = []
        for chunk in chunked(messages, KINESIS_BATCH_LIMIT):
            failures.extend(await asyncio.to_thread(self._put_chunk, client, chunk, headers))
        return BatchResult(tuple(failures))

    def _put_chunk(
        self, client: Any, chunk: list[tuple[int, tuple[str, Any]]], headers: dict[str, str] | None
    ) -> list[FailedMessage]:
        records, failures, sent = [], [], []
        for index, (topic, message) in chunk:
            try:
                records.append(
                    {
                        "Data": _embed_headers(message, headers, self._serialize),
                        "PartitionKey": self._partition_key(topic, headers),
                    }
                )
            except Exception as ex:  # one unserializable payload is that entry's failure alone
                failures.append(FailedMessage(index, Status.BAD_REQUEST, str(ex)))
                continue
            sent.append((index, message))
        if not records:
            return failures
        try:
            response = client.put_records(StreamName=self._stream_name, Records=records)
        except Exception as ex:
            return failures + _chunk_failed(sent, str(ex))
        if not response.get("FailedRecordCount"):
            return failures
        return failures + _positional_failures(sent, response.get("Records", []))


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
            self._client = _boto3("LambdaMessageSender").client("lambda")
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
        except ImportError:
            raise  # a missing boto3 is a deployment error, never a service-unavailable result
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

    async def send_batch(
        self, messages: Sequence[tuple[str, Any]], headers: dict[str, str] | None = None
    ) -> BatchResult:
        """**Sequential fallback**: Lambda ``Invoke`` takes one payload, so this is N invokes.

        There is no batch invoke API to wrap, and pretending otherwise would sell one round trip
        where the caller pays N — so this loops rather than faking atomicity, and still reports a
        per-message outcome: each invoke's own decoded :class:`~benzene.results.Result` status is
        recorded at that message's caller index, and one failure aborts nothing.
        """
        return await send_batch_sequentially(self, messages, headers)


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
