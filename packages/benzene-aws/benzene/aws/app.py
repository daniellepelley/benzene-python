"""AWS Lambda host (transport-bindings §1 — one host, inner bindings selected by event shape).

``AwsLambdaApp`` hosts the same Benzene handlers behind several Lambda event sources:

- **API Gateway** (HTTP-like): topic from the route (via the ``benzene.http`` binding), response
  mapped to an API Gateway proxy response.
- **SQS**: a batch of records; **one pipeline invocation and one scope per record**; failures are
  reported via the SQS partial-batch-response (``batchItemFailures``) so only the failed records are
  redelivered.
- **SNS**: a batch of records; one invocation per record; a failure raises so Lambda retries.
- **S3** (object-created): one invocation per record; no partial-batch channel, so a failure raises.
- **EventBridge**: a *single* event → one invocation; a failure raises so the rule retries.
- **DynamoDB Streams** / **Kinesis Data Streams**: a batch of records; one invocation per record;
  both support partial batches, so failures are reported via ``batchItemFailures`` keyed by the
  record's sequence number (only the failed records are reprocessed).
- **Kafka / MSK**: records grouped by topic-partition; one invocation per record; the MSK event
  source has no partial-batch channel, so a failure raises for redelivery.
- **Direct invoke**: a bare ``{"topic", "headers", "body"}`` Payload — what another Lambda's
  ``lambda.invoke(FunctionName=..., Payload=...)`` sends (see
  :class:`~benzene.aws.LambdaMessageSender`), or any caller speaking the wire envelope directly. This
  is the one *synchronous* source besides API Gateway: the response envelope is returned verbatim as
  the invoke's own response Payload, for the caller to decode back into a
  :class:`~benzene.results.Result` (:func:`~benzene.core.decode_response`).

**Settlement** (transport-bindings §1, obligation 8). Every source but API Gateway and a synchronous
direct invoke is one-way, and by default a message is acknowledged only when its invocation ended
``isSuccessful`` — a failure, a missing result, an unmatched topic or an uncaught error is handed back
for redelivery, and Lambda's own retry / on-failure destination / DLQ configuration decides when to
give up. The unit of settlement per source:

- **SQS** — per message: failures are reported by ``messageId`` (needs *ReportBatchItemFailures* on the
  event source mapping); every record runs.
- **DynamoDB Streams / Kinesis** — per record, reported by sequence number; every record runs, and
  Lambda resumes the shard from the lowest reported failure, so the records after it are redelivered
  too.
- **SNS / S3 / Kafka (MSK)** — the whole invocation: the first failing record raises, the records
  after it do not run, and Lambda retries the whole event (for MSK, every topic-partition in it).
- **EventBridge / an asynchronous direct invoke** — the one event the invocation carried.

``settlement=Settlement(at_most_once=True)`` acknowledges whatever the outcome (nothing is reported,
nothing raises); ``Settlement(ignore_unmatched_topics=True)`` acknowledges a topic no handler serves as
``ignored``. A direct invoke is synchronous — the caller gets the envelope — unless the function is
invoked with ``InvocationType="Event"``, which the function cannot detect: deploy it with
``invocation_type="Event"`` and a failure raises so Lambda's asynchronous retry takes over.

Topic for the attribute-carrying transports (SQS, SNS, Kafka) comes from the reserved ``topic``
metadata key (renamed, with the version and EventBridge ``benzene-headers`` keys, by the application's
``metadata_keys`` — ``AppDefinition(metadata_keys=...)``); the channel-less sources (S3, EventBridge, DynamoDB, Kinesis) take their topic from an
injectable convention configured on the host; a direct invoke's topic is exactly what the caller sent.
The free function :func:`to_lambda_handler` produces the ``handler(event, context)`` callable Lambda
invokes — the *same* function answers every source above, so a Lambda already reached over API
Gateway/SQS/SNS/etc. is, with zero extra wiring, also directly invokable by another Lambda.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

from benzene.core import (
    ONE_SHOT,
    QUEUE,
    REQUEST_RESPONSE,
    STREAM,
    AppDefinition,
    BenzeneMessageApplication,
    MessageHandlingError,
    MetadataKeys,
    Registry,
    Settlement,
    SettlementDefaults,
    application_from,
    dispatch_one_way,
    redelivers,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths

from .events import (
    DEFAULT_EVENTBRIDGE_TOPIC,
    DEFAULT_KINESIS_TOPIC,
    DEFAULT_S3_TOPIC,
    api_gateway_request,
    dynamodb_record_envelope,
    event_source,
    eventbridge_envelope,
    invoke_envelope,
    kafka_record_envelope,
    kafka_records,
    kinesis_record_envelope,
    s3_record_envelope,
    sns_record_envelope,
    sqs_record_envelope,
)

#: How a direct invoke reached the function (AWS's own ``InvocationType`` values).
InvocationType = Literal["RequestResponse", "Event"]


class AwsLambdaApp:
    #: Each source's settlement shape and defaults, keyed by its ``settlementDefaults`` binding id
    #: (transport-bindings §1, obligation 8) — the constants the conformance runner checks.
    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (
        redelivers("sqs", QUEUE),
        redelivers("sns", ONE_SHOT),
        redelivers("s3", ONE_SHOT),
        redelivers("eventbridge", ONE_SHOT),
        redelivers("kafka", STREAM),
        redelivers("dynamodb-streams", STREAM),
        redelivers("kinesis", STREAM),
        redelivers("envelope-async", ONE_SHOT),
        redelivers("envelope", REQUEST_RESPONSE),
        redelivers("http", REQUEST_RESPONSE),
    )

    def __init__(
        self,
        http_router: HttpRouter | None = None,
        registry: Registry | None = None,
        application: BenzeneMessageApplication | None = None,
        *,
        standard_paths: StandardPaths | None = None,
        s3_topic: str = DEFAULT_S3_TOPIC,
        eventbridge_topic: str = DEFAULT_EVENTBRIDGE_TOPIC,
        kinesis_topic: str = DEFAULT_KINESIS_TOPIC,
        dynamodb_topic: str | None = None,
        settlement: Settlement = Settlement(),
        invocation_type: InvocationType = "RequestResponse",
    ) -> None:
        if application is None:
            if registry is None:
                registry = Registry.from_definitions(http_router) if http_router else Registry()
            application = BenzeneMessageApplication(registry)
        if invocation_type not in ("RequestResponse", "Event"):
            raise ValueError(
                f"invocation_type={invocation_type!r}: a direct invoke is 'RequestResponse' (the "
                "caller waits for the envelope) or 'Event' (asynchronous - a failure raises so Lambda "
                "retries it)"
            )
        self._application = application
        self._settlement = settlement
        self._invocation_type = invocation_type
        # Topic conventions for the sources with no native metadata channel. ``dynamodb_topic`` stays
        # ``None`` to mean "derive per record as {tableName}:{eventName}" (e.g. ``orders:INSERT``).
        self._s3_topic = s3_topic
        self._eventbridge_topic = eventbridge_topic
        self._kinesis_topic = kinesis_topic
        self._dynamodb_topic = dynamodb_topic
        self._http_app = (
            BenzeneHttpApp(http_router, application=application, standard_paths=standard_paths)
            if http_router
            else None
        )

    @classmethod
    def from_definition(
        cls,
        definition: AppDefinition,
        *,
        settlement: Settlement = Settlement(),
        invocation_type: InvocationType = "RequestResponse",
    ) -> AwsLambdaApp:
        """Build the Lambda host from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(
            http_router=definition.router,
            application=application_from(definition),
            standard_paths=definition.standard_paths,
            settlement=settlement,
            invocation_type=invocation_type,
        )

    @property
    def settlement(self) -> Settlement:
        """How the one-way sources settle each message (see the module docstring)."""
        return self._settlement

    def handle(self, event: dict[str, Any], context: Any = None) -> dict[str, Any] | None:
        source = event_source(event)
        if source == "apigateway":
            return self._handle_api_gateway(event)
        if source == "sqs":
            return self._handle_sqs(event)
        if source == "dynamodb":
            return self._handle_dynamodb(event)
        if source == "kinesis":
            return self._handle_kinesis(event)
        if source == "sns":
            self._handle_sns(event)  # SNS is fire-and-forget: no response envelope to return
            return None
        if source == "s3":
            self._handle_s3(event)  # no partial-batch channel: a failure raises for retry
            return None
        if source == "eventbridge":
            self._handle_eventbridge(event)  # a single event, no response envelope
            return None
        if source == "kafka":
            self._handle_kafka(event)  # MSK has no partial-batch channel: a failure raises
            return None
        if source == "invoke":
            return self._handle_invoke(event)
        raise ValueError(
            "Unrecognised Lambda event: not API Gateway, SQS, SNS, S3, EventBridge, "
            "DynamoDB, Kinesis, Kafka, or a direct invoke (a bare {topic, headers, body} Payload)"
        )

    # --- API Gateway -----------------------------------------------------------------------
    def _handle_api_gateway(self, event: dict[str, Any]) -> dict[str, Any]:
        if self._http_app is None:
            return {
                "statusCode": 501,
                "headers": {"content-type": "application/json"},
                "body": '{"status": "not-implemented"}',
            }
        req = api_gateway_request(event)
        response = asyncio.run(self._http_app.handle(**req))
        return {
            "statusCode": response.status_code,
            "headers": dict(response.headers),
            "body": response.body,
        }

    # --- SQS (partial batch response) ------------------------------------------------------
    def _handle_sqs(self, event: dict[str, Any]) -> dict[str, Any]:
        async def run() -> list[dict[str, str]]:
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                outcome = await dispatch_one_way(
                    self._application, self._settlement, sqs_record_envelope, record, keys=self._keys
                )
                if not outcome.acknowledge:
                    failures.append({"itemIdentifier": record.get("messageId", "")})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    # --- SNS -------------------------------------------------------------------------------
    def _handle_sns(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in event.get("Records", []):
                await self._settle_or_raise(sns_record_envelope, record, keys=self._keys)

        asyncio.run(run())

    # --- S3 (object-created; no partial-batch channel) -------------------------------------
    def _handle_s3(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in event.get("Records", []):
                await self._settle_or_raise(s3_record_envelope, record, self._s3_topic)

        asyncio.run(run())

    # --- EventBridge (a single event) ------------------------------------------------------
    def _handle_eventbridge(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            await self._settle_or_raise(
                eventbridge_envelope, event, self._eventbridge_topic, keys=self._keys
            )

        asyncio.run(run())

    # --- Kafka / MSK (no partial-batch channel) --------------------------------------------
    def _handle_kafka(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in kafka_records(event):
                await self._settle_or_raise(kafka_record_envelope, record, keys=self._keys)

        asyncio.run(run())

    # --- DynamoDB Streams (partial batch response) -----------------------------------------
    def _handle_dynamodb(self, event: dict[str, Any]) -> dict[str, Any]:
        async def run() -> list[dict[str, str]]:
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                outcome = await dispatch_one_way(
                    self._application,
                    self._settlement,
                    dynamodb_record_envelope,
                    record,
                    self._dynamodb_topic,
                )
                if not outcome.acknowledge:
                    sequence = (record.get("dynamodb") or {}).get("SequenceNumber", "")
                    failures.append({"itemIdentifier": sequence})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    # --- Kinesis Data Streams (partial batch response) -------------------------------------
    def _handle_kinesis(self, event: dict[str, Any]) -> dict[str, Any]:
        async def run() -> list[dict[str, str]]:
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                outcome = await dispatch_one_way(
                    self._application,
                    self._settlement,
                    kinesis_record_envelope,
                    record,
                    self._kinesis_topic,
                )
                if not outcome.acknowledge:
                    sequence = (record.get("kinesis") or {}).get("sequenceNumber", "")
                    failures.append({"itemIdentifier": sequence})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    # --- Direct invoke (synchronous like API Gateway, or asynchronous by configuration) -----
    def _handle_invoke(self, event: dict[str, Any]) -> dict[str, Any]:
        """Answer a direct ``lambda.invoke()`` Payload with the response envelope, verbatim.

        Unlike every trigger-delivered source, a direct invoke already speaks the Benzene wire
        envelope on both sides, so there is no transport-shaped response to build — the raw
        ``{statusCode, headers, body}`` :class:`~benzene.core.BenzeneMessageApplication` produces
        *is* the invoke's response Payload, ready for :class:`~benzene.aws.LambdaMessageSender` (or
        any caller) to decode back into a :class:`~benzene.results.Result`.

        Configured ``invocation_type="Event"``, there is no caller to read that envelope, so the
        invoke settles like any one-way source: a failure raises :class:`MessageHandlingError` and
        Lambda's asynchronous retry takes over (transport-bindings §1, the asynchronous envelope).
        """
        if self._invocation_type == "Event":
            outcome = asyncio.run(self._settle_or_raise(invoke_envelope, event))
            return dict(outcome) if outcome is not None else {}
        return asyncio.run(self._application.handle(invoke_envelope(event)))

    @property
    def _keys(self) -> MetadataKeys:
        """The reserved metadata names, configured once on the application (wire-contracts §2)."""
        return self._application.metadata_keys

    async def _settle_or_raise(
        self, decode: Callable[..., Mapping[str, Any]], *args: Any, **kwargs: Any
    ) -> Mapping[str, Any] | None:
        """Run one message for a source with no partial-batch channel; fault the invocation unless
        it is acknowledged.

        The only way to decline the acknowledgement on SNS, S3, EventBridge, MSK and an asynchronous
        invoke is to fail the invocation: raise ``MessageHandlingError`` naming the topic, the status
        and the problem document. Whether it failed is the envelope's ``isSuccessful`` to say
        (wire-contracts §1.2), never the status text.
        """
        outcome = await dispatch_one_way(
            self._application, self._settlement, decode, *args, **kwargs
        )
        if not outcome.acknowledge:
            raise MessageHandlingError(outcome.topic, outcome.result.status, outcome.detail)
        return outcome.response


def to_lambda_handler(app: AwsLambdaApp) -> Callable[..., Any]:
    """Return the ``handler(event, context)`` callable AWS Lambda invokes."""

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        return app.handle(event, context)

    return handler
