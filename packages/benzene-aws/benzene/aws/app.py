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

Topic for the attribute-carrying transports (SQS, SNS, Kafka) comes from the reserved ``topic``
metadata key; the channel-less sources (S3, EventBridge, DynamoDB, Kinesis) take their topic from an
injectable convention configured on the host. The free function :func:`to_lambda_handler` produces
the ``handler(event, context)`` callable Lambda invokes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    MessageHandlingError,
    Registry,
    application_from,
)
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.results import is_successful

from .events import (
    DEFAULT_EVENTBRIDGE_TOPIC,
    DEFAULT_KINESIS_TOPIC,
    DEFAULT_S3_TOPIC,
    api_gateway_request,
    dynamodb_record_envelope,
    event_source,
    eventbridge_envelope,
    kafka_record_envelope,
    kafka_records,
    kinesis_record_envelope,
    s3_record_envelope,
    sns_record_envelope,
    sqs_record_envelope,
)


class AwsLambdaApp:
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
    ) -> None:
        if application is None:
            if registry is None:
                registry = Registry.from_definitions(http_router) if http_router else Registry()
            application = BenzeneMessageApplication(registry)
        self._application = application
        # Topic conventions for the sources with no native metadata channel. ``dynamodb_topic`` stays
        # ``None`` to mean "derive per record from its eventName" (e.g. ``dynamodb:insert``).
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
    def from_definition(cls, definition: AppDefinition) -> AwsLambdaApp:
        """Build the Lambda host from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(
            http_router=definition.router,
            application=application_from(definition),
            standard_paths=definition.standard_paths,
        )

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
        raise ValueError(
            "Unrecognised Lambda event: not API Gateway, SQS, SNS, S3, EventBridge, "
            "DynamoDB, Kinesis, or Kafka"
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
                envelope = sqs_record_envelope(record)
                response = await self._application.handle(envelope)
                if not is_successful(response["statusCode"]):
                    failures.append({"itemIdentifier": record.get("messageId", "")})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    # --- SNS -------------------------------------------------------------------------------
    def _handle_sns(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in event.get("Records", []):
                envelope = sns_record_envelope(record)
                await self._dispatch_or_raise(envelope)

        asyncio.run(run())

    # --- S3 (object-created; no partial-batch channel) -------------------------------------
    def _handle_s3(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in event.get("Records", []):
                envelope = s3_record_envelope(record, self._s3_topic)
                await self._dispatch_or_raise(envelope)

        asyncio.run(run())

    # --- EventBridge (a single event) ------------------------------------------------------
    def _handle_eventbridge(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            await self._dispatch_or_raise(eventbridge_envelope(event, self._eventbridge_topic))

        asyncio.run(run())

    # --- Kafka / MSK (no partial-batch channel) --------------------------------------------
    def _handle_kafka(self, event: dict[str, Any]) -> None:
        async def run() -> None:
            for record in kafka_records(event):
                envelope = kafka_record_envelope(record)
                await self._dispatch_or_raise(envelope)

        asyncio.run(run())

    # --- DynamoDB Streams (partial batch response) -----------------------------------------
    def _handle_dynamodb(self, event: dict[str, Any]) -> dict[str, Any]:
        async def run() -> list[dict[str, str]]:
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                envelope = dynamodb_record_envelope(record, self._dynamodb_topic)
                response = await self._application.handle(envelope)
                if not is_successful(response["statusCode"]):
                    sequence = (record.get("dynamodb") or {}).get("SequenceNumber", "")
                    failures.append({"itemIdentifier": sequence})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    # --- Kinesis Data Streams (partial batch response) -------------------------------------
    def _handle_kinesis(self, event: dict[str, Any]) -> dict[str, Any]:
        async def run() -> list[dict[str, str]]:
            failures: list[dict[str, str]] = []
            for record in event.get("Records", []):
                envelope = kinesis_record_envelope(record, self._kinesis_topic)
                response = await self._application.handle(envelope)
                if not is_successful(response["statusCode"]):
                    sequence = (record.get("kinesis") or {}).get("sequenceNumber", "")
                    failures.append({"itemIdentifier": sequence})
            return failures

        return {"batchItemFailures": asyncio.run(run())}

    async def _dispatch_or_raise(self, envelope: dict[str, Any]) -> None:
        """Run one envelope; on a failure result raise ``MessageHandlingError`` (retry/redeliver).

        The shared path for the sources with no partial-batch channel (SNS, S3, EventBridge, Kafka):
        the only way to signal failure back to the platform is to fault the invocation.
        """
        response = await self._application.handle(envelope)
        if not is_successful(response["statusCode"]):
            raise MessageHandlingError(envelope["topic"], response["statusCode"], response["body"])


def to_lambda_handler(app: AwsLambdaApp) -> Callable[..., Any]:
    """Return the ``handler(event, context)`` callable AWS Lambda invokes."""

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        return app.handle(event, context)

    return handler
