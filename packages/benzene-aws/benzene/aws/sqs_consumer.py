"""AWS SQS inbound binding — a self-hosted consumer (transport-bindings §"AWS SQS"), the
standalone-worker counterpart of the Lambda SQS trigger (:mod:`benzene.aws.events`/:class:`AwsLambdaApp`).

Reach for this when the service polls SQS itself — a long-running worker, a container, a Kubernetes
Deployment — rather than being invoked by a Lambda event source mapping. Worth it even if SQS is the
only transport this service ever has: ``boto3``'s ``receive_message`` on its own hands back a raw
message and stops there — decoding the payload, dispatching on whatever identifies its type, and every
cross-cutting concern (validation, correlation, retries, structured logging) is code you would
otherwise write yourself in the poll loop. This binding is that missing routing-plus-middleware layer,
mirroring :mod:`benzene.kafka`'s self-hosted consumer.

The message body is the JSON payload; the Benzene **topic** is carried in a ``topic`` message
attribute (the cross-port convention shared with SNS/Pub/Sub/Service Bus, wire-contracts §2). There is
**no response channel** — one message is one pipeline invocation and one DI scope, and the handler's
result governs whether the loop deletes the message (success, at-least-once) or leaves it on the queue
for redelivery/DLQ redrive, never a reply.

``client`` is duck-typed against ``boto3``'s SQS client (``receive_message``/``delete_message``), so
the binding — decode, per-message dispatch, and the poll loop — is exercised in memory with fakes and
needs neither a queue nor the SDK. ``boto3`` is only needed for the real client and stays an optional
dependency, exactly like :class:`~benzene.aws.clients.SqsMessageSender`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from benzene.core import (
    AppDefinition,
    BenzeneMessageApplication,
    application_from,
    read_message_metadata,
)
from benzene.results import Result


def _decode_attributes(raw: dict[str, Any] | None) -> dict[str, str]:
    """``receive_message``'s ``MessageAttributes`` (``StringValue``/``DataType``) → the flat string dict."""
    attributes: dict[str, str] = {}
    for key, value in (raw or {}).items():
        attributes[key] = value.get("StringValue") or ""
    return attributes


def decode_sqs_message(message: dict[str, Any]) -> dict[str, Any]:
    """Decode one ``receive_message()`` message dict into a Benzene envelope ``{topic, headers, body}``.

    The Benzene topic is resolved out of the ``topic`` message attribute; every other attribute becomes
    a Benzene header. Note this is **not** the same shape as a Lambda SQS-trigger record
    (:func:`benzene.aws.events.sqs_record_envelope`) — ``receive_message``'s ``MessageAttributes`` uses
    ``StringValue``/``DataType`` where a Lambda event's ``messageAttributes`` uses lower-cased keys.
    """
    topic, headers = read_message_metadata(_decode_attributes(message.get("MessageAttributes")))
    return {"topic": topic, "headers": headers, "body": message.get("Body") or ""}


class SqsConsumerApp:
    """Runs one SQS message through the Benzene pipeline (one message → one invocation → one scope).

    :meth:`handle_message` never raises — the entry point always returns a response envelope (a
    malformed body is a ``bad-request``, a handler fault a ``service-unavailable``), so a poison
    message can never crash the poll loop. Since SQS has no response channel here, the mapped
    :class:`~benzene.results.Result` is returned for the loop to *act on* (delete / leave for
    redelivery), not sent back to a caller.
    """

    def __init__(self, application: BenzeneMessageApplication) -> None:
        self._application = application

    @classmethod
    def from_definition(cls, definition: AppDefinition) -> SqsConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition))

    async def handle_message(self, message: dict[str, Any]) -> Result:
        envelope = decode_sqs_message(message)
        response = await self._application.handle(envelope)
        return Result(response["statusCode"])


async def run_sqs_consumer_loop(
    app: SqsConsumerApp,
    client: Any,
    queue_url: str,
    *,
    max_number_of_messages: int = 10,
    wait_time_seconds: int = 20,
    should_continue: Callable[[], bool] = lambda: True,
    delete: bool = True,
    on_result: Callable[[dict[str, Any], Result], None] | None = None,
) -> None:
    """Drive a self-hosted consumer: long-poll, dispatch each message, delete only the ones that
    succeeded.

    ``client`` is duck-typed (``receive_message(QueueUrl=, MaxNumberOfMessages=, WaitTimeSeconds=,
    MessageAttributeNames=)`` → ``{"Messages": [...]}``; ``delete_message(QueueUrl=, ReceiptHandle=)``).
    With ``delete=True`` (the default, at-least-once) a message is deleted only after a **successful**
    result, so a failed message is left on the queue individually for redelivery/DLQ redrive rather
    than lost with the rest of the batch; a caller wanting different semantics passes ``delete=False``
    and deletes from ``on_result``. ``wait_time_seconds`` defaults to 20 (SQS's maximum long-poll), so
    an empty queue costs one mostly-idle request rather than a tight poll loop. ``should_continue``
    bounds the loop (a real worker loops forever; a test stops after N polls).
    """
    while should_continue():
        response = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=wait_time_seconds,
            MessageAttributeNames=["All"],
        )
        for message in response.get("Messages") or []:
            result = await app.handle_message(message)
            if on_result is not None:
                on_result(message, result)
            # At-least-once: delete only a successful outcome, so a failed message is redelivered.
            if delete and result.is_successful:
                client.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
