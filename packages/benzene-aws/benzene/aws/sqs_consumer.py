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
**no response channel** — one message is one pipeline invocation and one DI scope, and the outcome
**settles** the message (transport-bindings §1, obligation 8): deleted when its invocation ended
``isSuccessful`` (``ignored`` included), otherwise left on the queue — a failure, a missing result, an
unmatched topic — to reappear after its visibility timeout and go to the queue's redrive/DLQ. The unit
of settlement is the one message. ``SqsConsumerApp(..., settlement=Settlement(at_most_once=True))``
deletes whatever the outcome; ``Settlement(ignore_unmatched_topics=True)`` deletes a topic no handler
serves as ``ignored``.

``client`` is duck-typed against ``boto3``'s SQS client (``receive_message``/``delete_message``), so
the binding — decode, per-message dispatch, and the poll loop — is exercised in memory with fakes and
needs neither a queue nor the SDK. ``boto3`` is only needed for the real client and stays an optional
dependency, exactly like :class:`~benzene.aws.clients.SqsMessageSender`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, ClassVar

from benzene.core import (
    DEFAULT_METADATA_KEYS,
    QUEUE,
    AppDefinition,
    BenzeneMessageApplication,
    MetadataKeys,
    Settlement,
    SettlementDefaults,
    StopSignal,
    Worker,
    application_from,
    dispatch_one_way,
    read_message_metadata,
    redelivers,
)
from benzene.results import Result


def _decode_attributes(raw: dict[str, Any] | None) -> dict[str, str]:
    """``receive_message``'s ``MessageAttributes`` (``StringValue``/``DataType``) → the flat string dict."""
    attributes: dict[str, str] = {}
    for key, value in (raw or {}).items():
        attributes[key] = value.get("StringValue") or ""
    return attributes


def decode_sqs_message(
    message: dict[str, Any], *, keys: MetadataKeys = DEFAULT_METADATA_KEYS
) -> dict[str, Any]:
    """Decode one ``receive_message()`` message dict into a Benzene envelope ``{topic, headers, body}``.

    The Benzene topic is resolved out of the ``topic`` (``keys.topic``) message attribute; every other attribute becomes
    a Benzene header. Note this is **not** the same shape as a Lambda SQS-trigger record
    (:func:`benzene.aws.events.sqs_record_envelope`) — ``receive_message``'s ``MessageAttributes`` uses
    ``StringValue``/``DataType`` where a Lambda event's ``messageAttributes`` uses lower-cased keys.
    """
    topic, headers = read_message_metadata(_decode_attributes(message.get("MessageAttributes")), keys)
    return {"topic": topic, "headers": headers, "body": message.get("Body") or ""}


class SqsConsumerApp:
    """Runs one SQS message through the Benzene pipeline (one message → one invocation → one scope).

    :meth:`handle_message` returns the invocation's :class:`~benzene.results.Result` — decoded from
    the response envelope with its ``isSuccessful`` authoritative, never re-derived from the status
    text — for the loop to *settle* (delete / leave for redelivery), not to send back to a caller. A
    malformed body is a ``bad-request`` and a handler fault a ``service-unavailable``, so content never
    crashes the poll loop; an exception escaping a middleware does (the message stays undeleted),
    unless ``settlement.at_most_once``.
    """

    SETTLEMENT_DEFAULTS: ClassVar[tuple[SettlementDefaults, ...]] = (redelivers("sqs", QUEUE),)

    def __init__(
        self, application: BenzeneMessageApplication, *, settlement: Settlement = Settlement()
    ) -> None:
        self._application = application
        self._settlement = settlement

    @classmethod
    def from_definition(
        cls, definition: AppDefinition, *, settlement: Settlement = Settlement()
    ) -> SqsConsumerApp:
        """Build the consumer app from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(application_from(definition), settlement=settlement)

    @property
    def settlement(self) -> Settlement:
        """How the loop settles each message: deleted iff ``settlement.acknowledges(result)``."""
        return self._settlement

    async def handle_message(self, message: dict[str, Any]) -> Result:
        outcome = await dispatch_one_way(
            self._application,
            self._settlement,
            decode_sqs_message,
            message,
            keys=self._application.metadata_keys,
        )
        return outcome.result


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
    With ``delete=True`` (the default) a message is deleted only when ``app.settlement`` acknowledges
    its result — by default a **successful** one, so a failed message is left on the queue
    individually for redelivery/DLQ redrive rather than lost with the rest of the batch (set
    at-most-once on the app's ``Settlement`` to delete everything); a caller deleting by hand passes
    ``delete=False`` and deletes from ``on_result``. ``wait_time_seconds`` defaults to 20 (SQS's maximum long-poll), so
    an empty queue costs one mostly-idle request rather than a tight poll loop. ``should_continue``
    bounds the loop (a real worker loops forever; a test stops after N polls).

    ``client.receive_message``/``client.delete_message`` are plain synchronous ``boto3`` calls, run via
    :func:`asyncio.to_thread` rather than called directly on the event loop - called directly, a
    20-second long-poll would block every other coroutine on the loop for its duration (worse for
    Kafka's poll loop, which has no timeout to bound it at all), starving anything else sharing the
    process, e.g. an HTTP server hosted alongside this consumer. See
    ``docs/getting-started-kubernetes.md`` for the multi-transport-in-one-process story this makes
    possible.
    """
    while should_continue():
        response = await asyncio.to_thread(
            client.receive_message,
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=wait_time_seconds,
            MessageAttributeNames=["All"],
        )
        for message in response.get("Messages") or []:
            result = await app.handle_message(message)
            if on_result is not None:
                on_result(message, result)
            # Settlement: delete only what the app's Settlement acknowledges - by default a successful
            # outcome, so a failed message reappears and the queue's redrive policy decides its fate.
            if delete and app.settlement.acknowledges(result):
                await asyncio.to_thread(
                    client.delete_message, QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )


def sqs_consumer_worker(
    app: SqsConsumerApp,
    client: Any,
    queue_url: str,
    **loop_options: Any,
) -> Worker:
    """This consumer as one leg of a :class:`~benzene.core.WorkerHost` — for SQS *alongside* HTTP.

    Reach for this only when the process runs more than one transport; a queue-only worker just
    awaits :func:`run_sqs_consumer_loop` directly and needs nothing from here.

    **The explicit form this composes** is two lines you can write yourself, and it is the whole
    implementation — there is no privileged path::

        async def worker(stop):
            await run_sqs_consumer_loop(app, client, queue_url,
                                        should_continue=stop.should_continue)

    ``**loop_options`` are passed straight through to :func:`run_sqs_consumer_loop`
    (``wait_time_seconds``, ``delete``, ``on_result``, ...), so anything you can do there you can do
    here. Passing ``should_continue`` is refused: the host owns that, and silently ignoring your
    callback would be worse than saying so.
    """
    if "should_continue" in loop_options:
        raise TypeError(
            "sqs_consumer_worker() does not take should_continue - the WorkerHost supplies it, so "
            "one leg finishing winds the others down. To bound the loop yourself, call "
            "run_sqs_consumer_loop(app, client, queue_url, should_continue=...) directly instead."
        )

    async def worker(stop: StopSignal) -> None:
        await run_sqs_consumer_loop(
            app, client, queue_url, should_continue=stop.should_continue, **loop_options
        )

    return worker
