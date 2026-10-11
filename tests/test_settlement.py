"""Settlement on every one-way binding (transport-bindings §1, obligation 8).

One table of outcomes, run through every one-way entry point this port ships. By default a message is
acknowledged only when its invocation ended ``isSuccessful`` — ``ignored`` and an application-defined
success included — and a failure, a missing result, an unmatched topic or an uncaught error is handed
back for redelivery. ``Settlement(at_most_once=True)`` acknowledges everything;
``Settlement(ignore_unmatched_topics=True)`` acknowledges a topic no handler serves, and only that.

Each binding's declared ``SETTLEMENT_DEFAULTS`` are checked against the conformance fixture by the
runner; these tests are what make those constants true of the code.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import pytest
from benzene.aws import AwsLambdaApp, SqsConsumerApp, run_sqs_consumer_loop
from benzene.aws.testing import (
    DynamoDbStreamBuilder,
    EventBridgeEventBuilder,
    KafkaLambdaEventBuilder,
    KinesisEventBuilder,
    S3EventBuilder,
    SnsEventBuilder,
    SqsEventBuilder,
    SqsMessageBuilder,
)
from benzene.azure import AzureFunctionsApp
from benzene.azure.testing import (
    cosmos_change_feed,
    event_grid_event,
    event_hub_event,
    queue_storage_message,
    service_bus_message,
    timer_request,
)
from benzene.core import (
    BenzeneMessageApplication,
    Context,
    MessageHandlingError,
    MiddlewarePipeline,
    Next,
    Registry,
    Settlement,
)
from benzene.gcp import GcpFunctionsApp
from benzene.gcp.testing import PubSubEventBuilder
from benzene.kafka import KafkaConsumerApp, run_consumer_loop
from benzene.kafka.testing import KafkaMessageBuilder, RecordingKafkaConsumer
from benzene.rabbitmq import RabbitMqConsumerApp
from benzene.rabbitmq import run_consumer_loop as run_rabbitmq_loop
from benzene.results import Result

AT_MOST_ONCE = Settlement(at_most_once=True)
IGNORE_UNMATCHED = Settlement(ignore_unmatched_topics=True)


class Boom(RuntimeError):
    """An error escaping the pipeline (a middleware's, not a handler's — the router catches those)."""


async def _raises_or_drops(context: Context, next: Next) -> None:
    if context.topic == "raises":
        raise Boom("a middleware fault")
    if context.topic == "no-result":
        return  # ends the pipeline without setting a result
    await next()


def _application() -> BenzeneMessageApplication:
    async def ok(_request: Any) -> Result:
        return Result.ok({"done": True})

    async def ignored(_request: Any) -> Result:
        return Result.ignored()

    async def fails(_request: Any) -> Result:
        return Result.conflict("try again later")

    async def app_success(_request: Any) -> Result:
        return Result.set("cache-warm", None, successful=True)

    async def versioned(_request: Any) -> Result:
        return Result.ok()

    registry = (
        Registry()
        .register("succeeds", ok)
        .register("ignored", ignored)
        .register("fails", fails)
        .register("app-success", app_success)
        .register("versioned", versioned, version="v1", default=True)
    )
    return BenzeneMessageApplication(registry, MiddlewarePipeline([_raises_or_drops]))


#: (topic, headers, acknowledged by default?)
OUTCOMES = [
    ("succeeds", {}, True),
    ("ignored", {}, True),
    ("app-success", {}, True),  # isSuccessful decides - never the status text (wire §1.2)
    ("fails", {}, False),
    ("no-result", {}, False),
    ("nobody-serves-this", {}, False),
    ("versioned", {"benzene-version": "v9"}, False),
    ("raises", {}, False),
]

# --- one probe per one-way binding: deliver a message, report whether it was acknowledged --------

Deliver = Callable[[Settlement, str, dict[str, str]], bool]


def _raised(run: Callable[[], Any]) -> bool:
    """True when the trigger function returned normally (acknowledged), False when it raised."""
    try:
        run()
    except (MessageHandlingError, Boom):
        return False
    return True


def _lambda(settlement: Settlement, **options: Any) -> AwsLambdaApp:
    return AwsLambdaApp(application=_application(), settlement=settlement, **options)


def _sqs(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    event = SqsEventBuilder().with_message(topic, {}, message_id="m-1", headers=headers).build()
    try:
        response = _lambda(settlement).handle(event)
    except Boom:
        return False  # the whole batch fails, so this record is redelivered
    assert response is not None
    return response["batchItemFailures"] == []


def _sns(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    event = SnsEventBuilder().with_message(topic, {}, headers=headers).build()
    return _raised(lambda: _lambda(settlement).handle(event))


def _s3(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    event = S3EventBuilder().with_object("bucket", "key").build()
    return _raised(lambda: _lambda(settlement, s3_topic=topic).handle(event))


def _eventbridge(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    event = EventBridgeEventBuilder(topic).with_detail({"benzene-headers": headers}).build()
    return _raised(lambda: _lambda(settlement).handle(event))


def _msk(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    event = KafkaLambdaEventBuilder().with_message(topic, {}, headers=headers).build()
    return _raised(lambda: _lambda(settlement).handle(event))


def _dynamodb(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    event = DynamoDbStreamBuilder().with_record("INSERT", {"id": {"S": "1"}}, sequence_number="7").build()
    try:
        response = _lambda(settlement, dynamodb_topic=topic).handle(event)
    except Boom:
        return False
    assert response is not None
    return response["batchItemFailures"] == []


def _kinesis(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    event = KinesisEventBuilder().with_record({}, sequence_number="7").build()
    try:
        response = _lambda(settlement, kinesis_topic=topic).handle(event)
    except Boom:
        return False
    assert response is not None
    return response["batchItemFailures"] == []


def _async_invoke(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    event = {"topic": topic, "headers": headers, "body": "{}"}
    return _raised(lambda: _lambda(settlement, invocation_type="Event").handle(event))


def _azure(settlement: Settlement) -> AzureFunctionsApp:
    return AzureFunctionsApp(application=_application(), settlement=settlement)


def _service_bus(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    return _raised(lambda: _azure(settlement).handle_service_bus(service_bus_message(topic, {}, headers)))


def _event_hub(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    return _raised(lambda: _azure(settlement).handle_event_hub([event_hub_event(topic, {}, headers)]))


def _queue_storage(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    message = queue_storage_message(topic, {}, headers)
    return _raised(lambda: _azure(settlement).handle_queue_storage(message))


def _cosmos(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    documents = cosmos_change_feed({"id": "1"})
    return _raised(lambda: _azure(settlement).handle_cosmos(documents, default_topic=topic))


def _timer(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    return _raised(lambda: _azure(settlement).handle_timer(timer_request(), default_topic=topic))


def _event_grid(settlement: Settlement, topic: str, _headers: dict[str, str]) -> bool:
    return _raised(lambda: _azure(settlement).handle_event_grid(event_grid_event(topic, {})))


def _pubsub(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    builder = PubSubEventBuilder(topic).with_body({})
    for key, value in headers.items():
        builder.with_attribute(key, value)
    app = GcpFunctionsApp(application=_application(), settlement=settlement)
    return _raised(lambda: app.handle_pubsub(builder.build()))


class _SqsClient:
    def __init__(self, message: dict[str, Any]) -> None:
        self._messages = [message]
        self.deleted: list[str] = []

    def receive_message(self, **_kwargs: Any) -> dict[str, Any]:
        messages, self._messages = self._messages, []
        return {"Messages": messages}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:  # noqa: N803 - boto3
        self.deleted.append(ReceiptHandle)


def _sqs_consumer(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    builder = SqsMessageBuilder(topic).with_body({})
    for key, value in headers.items():
        builder.with_header(key, value)
    client = _SqsClient(builder.build())
    polls = iter([True, False])
    app = SqsConsumerApp(_application(), settlement=settlement)
    try:
        asyncio.run(
            run_sqs_consumer_loop(app, client, "q", should_continue=lambda: next(polls))
        )
    except Boom:
        return False
    return client.deleted != []


def _kafka_consumer(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    builder = KafkaMessageBuilder(topic).with_body({})
    for key, value in headers.items():
        builder.with_header(key, value)
    consumer = RecordingKafkaConsumer([builder.build()])
    polls = iter([True, False])
    app = KafkaConsumerApp(_application(), settlement=settlement)
    try:
        asyncio.run(run_consumer_loop(app, consumer, should_continue=lambda: next(polls)))
    except Boom:
        return False
    return consumer.committed != []


class _Method:
    delivery_tag = 1


class _Properties:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers


class _Channel:
    def __init__(self, topic: str, headers: dict[str, str]) -> None:
        self._deliveries = [(_Method(), _Properties({"topic": topic, **headers}), b"{}")]
        self.acked: list[int] = []
        self.nacked: list[tuple[int, bool]] = []

    def basic_get(self, _queue: str) -> tuple[Any, Any, Any]:
        return self._deliveries.pop(0) if self._deliveries else (None, None, None)

    def basic_ack(self, *, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(self, *, delivery_tag: int, requeue: bool) -> None:
        self.nacked.append((delivery_tag, requeue))


def _rabbitmq(settlement: Settlement, topic: str, headers: dict[str, str]) -> bool:
    channel = _Channel(topic, headers)
    polls = iter([True, False])
    app = RabbitMqConsumerApp(_application(), settlement=settlement)
    try:
        asyncio.run(run_rabbitmq_loop(app, channel, should_continue=lambda: next(polls)))
    except Boom:
        return False
    if channel.acked:
        return True
    assert channel.nacked == [(1, True)]  # declined, and requeued for redelivery
    return False


#: Bindings whose topic travels with the message (so a version header can ride along).
HEADER_BINDINGS: dict[str, Deliver] = {
    "lambda-sqs": _sqs,
    "lambda-sns": _sns,
    "lambda-eventbridge": _eventbridge,
    "lambda-msk": _msk,
    "lambda-invoke-event": _async_invoke,
    "azure-servicebus": _service_bus,
    "azure-eventhub": _event_hub,
    "azure-queue-storage": _queue_storage,
    "gcp-pubsub": _pubsub,
    "sqs-consumer": _sqs_consumer,
    "rabbitmq-consumer": _rabbitmq,
}

#: Bindings whose topic is a configured convention (no per-message headers to carry a version).
CONVENTION_BINDINGS: dict[str, Deliver] = {
    "lambda-s3": _s3,
    "lambda-dynamodb": _dynamodb,
    "lambda-kinesis": _kinesis,
    "azure-cosmos": _cosmos,
    "azure-timer": _timer,
    "azure-event-grid": _event_grid,
}

ALL_BINDINGS = {**HEADER_BINDINGS, **CONVENTION_BINDINGS}


def _cases(bindings: dict[str, Deliver]) -> list[Any]:
    return [
        pytest.param(deliver, topic, headers, expected, id=f"{name}-{topic}-{bool(headers)}")
        for name, deliver in bindings.items()
        for topic, headers, expected in OUTCOMES
        if not headers or bindings is HEADER_BINDINGS
    ]


@pytest.mark.parametrize("deliver,topic,headers,expected", _cases(HEADER_BINDINGS) + _cases(CONVENTION_BINDINGS))
def test_by_default_only_a_successful_invocation_is_acknowledged(
    deliver: Deliver, topic: str, headers: dict[str, str], expected: bool
) -> None:
    assert deliver(Settlement(), topic, headers) is expected


@pytest.mark.parametrize("deliver,topic,headers,_expected", _cases(HEADER_BINDINGS) + _cases(CONVENTION_BINDINGS))
def test_at_most_once_acknowledges_whatever_the_outcome(
    deliver: Deliver, topic: str, headers: dict[str, str], _expected: bool, caplog: Any
) -> None:
    with caplog.at_level(logging.WARNING, logger="benzene.settlement"):
        assert deliver(AT_MOST_ONCE, topic, headers) is True


@pytest.mark.parametrize("deliver", ALL_BINDINGS.values(), ids=ALL_BINDINGS.keys())
def test_ignore_unmatched_topics_acknowledges_a_topic_nobody_serves(deliver: Deliver) -> None:
    assert deliver(IGNORE_UNMATCHED, "nobody-serves-this", {}) is True
    # ...and only that: a failure is still a failure.
    assert deliver(IGNORE_UNMATCHED, "fails", {}) is False


@pytest.mark.parametrize("deliver", HEADER_BINDINGS.values(), ids=HEADER_BINDINGS.keys())
def test_ignore_unmatched_topics_still_redelivers_a_known_topic_at_an_unknown_version(
    deliver: Deliver,
) -> None:
    assert deliver(IGNORE_UNMATCHED, "versioned", {"benzene-version": "v9"}) is False


def test_at_most_once_logs_the_failure_it_drops(caplog: Any) -> None:
    with caplog.at_level(logging.WARNING, logger="benzene.settlement"):
        assert _sns(AT_MOST_ONCE, "fails", {}) is True
    assert "acknowledged a failed message on topic 'fails' (conflict)" in caplog.text


# --- the self-hosted Kafka loop: skip-and-continue by default, at-least-once on request -----------


def _kafka_records(*topics: str) -> list[Any]:
    return [KafkaMessageBuilder(topic).with_body({}).build() for topic in topics]


def test_kafka_loop_skips_a_failed_record_and_continues_by_default() -> None:
    records = _kafka_records("fails", "succeeds")
    consumer = RecordingKafkaConsumer(list(records))
    polls = iter([True, True, False])
    app = KafkaConsumerApp(_application())

    asyncio.run(run_consumer_loop(app, consumer, should_continue=lambda: next(polls)))

    # The failed record is not committed itself, but the loop moved on and committed the next one -
    # which, offsets being cumulative per partition, passes the failed record (skip-and-continue).
    assert consumer.committed == [records[1]]


def test_kafka_loop_stop_on_failure_is_at_least_once() -> None:
    records = _kafka_records("succeeds", "fails", "succeeds")
    consumer = RecordingKafkaConsumer(list(records))
    app = KafkaConsumerApp(_application())

    with pytest.raises(MessageHandlingError, match="'fails'"):
        asyncio.run(run_consumer_loop(app, consumer, stop_on_failure=True))

    # Stopped at the failed record without committing it or anything after it, so a restart resumes
    # there and redelivers it.
    assert consumer.committed == [records[0]]


def test_lambda_direct_invoke_is_synchronous_unless_configured_asynchronous() -> None:
    event = {"topic": "fails", "headers": {}, "body": "{}"}
    response = _lambda(Settlement()).handle(event)  # a caller is waiting: it gets the envelope
    assert response is not None and response["statusCode"] == "conflict"
    with pytest.raises(MessageHandlingError):
        _lambda(Settlement(), invocation_type="Event").handle(event)


def test_lambda_rejects_an_unknown_invocation_type() -> None:
    with pytest.raises(ValueError, match="invocation_type='Async'"):
        AwsLambdaApp(application=_application(), invocation_type="Async")  # type: ignore[arg-type]


def test_a_failed_trigger_names_the_topic_and_status() -> None:
    with pytest.raises(MessageHandlingError) as raised:
        _azure(Settlement()).handle_service_bus(service_bus_message("fails", {}, {}))
    assert raised.value.topic == "fails"
    assert raised.value.status == "conflict"
    assert "try again later" in raised.value.detail


def test_the_test_harness_builds_a_one_way_host_with_a_settlement() -> None:
    from benzene.core import AppDefinition, BenzeneStartUp
    from benzene.testing import create_test_host

    class StartUp(BenzeneStartUp):
        def configure(self, services: Any, config: Any) -> AppDefinition:
            return AppDefinition(registry=Registry())

    plain = create_test_host(StartUp).build_aws()
    with pytest.raises(MessageHandlingError):
        plain.send_sns("nobody-serves-this", {})  # not-found: redelivered

    shared_bus = create_test_host(StartUp).with_settlement(IGNORE_UNMATCHED).build_aws()
    shared_bus.send_sns("nobody-serves-this", {})  # acknowledged as ignored
