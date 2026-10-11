"""The transport bindings this port ships, driven through their real hosts for the conformance runner.

``transport-metadata-cases.json`` is written against a neutral string->string metadata dictionary and
leaves each runner to adapt it to its own native shapes (the fixture's own words). This module is that
adaptation: for every ``topicSources`` binding id, the native message each host of that binding
receives, built from the neutral dictionary, and the native metadata each outbound client writes, read
back into one. Nothing here decodes on the binding's behalf - every delivery goes through the host's
own entry point (``AwsLambdaApp.handle``, ``KafkaConsumerApp.handle_message``, ...) into an application
whose only middleware records what the pipeline saw, so a binding that resolved the topic wrongly, or
ignored the application's :class:`~benzene.core.MetadataKeys`, fails the case.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from benzene.aws import AwsLambdaApp, SnsMessageSender, SqsConsumerApp, SqsMessageSender
from benzene.azure import AzureFunctionsApp, EventHubMessageSender, ServiceBusMessageSender
from benzene.core import (
    BenzeneMessageApplication,
    Context,
    MetadataKeys,
    MiddlewarePipeline,
    Next,
    Registry,
)
from benzene.gcp import GcpFunctionsApp, PubSubMessageSender
from benzene.http import BenzeneHttpApp, HttpRouter
from benzene.kafka import KafkaConsumerApp, KafkaMessageSender
from benzene.rabbitmq import RabbitMqConsumerApp, RabbitMqMessageSender
from benzene.results import Result

#: The topic the non-metadata sources are asked to resolve (the fixture's own example topic).
TOPIC = "conformance:greet"


@dataclass(frozen=True)
class Seen:
    """What the pipeline saw for one delivery: the resolved topic, the headers and the version."""

    topic: str
    headers: dict[str, str]
    version: str


class _Recorder:
    """An application whose only middleware records each invocation and answers it ``ok``."""

    def __init__(self, keys: MetadataKeys) -> None:
        self.seen: list[Seen] = []

        async def record(context: Context, _next: Next) -> None:
            self.seen.append(Seen(context.topic, dict(context.headers), context.version))
            context.result = Result.ok()

        self.application = BenzeneMessageApplication(
            Registry(), MiddlewarePipeline([record]), metadata_keys=keys
        )

    def one(self) -> Seen:
        if len(self.seen) != 1:
            raise AssertionError(f"expected one pipeline invocation, saw {len(self.seen)}")
        return self.seen[0]


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# --- inbound: neutral metadata -> each host's native message -> what the pipeline saw ------------

Deliver = Callable[[dict[str, str], MetadataKeys], Seen]


def _lambda_sqs(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    attributes = {k: {"stringValue": v, "dataType": "String"} for k, v in metadata.items()}
    record = {
        "eventSource": "aws:sqs",
        "messageId": "m-1",
        "body": "{}",
        "messageAttributes": attributes,
    }
    AwsLambdaApp(application=recorder.application).handle({"Records": [record]})
    return recorder.one()


def _sqs_consumer(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    attributes = {k: {"StringValue": v, "DataType": "String"} for k, v in metadata.items()}
    message = {"MessageId": "m-1", "Body": "{}", "MessageAttributes": attributes}
    asyncio.run(SqsConsumerApp(recorder.application).handle_message(message))
    return recorder.one()


def _lambda_sns(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    attributes = {k: {"Type": "String", "Value": v} for k, v in metadata.items()}
    record = {"EventSource": "aws:sns", "Sns": {"Message": "{}", "MessageAttributes": attributes}}
    AwsLambdaApp(application=recorder.application).handle({"Records": [record]})
    return recorder.one()


def _pubsub(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    event = SimpleNamespace(data={"message": {"data": _b64("{}"), "attributes": dict(metadata)}})
    GcpFunctionsApp(application=recorder.application).handle_pubsub(event)
    return recorder.one()


def _service_bus(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    message = SimpleNamespace(application_properties=dict(metadata), get_body=lambda: b"{}")
    AzureFunctionsApp(application=recorder.application).handle_service_bus(message)
    return recorder.one()


def _event_hub(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    event = SimpleNamespace(properties=dict(metadata), get_body=lambda: b"{}")
    AzureFunctionsApp(application=recorder.application).handle_event_hub([event])
    return recorder.one()


def _kafka(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    recorder = _Recorder(keys)
    headers = [(k, v.encode("utf-8")) for k, v in metadata.items()]
    message = SimpleNamespace(headers=lambda: headers, value=lambda: b"{}", error=lambda: None)
    asyncio.run(KafkaConsumerApp(recorder.application).handle_message(message))
    return recorder.one()


def _lambda_msk(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    # The record sits on a physical Kafka topic of its own: Kafka names no fallback (topicSources),
    # so that topic must never surface as the Benzene topic.
    recorder = _Recorder(keys)
    record = {
        "topic": "physical-kafka-topic",
        "partition": 0,
        "offset": 0,
        "headers": [{k: list(v.encode("utf-8"))} for k, v in metadata.items()],
        "value": _b64("{}"),
    }
    event = {"eventSource": "aws:kafka", "records": {"physical-kafka-topic-0": [record]}}
    AwsLambdaApp(application=recorder.application).handle(event)
    return recorder.one()


def rabbitmq_delivery(metadata: dict[str, str], keys: MetadataKeys, routing_key: str = "") -> Seen:
    """One RabbitMQ delivery. The metadata cases send it with no routing key, so the headers alone
    decide; the routing-key fallback is checked on its own (``topicSources``)."""
    recorder = _Recorder(keys)
    method = SimpleNamespace(delivery_tag=1, routing_key=routing_key)
    properties = SimpleNamespace(headers=dict(metadata))
    asyncio.run(RabbitMqConsumerApp(recorder.application).handle_message(method, properties, b"{}"))
    return recorder.one()


def _rabbitmq(metadata: dict[str, str], keys: MetadataKeys) -> Seen:
    return rabbitmq_delivery(metadata, keys)


#: Every host of every metadata-sourced binding this port ships, keyed by its ``topicSources`` id.
METADATA_HOSTS: dict[str, dict[str, Deliver]] = {
    "sqs": {"AwsLambdaApp": _lambda_sqs, "SqsConsumerApp": _sqs_consumer},
    "sns": {"AwsLambdaApp": _lambda_sns},
    "pubsub": {"GcpFunctionsApp": _pubsub},
    "servicebus": {"AzureFunctionsApp": _service_bus},
    "eventhub": {"AzureFunctionsApp": _event_hub},
    "kafka": {"KafkaConsumerApp": _kafka, "AwsLambdaApp (MSK)": _lambda_msk},
    "rabbitmq": {"RabbitMqConsumerApp": _rabbitmq},
}


# --- the sources that are not metadata ------------------------------------------------------------


def eventbridge_event(detail_type: str, keys: MetadataKeys) -> Seen:
    """An EventBridge event carrying no topic attribute at all - only its ``detail-type``."""
    recorder = _Recorder(keys)
    event = {"detail-type": detail_type, "source": "conformance", "detail": {"name": "x"}}
    AwsLambdaApp(application=recorder.application).handle(event)
    return recorder.one()


def dynamodb_record(table_name: str, event_name: str, keys: MetadataKeys) -> Seen:
    """A DynamoDB Streams record on ``table_name``'s stream - no metadata, no producer."""
    recorder = _Recorder(keys)
    record = {
        "eventSource": "aws:dynamodb",
        "eventSourceARN": (
            f"arn:aws:dynamodb:us-east-1:000000000000:table/{table_name}/stream/2024-01-01T00:00:00.000"
        ),
        "eventName": event_name,
        "dynamodb": {"Keys": {"id": {"S": "1"}}, "SequenceNumber": "1"},
    }
    AwsLambdaApp(application=recorder.application).handle({"Records": [record]})
    return recorder.one()


def http_route(topic: str, keys: MetadataKeys) -> Seen:
    """A request on an HTTP route mapped to ``topic`` - the topic comes from the route."""
    recorder = _Recorder(keys)
    router = HttpRouter()

    async def unused(_request: Any) -> Result:  # the recorder answers before the router runs
        return Result.ok()

    router.register("POST", "/conformance/greet", topic, unused)
    app = BenzeneHttpApp(router, application=recorder.application)
    asyncio.run(app.handle("POST", "/conformance/greet", body="{}"))
    return recorder.one()


def grpc_method(topic: str, keys: MetadataKeys) -> Seen | None:
    """A unary call on ``topic``'s gRPC method - the topic comes from the method. ``None`` when
    grpcio is not installed (the transport's ``[transport]`` extra)."""
    try:
        from benzene.grpc.server import BenzeneGrpcHandler, method_for
    except ImportError:
        return None
    recorder = _Recorder(keys)
    handler = BenzeneGrpcHandler(recorder.application)
    rpc = handler.service(SimpleNamespace(method=method_for(topic)))
    context = SimpleNamespace(
        invocation_metadata=lambda: (),
        set_code=lambda _code: None,
        set_details=lambda _details: None,
        set_trailing_metadata=lambda _trailers: None,
    )
    rpc.unary_unary(b"{}", context)
    return recorder.one()


# --- outbound: what each client writes into the native metadata channel ---------------------------

Write = Callable[[str, dict[str, str], MetadataKeys], "dict[str, str] | None"]


class _Calls:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def record(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        return SimpleNamespace(result=lambda: None)


def _send(sender: Any, topic: str, headers: dict[str, str]) -> None:
    result = asyncio.run(sender.send_message(topic, {}, headers))
    if not result.is_successful:
        raise AssertionError(f"{type(sender).__name__} failed to send: {result.status}")


def _write_sns(topic: str, headers: dict[str, str], keys: MetadataKeys) -> dict[str, str]:
    fake = _Calls()
    _send(
        SnsMessageSender("arn", client=SimpleNamespace(publish=fake.record), metadata_keys=keys),
        topic,
        headers,
    )
    return {k: v["StringValue"] for k, v in fake.calls[0][1]["MessageAttributes"].items()}


def _write_sqs(topic: str, headers: dict[str, str], keys: MetadataKeys) -> dict[str, str]:
    fake = _Calls()
    client = SimpleNamespace(send_message=fake.record)
    _send(SqsMessageSender("url", client=client, metadata_keys=keys), topic, headers)
    return {k: v["StringValue"] for k, v in fake.calls[0][1]["MessageAttributes"].items()}


def _write_pubsub(topic: str, headers: dict[str, str], keys: MetadataKeys) -> dict[str, str]:
    fake = _Calls()
    sender = PubSubMessageSender(
        "projects/p/topics/t", publisher=SimpleNamespace(publish=fake.record), metadata_keys=keys
    )
    _send(sender, topic, headers)
    return dict(fake.calls[0][1])


def _text(value: Any) -> str:
    return bytes(value).decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)


def _write_service_bus(
    topic: str, headers: dict[str, str], keys: MetadataKeys
) -> dict[str, str] | None:
    try:
        import azure.servicebus  # noqa: F401 - the message object is the SDK's own
    except ImportError:
        return None
    fake = _Calls()
    _send(
        ServiceBusMessageSender(
            sender=SimpleNamespace(send_messages=fake.record), metadata_keys=keys
        ),
        topic,
        headers,
    )
    message = fake.calls[0][0][0]
    return {_text(k): _text(v) for k, v in (message.application_properties or {}).items()}


def _write_event_hub(
    topic: str, headers: dict[str, str], keys: MetadataKeys
) -> dict[str, str] | None:
    try:
        import azure.eventhub  # noqa: F401 - the event object is the SDK's own
    except ImportError:
        return None
    batch: list[Any] = []
    producer = SimpleNamespace(
        create_batch=lambda: SimpleNamespace(add=batch.append), send_batch=lambda _batch: None
    )
    _send(EventHubMessageSender(producer=producer, metadata_keys=keys), topic, headers)
    return {_text(k): _text(v) for k, v in (batch[0].properties or {}).items()}


def _write_kafka(topic: str, headers: dict[str, str], keys: MetadataKeys) -> dict[str, str]:
    fake = _Calls()
    producer = SimpleNamespace(produce=fake.record, flush=lambda _timeout: 0)
    _send(KafkaMessageSender("physical", producer=producer, metadata_keys=keys), topic, headers)
    return {k: _text(v) for k, v in fake.calls[0][1]["headers"]}


def _write_rabbitmq(topic: str, headers: dict[str, str], keys: MetadataKeys) -> dict[str, str]:
    fake = _Calls()
    sender = RabbitMqMessageSender(
        channel=SimpleNamespace(basic_publish=fake.record), metadata_keys=keys
    )
    _send(sender, topic, headers)
    return {k: _text(v) for k, v in (fake.calls[0][1]["properties"].headers or {}).items()}


#: Every outbound client of a metadata-sourced binding this port ships, keyed by ``topicSources`` id.
#: A writer returns ``None`` when its SDK (whose message object is the wire contract) is absent.
METADATA_CLIENTS: dict[str, dict[str, Write]] = {
    "sqs": {"SqsMessageSender": _write_sqs},
    "sns": {"SnsMessageSender": _write_sns},
    "pubsub": {"PubSubMessageSender": _write_pubsub},
    "servicebus": {"ServiceBusMessageSender": _write_service_bus},
    "eventhub": {"EventHubMessageSender": _write_event_hub},
    "kafka": {"KafkaMessageSender": _write_kafka},
    "rabbitmq": {"RabbitMqMessageSender": _write_rabbitmq},
}


def describe(value: Any) -> str:
    return json.dumps(value, sort_keys=True)
