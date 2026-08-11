"""The self-hosted SQS consumer — inbound decode + poll loop (no queue, no boto3).

Drives the real binding logic against duck-typed fakes: a message decodes to the Benzene envelope
with the topic lifted from the `topic` message attribute, the poll loop dispatches one message per
scope and deletes only successful outcomes (at-least-once), and a poison message never crashes the
loop. Distinct from the Lambda SQS-trigger binding covered by test_aws_host.py — this is the
standalone-worker poller (benzene.aws.sqs_consumer), a different code path with a different wire
shape (receive_message's MessageAttributes, not a Lambda event's messageAttributes).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from benzene.aws import SqsConsumerApp, decode_sqs_message, run_sqs_consumer_loop
from benzene.aws.testing import RecordingSqsClient, SqsMessageBuilder
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.results import Result, Status


@dataclass
class PlaceOrder:
    sku: str = ""


def _app(handler=None) -> BenzeneMessageApplication:
    async def default(request: PlaceOrder) -> Result:
        return Result.created({"sku": request.sku})

    registry = Registry().register("orders:place", handler or default, request_type=PlaceOrder)
    return BenzeneMessageApplication(registry, MiddlewarePipeline())


# --- inbound decode ------------------------------------------------------------------------------


def test_decode_lifts_topic_from_the_message_attribute_and_keeps_the_rest() -> None:
    message = (
        SqsMessageBuilder("orders:place")
        .with_header("x-correlation-id", "c1")
        .with_body({"sku": "A"})
        .build()
    )
    envelope = decode_sqs_message(message)
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"] == {"x-correlation-id": "c1"}  # topic removed, rest preserved
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_tolerates_absent_attributes_and_body() -> None:
    envelope = decode_sqs_message({"MessageId": "m1", "ReceiptHandle": "r1"})
    assert envelope == {"topic": "", "headers": {}, "body": ""}


# --- consumer dispatch + loop ---------------------------------------------------------------------


def test_handle_message_runs_the_pipeline_and_maps_the_result() -> None:
    app = SqsConsumerApp(_app())
    message = SqsMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))
    assert result.status == Status.CREATED


def test_a_poison_message_never_raises() -> None:
    async def boom(_request: PlaceOrder) -> Result:
        raise RuntimeError("handler blew up")

    app = SqsConsumerApp(_app(boom))
    message = SqsMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))  # must not raise
    assert not result.is_successful  # the fault is a failure result, not a crash


def test_loop_deletes_only_successful_messages() -> None:
    async def flaky(request: PlaceOrder) -> Result:
        return (
            Result.created({})
            if request.sku == "ok"
            else Result.failure(Status.SERVICE_UNAVAILABLE)
        )

    app = SqsConsumerApp(_app(flaky))
    good = SqsMessageBuilder("orders:place").with_body({"sku": "ok"}).with_receipt_handle("r-good").build()
    bad = SqsMessageBuilder("orders:place").with_body({"sku": "bad"}).with_receipt_handle("r-bad").build()
    client = RecordingSqsClient(messages=[good, bad])

    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= 2  # one batch receive + one empty receive

    asyncio.run(
        run_sqs_consumer_loop(app, client, "https://sqs.example/q", should_continue=should_continue)
    )
    # at-least-once: the good message is deleted, the failed one is left for redelivery/DLQ redrive.
    assert client.deleted == ["r-good"]


def test_loop_with_delete_disabled_lets_the_caller_control_deletion() -> None:
    app = SqsConsumerApp(_app())
    message = SqsMessageBuilder("orders:place").with_body({"sku": "A"}).with_receipt_handle("r1").build()
    client = RecordingSqsClient(messages=[message])
    seen: list[Any] = []

    asyncio.run(
        run_sqs_consumer_loop(
            app,
            client,
            "https://sqs.example/q",
            delete=False,
            should_continue=lambda: bool(client.messages),
            on_result=lambda m, r: seen.append((m["ReceiptHandle"], r)),
        )
    )
    assert [r for _, r in seen][0].is_successful
    assert client.deleted == []  # delete=False: the caller owns deletion, the loop never does it
