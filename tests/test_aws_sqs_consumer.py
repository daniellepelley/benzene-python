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
import logging
from dataclasses import dataclass
from typing import Any

import benzene.aws
import benzene.aws.sqs_consumer as sqs_consumer_module
import pytest
from benzene.aws import (
    SqsConsumerApp,
    decode_sqs_message,
    run_consumer_loop,
    run_sqs_consumer_loop,
)
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
    good = (
        SqsMessageBuilder("orders:place")
        .with_body({"sku": "ok"})
        .with_receipt_handle("r-good")
        .build()
    )
    bad = (
        SqsMessageBuilder("orders:place")
        .with_body({"sku": "bad"})
        .with_receipt_handle("r-bad")
        .build()
    )
    client = RecordingSqsClient(messages=[good, bad])

    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= 2  # one batch receive + one empty receive

    asyncio.run(
        run_consumer_loop(app, client, "https://sqs.example/q", should_continue=should_continue)
    )
    # at-least-once: the good message is deleted, the failed one is left for redelivery/DLQ redrive.
    assert client.deleted == ["r-good"]


def test_loop_with_delete_disabled_lets_the_caller_control_deletion() -> None:
    app = SqsConsumerApp(_app())
    message = (
        SqsMessageBuilder("orders:place").with_body({"sku": "A"}).with_receipt_handle("r1").build()
    )
    client = RecordingSqsClient(messages=[message])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
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


def test_loop_runs_the_blocking_boto3_calls_via_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # The receive/delete boto3 calls must run via asyncio.to_thread (off the event loop) so a ~20s
    # long poll can't starve a coroutine sharing the loop, e.g. uvicorn in the k8s_orders example.
    routed: list[str] = []
    real_to_thread = asyncio.to_thread

    async def spy(func: Any, *args: Any, **kwargs: Any) -> Any:
        routed.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(sqs_consumer_module.asyncio, "to_thread", spy)

    app = SqsConsumerApp(_app())
    message = (
        SqsMessageBuilder("orders:place").with_body({"sku": "A"}).with_receipt_handle("r1").build()
    )
    client = RecordingSqsClient(messages=[message])

    asyncio.run(
        run_consumer_loop(
            app, client, "https://sqs.example/q", should_continue=lambda: bool(client.messages)
        )
    )

    # Both blocking SDK calls were dispatched off the event loop, in order (receive then delete).
    assert routed == ["receive_message", "delete_message"]
    assert client.deleted == ["r1"]


# --- naming parity + failure logging (D8/D9) ------------------------------------------------------


def test_the_loop_is_named_for_parity_with_the_other_consumers_and_keeps_its_old_alias() -> None:
    # kafka/rabbitmq both expose ``run_consumer_loop``; namespacing (``benzene.aws.``) disambiguates.
    assert run_sqs_consumer_loop is run_consumer_loop  # the old name still works, deprecated
    assert {"run_consumer_loop", "run_sqs_consumer_loop"} <= set(benzene.aws.__all__)


def test_a_failed_message_is_logged_for_the_operator(caplog: pytest.LogCaptureFixture) -> None:
    # Without ``on_result`` wired, a poison message would otherwise loop invisibly.
    async def refuse(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE)

    app = SqsConsumerApp(_app(refuse))
    message = (
        SqsMessageBuilder("orders:place").with_body({"sku": "A"}).with_receipt_handle("r1").build()
    )
    client = RecordingSqsClient(messages=[message])

    with caplog.at_level(logging.WARNING, logger="benzene.aws.sqs_consumer"):
        asyncio.run(
            run_consumer_loop(
                app, client, "https://sqs.example/q", should_continue=lambda: bool(client.messages)
            )
        )

    assert [r.name for r in caplog.records] == ["benzene.aws.sqs_consumer"]
    logged = caplog.records[0].getMessage()
    assert "orders:place" in logged
    assert "service-unavailable" in logged
    assert client.deleted == []  # still left on the queue for redelivery
