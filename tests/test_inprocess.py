"""In-process transport: dispatches straight to a handler pipeline in the same runtime, no wire
hop. See ``benzene.core.inprocess`` for the port-divergence rationale (per-instance ``Registry``
means no per-target-topic workaround is needed here, unlike .NET/TypeScript).
"""

from __future__ import annotations

import asyncio

from benzene.core import (
    BenzeneMessageApplication,
    DuplicatePipelineError,
    InProcessFanOutSender,
    InProcessMessageSender,
    PipelineNotFoundError,
    Pipelines,
    Registry,
    message,
)
from benzene.results import Result, is_successful


def test_sender_dispatches_to_the_registered_pipeline_and_returns_a_typed_response() -> None:
    @message("billing:echo")
    async def echo(request: dict) -> Result:
        return Result.ok({"echo": request["message"]})

    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry().add(echo)))
    sender = InProcessMessageSender(pipelines, "billing")

    result = asyncio.run(sender.send_message("billing:echo", {"message": "hello"}))

    assert result.is_successful
    assert result.payload == {"echo": "hello"}


def test_pipelines_add_duplicate_name_raises() -> None:
    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry()))

    try:
        pipelines.add("billing", BenzeneMessageApplication(Registry()))
        raise AssertionError("expected DuplicatePipelineError")
    except DuplicatePipelineError:
        pass


def test_sender_construction_with_an_unregistered_name_raises_listing_what_is_registered() -> None:
    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry()))

    try:
        InProcessMessageSender(pipelines, "shipping")
        raise AssertionError("expected PipelineNotFoundError")
    except PipelineNotFoundError as exc:
        assert "billing" in str(exc)


def test_sender_unhandled_topic_degrades_to_not_found() -> None:
    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry()))
    sender = InProcessMessageSender(pipelines, "billing")

    result = asyncio.run(sender.send_message("no:such:topic", {}))

    assert not result.is_successful
    assert result.status == "not-found"


def test_sender_handler_failure_surfaces_status_and_detail() -> None:
    @message("billing:reject")
    async def reject(_request: dict) -> Result:
        return Result.bad_request("nope")

    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry().add(reject)))
    sender = InProcessMessageSender(pipelines, "billing")

    result = asyncio.run(sender.send_message("billing:reject", {}))

    assert result.status == "bad-request"
    assert result.messages == ("nope",)


def test_fanout_dispatches_to_every_target_concurrently() -> None:
    calls: list[str] = []

    @message("order:created")
    async def billing_handler(_request: dict) -> Result:
        calls.append("billing")
        return Result.ok()

    @message("order:created")
    async def shipping_handler(_request: dict) -> Result:
        calls.append("shipping")
        return Result.ok()

    # Both targets register a handler for the literal SAME topic - legal here because each named
    # pipeline has its own independent Registry, unlike the .NET/TypeScript ports (see the module
    # doc comment in benzene.core.inprocess).
    pipelines = (
        Pipelines()
        .add("billing", BenzeneMessageApplication(Registry().add(billing_handler)))
        .add("shipping", BenzeneMessageApplication(Registry().add(shipping_handler)))
    )
    sender = InProcessFanOutSender(pipelines, "billing", "shipping")

    result = asyncio.run(sender.send_message("order:created", {}))

    assert result.is_successful
    assert sorted(calls) == ["billing", "shipping"]


def test_fanout_one_consumer_fails_does_not_affect_the_other_or_the_callers_result() -> None:
    shipping_called = False

    @message("order:created")
    async def failing_handler(_request: dict) -> Result:
        raise RuntimeError("boom")

    async def shipping_handler(_request: dict) -> Result:
        nonlocal shipping_called
        shipping_called = True
        return Result.ok()

    pipelines = (
        Pipelines()
        .add("failing", BenzeneMessageApplication(Registry().add(failing_handler)))
        .add("shipping", BenzeneMessageApplication(Registry().register("order:created", shipping_handler)))
    )
    sender = InProcessFanOutSender(pipelines, "failing", "shipping")

    result = asyncio.run(sender.send_message("order:created", {}))

    # Unconditional success once accepted - matching what a real SNS publish returns.
    assert is_successful(result.status)
    assert shipping_called


def test_fanout_construction_requires_at_least_one_name() -> None:
    pipelines = Pipelines()

    try:
        InProcessFanOutSender(pipelines)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_fanout_construction_with_an_unregistered_name_raises() -> None:
    pipelines = Pipelines().add("billing", BenzeneMessageApplication(Registry()))

    try:
        InProcessFanOutSender(pipelines, "billing", "shipping")
        raise AssertionError("expected PipelineNotFoundError")
    except PipelineNotFoundError:
        pass
