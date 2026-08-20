"""The AWS Lambda host's entry point and event-source dispatch edges.

`to_lambda_handler(app)` is the `handler(event, context)` callable Lambda invokes; `handle` routes by
event shape. These cover the deployment entry point and the branches the in-memory `send_*` helpers
don't reach: a message-only app answering HTTP with 501, an unrecognised event, and an SNS record
whose handler fails (which must raise so Lambda retries).
"""

from __future__ import annotations

import asyncio
import json

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import AwsLambdaApp, to_lambda_handler
from benzene.aws.testing import (
    ApiGatewayRequestBuilder,
    AwsLambdaTestHost,
    SnsEventBuilder,
    SqsEventBuilder,
)
from benzene.core import MessageHandlingError, Registry, message
from benzene.http import HttpRouter, http_endpoint
from benzene.results import Result


@http_endpoint("POST", "/echo")
@message("say:echo")
async def echo(request: dict) -> Result:
    return Result.ok({"saw": request.get("name")})


def test_to_lambda_handler_dispatches_an_api_gateway_event() -> None:
    handler = to_lambda_handler(AwsLambdaApp(http_router=HttpRouter().add(echo)))
    event = ApiGatewayRequestBuilder("POST", "/echo").with_body({"name": "lambda"}).build()
    response = handler(event)  # the callable Lambda invokes: handler(event, context)
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"saw": "lambda"}


def test_message_only_app_answers_http_with_501() -> None:
    app = AwsLambdaApp(registry=Registry().register("t", echo))  # no HTTP router
    event = ApiGatewayRequestBuilder("GET", "/echo").build()
    response = app.handle(event)
    assert response["statusCode"] == 501


def test_unrecognised_event_raises() -> None:
    app = AwsLambdaApp(http_router=HttpRouter().add(echo))
    with pytest.raises(ValueError, match="Unrecognised Lambda event"):
        app.handle({"not": "a known event shape"})


def test_sns_record_failure_raises_for_retry() -> None:
    async def reject(_request: dict) -> Result:
        return Result.not_found("gone")

    app = AwsLambdaApp(registry=Registry().register("orders:created", reject))
    event = SnsEventBuilder().with_message("orders:created", {"id": "x"}).build()
    # SNS is not partial-batch: a failing record raises so Lambda redelivers the whole event.
    with pytest.raises(MessageHandlingError):
        app.handle(event)


def test_sqs_poison_record_is_isolated_not_a_whole_batch_crash() -> None:
    seen: list[dict] = []

    async def handler(request: dict) -> Result:
        seen.append(request)
        return Result.ok()

    app = AwsLambdaApp(registry=Registry().register("greet", handler))
    event = (
        SqsEventBuilder()
        .with_message("greet", "this is not json", message_id="bad")  # a poison, non-JSON body
        .with_message("greet", {"ok": True}, message_id="good")
        .build()
    )
    # The whole batch must not crash: only the poison record is reported for redelivery, and the
    # good record is still processed (partial-batch response, not an all-or-nothing failure).
    response = app.handle(event)
    assert response["batchItemFailures"] == [{"itemIdentifier": "bad"}]
    assert seen == [{"ok": True}]


# --- the sync test host, called from an async test (D10) -------------------------------------------


def test_a_sync_send_inside_a_running_loop_teaches_how_to_drive_the_app() -> None:
    # ``send_*`` calls asyncio.run under the hood; from an async test that dies with asyncio's
    # opaque "cannot be called from a running event loop". Say what to do instead.
    host = AwsLambdaTestHost(AwsLambdaApp(http_router=HttpRouter().add(echo)))

    async def an_async_test() -> None:
        host.send_http("POST", "/echo", {"name": "x"})

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(an_async_test())
    detail = str(excinfo.value)
    assert "send_http() is synchronous" in detail
    assert "plain 'def' test" in detail
    assert "await host._app.handle(...)" in detail
