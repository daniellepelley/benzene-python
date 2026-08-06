"""The AWS Lambda host's entry point and event-source dispatch edges.

`to_lambda_handler(app)` is the `handler(event, context)` callable Lambda invokes; `handle` routes by
event shape. These cover the deployment entry point and the branches the in-memory `send_*` helpers
don't reach: a message-only app answering HTTP with 501, an unrecognised event, and an SNS record
whose handler fails (which must raise so Lambda retries).
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("benzene.aws")

from benzene.aws import AwsLambdaApp, to_lambda_handler
from benzene.aws.testing import ApiGatewayRequestBuilder, SnsEventBuilder
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
