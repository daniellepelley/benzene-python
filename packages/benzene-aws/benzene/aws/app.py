"""AWS Lambda host (transport-bindings §1 — one host, inner bindings selected by event shape).

``AwsLambdaApp`` hosts the same Benzene handlers behind several Lambda event sources:

- **API Gateway** (HTTP-like): topic from the route (via the ``benzene.http`` binding), response
  mapped to an API Gateway proxy response.
- **SQS**: a batch of records; **one pipeline invocation and one scope per record**; failures are
  reported via the SQS partial-batch-response (``batchItemFailures``) so only the failed records are
  redelivered.
- **SNS**: a batch of records; one invocation per record; a failure raises so Lambda retries.

Topic for the messaging transports comes from the ``topic`` message attribute. The free function
:func:`to_lambda_handler` produces the ``handler(event, context)`` callable Lambda invokes.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from benzene.core import BenzeneMessageApplication, MessageHandlingError, Registry
from benzene.http import BenzeneHttpApp, HttpRouter, StandardPaths
from benzene.results import is_successful

from .events import (
    api_gateway_request,
    event_source,
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
    ) -> None:
        if application is None:
            if registry is None:
                registry = Registry.from_definitions(http_router) if http_router else Registry()
            application = BenzeneMessageApplication(registry)
        self._application = application
        self._http_app = (
            BenzeneHttpApp(http_router, application=application, standard_paths=standard_paths)
            if http_router
            else None
        )

    def handle(self, event: dict[str, Any], context: Any = None) -> dict[str, Any] | None:
        source = event_source(event)
        if source == "apigateway":
            return self._handle_api_gateway(event)
        if source == "sqs":
            return self._handle_sqs(event)
        if source == "sns":
            self._handle_sns(event)  # SNS is fire-and-forget: no response envelope to return
            return None
        raise ValueError("Unrecognised Lambda event: not API Gateway, SQS, or SNS")

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
                response = await self._application.handle(envelope)
                if not is_successful(response["statusCode"]):
                    raise MessageHandlingError(
                        envelope["topic"], response["statusCode"], response["body"]
                    )

        asyncio.run(run())


def to_lambda_handler(app: AwsLambdaApp) -> Callable[..., Any]:
    """Return the ``handler(event, context)`` callable AWS Lambda invokes."""

    def handler(event: dict[str, Any], context: Any = None) -> Any:
        return app.handle(event, context)

    return handler
