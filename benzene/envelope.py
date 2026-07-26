"""The BenzeneMessage envelope entry point (wire-contracts.md section 1).

``BenzeneMessageApplication`` is the transport-neutral entry point: it decodes a request envelope
``{topic, headers, body}``, runs the pipeline (with the message router last), and encodes a
response envelope ``{statusCode, headers, body}``. ``body`` is always a pre-serialized JSON string.
"""

from __future__ import annotations

import json
from typing import Any

from ._mapping import to_jsonable
from .container import Container
from .context import Context
from .pipeline import MiddlewarePipeline
from .registry import Registry
from .result import Result
from .router import message_router
from .status import Status

#: The header carrying the payload/handler version (draft in the spec; read with an empty default).
VERSION_HEADER = "benzene-version"


class BenzeneMessageApplication:
    def __init__(
        self,
        registry: Registry,
        pipeline: MiddlewarePipeline | None = None,
        container: Container | None = None,
    ) -> None:
        self._registry = registry
        self._container = container or Container()
        self._pipeline = pipeline or MiddlewarePipeline()
        # The router is the terminal middleware, registered last.
        self._pipeline.use(message_router(registry))

    async def handle_async(self, request_envelope: dict[str, Any]) -> dict[str, Any]:
        topic = request_envelope.get("topic") or ""
        headers = {
            k.lower(): v for k, v in (request_envelope.get("headers") or {}).items()
        }
        body = request_envelope.get("body") or ""
        version = headers.get(VERSION_HEADER, "")

        parsed = json.loads(body) if body else {}
        scope = self._container.create_scope()
        context = Context(topic, parsed, headers, scope, version)

        await self._pipeline.handle(context)
        return encode_response(context.result)


def error_payload(result: Result[Any]) -> dict[str, Any]:
    """The problem-details-shaped error body (wire-contracts.md section 1.3)."""
    return {"status": result.status, "detail": ", ".join(result.errors)}


def encode_response(result: Result[Any] | None) -> dict[str, Any]:
    """Encode a Result into a response envelope."""
    if result is None:
        result = Result.failure(Status.UNEXPECTED_ERROR, "The pipeline produced no result")

    headers = {"content-type": "application/json"}
    if result.is_successful:
        body = "" if result.payload is None else json.dumps(to_jsonable(result.payload))
    else:
        body = json.dumps(error_payload(result))

    return {"statusCode": result.status, "headers": headers, "body": body}
