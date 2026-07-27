"""The BenzeneMessage envelope entry point (wire-contracts.md section 1).

``BenzeneMessageApplication`` is the transport-neutral entry point: it decodes a request envelope
``{topic, headers, body}``, runs the pipeline (with the message router last), and encodes a
response envelope ``{statusCode, headers, body}``. ``body`` is always a pre-serialized JSON string.
"""

from __future__ import annotations

import json
from typing import Any

from benzene.results import Result, Status

from .context import Context
from .dependencies import Container
from .mapping import to_jsonable
from .pipeline import MiddlewarePipeline
from .registry import Registry
from .metadata import WireNames
from .metadata import wire_names as resolve_wire_names
from .router import message_router

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
        self._pipeline.use(message_router(registry, self.wire_names))

    @property
    def wire_names(self) -> WireNames:
        """The reserved wire names this service uses — its own registration, or the defaults.

        Resolved per read rather than cached at construction: a host may build the application
        before the startup finished registering, and the container is the single source of truth.
        """
        return resolve_wire_names(self._container.create_scope())

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
