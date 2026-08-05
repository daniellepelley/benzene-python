"""The BenzeneMessage envelope entry point (wire-contracts.md section 1).

``BenzeneMessageApplication`` is the transport-neutral entry point: it decodes a request envelope
``{topic, headers, body}``, runs the pipeline (with the message router last), and encodes a
response envelope ``{statusCode, headers, body}``. ``body`` is always a pre-serialized JSON string.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from benzene.results import Result, Status

from .context import Context
from .dependencies import Container
from .mapping import to_jsonable
from .pipeline import MiddlewarePipeline
from .registry import Registry, VersionSelector
from .router import message_router

#: The canonical header carrying the payload/handler version (versioning.md §2). Written outbound.
VERSION_HEADER = "benzene-version"

#: The ordered fallback list read inbound — a peer (.NET/Go/TS) may send any of these; first wins.
VERSION_HEADER_NAMES: tuple[str, ...] = (VERSION_HEADER, "version", "x-version")


def resolve_version(headers: Mapping[str, str], names: tuple[str, ...] = VERSION_HEADER_NAMES) -> str:
    """Read the message version from the first present header in ``names`` (versioning.md §2).

    Absent from all of them → ``""`` (the unversioned default). Headers are matched lower-case, as
    the envelope normalises them.
    """
    for name in names:
        value = headers.get(name)
        if value:
            return value
    return ""


class BenzeneMessageApplication:
    """The transport-neutral entry point.

    Decodes a request envelope ``{topic, headers, body}``, runs the pipeline (with the message
    router registered last), and encodes a response envelope ``{statusCode, headers, body}``.
    """

    def __init__(
        self,
        registry: Registry,
        pipeline: MiddlewarePipeline | None = None,
        container: Container | None = None,
        *,
        version_selector: VersionSelector | None = None,
    ) -> None:
        self._registry = registry
        self._container = container or Container()
        self._pipeline = pipeline or MiddlewarePipeline()
        # The router is the terminal middleware, registered last.
        self._pipeline.use(message_router(registry, version_selector))

    async def handle(self, request_envelope: dict[str, Any]) -> dict[str, Any]:
        topic = request_envelope.get("topic") or ""
        headers = {
            k.lower(): v for k, v in (request_envelope.get("headers") or {}).items()
        }
        body = request_envelope.get("body") or ""
        version = resolve_version(headers)

        parsed = json.loads(body) if body else {}
        scope = self._container.create_scope()
        context = Context(topic, parsed, headers, scope, version)

        await self._pipeline.handle(context)
        response = encode_response(context.result)
        # Echo the resolved version back (wire-contracts §2.1 lists benzene-version as an outbound
        # header; versioning.md §4.2 "respond in the same version the request declared"). Only when
        # the request actually declared one, so unversioned traffic is byte-for-byte unchanged and a
        # consumer that sees the header can trust the body is that version (e.g. a downcast reply).
        if version:
            response["headers"][VERSION_HEADER] = version
        return response


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
