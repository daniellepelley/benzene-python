"""The BenzeneMessage envelope entry point (wire-contracts.md section 1).

``BenzeneMessageApplication`` is the transport-neutral entry point: it decodes a request envelope
``{topic, headers, body}``, runs the pipeline (with the message router last), and encodes a
response envelope ``{statusCode, headers, body}``. ``body`` is always a pre-serialized JSON string.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from benzene.results import Result, Status, is_successful

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


def resolve_version(
    headers: Mapping[str, str], names: tuple[str, ...] = VERSION_HEADER_NAMES
) -> str:
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
        headers = {k.lower(): v for k, v in (request_envelope.get("headers") or {}).items()}
        body = request_envelope.get("body") or ""
        version = resolve_version(headers)

        try:
            parsed = json.loads(body) if body else {}
        except (ValueError, TypeError):
            # A malformed body is the caller's error, never a crash: the entry point must always
            # return a response envelope so a transport (e.g. SQS partial-batch, /benzene/invoke)
            # can classify it rather than throwing out of the adapter.
            return encode_response(Result.bad_request("Request body is not valid JSON"))
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


def decode_response(response: Mapping[str, Any]) -> Result[Any]:
    """The inverse of :func:`encode_response`: a response envelope ``{statusCode, headers, body}``
    back into a :class:`~benzene.results.Result`.

    For a **transport whose response Payload/body IS the Benzene response envelope verbatim** — an
    in-process dispatch, a direct AWS Lambda invoke of another Benzene function, or any bespoke
    caller that speaks the wire envelope directly rather than translating through HTTP status codes
    or gRPC codes — this is the one decode step needed, no reverse status-code table involved (unlike
    ``benzene.http``'s ``from_http`` or ``benzene.grpc``'s ``code_to_status``, whose peers speak a
    *different* status vocabulary on the wire). An empty ``body`` maps to a ``None`` payload; a failure
    body in :func:`error_payload`'s shape (``{"status", "detail"}``) has its ``detail`` split back into
    the result's ``errors`` tuple; a body that isn't valid JSON becomes ``unexpected-error`` rather than
    raising, matching this envelope's "never crash the caller" rule everywhere else.
    """
    status = response.get("statusCode") or Status.UNEXPECTED_ERROR
    body = response.get("body") or ""
    if not body:
        return Result(status, None)

    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return Result.unexpected_error(f"response body is not valid JSON: {body!r}")

    if not is_successful(status) and isinstance(parsed, dict) and "detail" in parsed:
        # error_payload() always encodes a failure as {"status": ..., "detail": ...} - surface the
        # detail as the Result's error message(s), matching what a peer decoding this envelope does.
        detail = parsed.get("detail") or ""
        errors = tuple(e for e in detail.split(", ") if e) if detail else ()
        return Result(status, None, errors)

    return Result(status, parsed)
