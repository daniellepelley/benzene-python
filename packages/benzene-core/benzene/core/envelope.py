"""The BenzeneMessage envelope entry point (wire-contracts.md section 1).

``BenzeneMessageApplication`` is the transport-neutral entry point: it decodes a request envelope
``{topic, headers, body}``, runs the pipeline (with the message router last), and encodes a
response envelope ``{statusCode, headers, body}``. ``body`` is always a pre-serialized JSON string.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from benzene.results import BenzeneError, Result, Status, is_successful
from benzene.results.problems import problem_title, problem_type

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
    """The RFC 9457 problem document written as a failed response's body (wire-contracts.md 1.3).

    A genuine problem document, not a problem-shaped dict: ``type`` is the section 3.1 registry URI
    for the status (omitted for an application-defined status, which the framework has no URI for),
    ``benzeneStatus`` is the required transport-neutral discriminator mirroring the envelope's
    ``statusCode``, ``detail`` is the error messages joined with ``", "``, and ``errors`` carries
    them individually and in order.

    ``status`` is deliberately absent. RFC 9457 defines it as the integer HTTP response code, and
    section 1.3 requires it to be omitted - not null - wherever no HTTP response exists, which is
    every transport this function serves. An HTTP binding adds it when it renders the document as
    an HTTP body (section 4.1).

    The previous shape put the Benzene *status string* in a member named ``status``, colliding with
    RFC 9457's own integer member. That collision was resolved by rename, not by dropping the RFC
    alignment: the Benzene status now travels as ``benzeneStatus``.
    """
    # An application-authored document is emitted verbatim (Result.problem). Deriving one from the
    # status instead would overwrite the application's own `type` URI with the registry URI, which
    # is the entire reason for authoring it.
    if result.problem_document is not None:
        return result.problem_document.to_payload()

    problem: dict[str, Any] = {}

    type_uri = problem_type(result.status)
    if type_uri is not None:
        problem["type"] = type_uri
        problem["title"] = problem_title(result.status)

    problem["detail"] = ", ".join(result.messages)
    problem["benzeneStatus"] = result.status

    # Authoritative and ordered when present (section 1.3): this replaces the withdrawn "recover
    # errors by splitting detail on ', '" rule, which was never safe - messages contain commas.
    # Each error is emitted whole, so a field and a code the producer knew reach the caller.
    if result.errors:
        problem["errors"] = [error.to_payload() for error in result.errors]

    return problem


def encode_response(result: Result[Any] | None) -> dict[str, Any]:
    """Encode a Result into a response envelope."""
    if result is None:
        result = Result.failure(Status.UNEXPECTED_ERROR, "The pipeline produced no result")

    headers = {"content-type": "application/json"}
    if result.is_successful:
        body = "" if result.payload is None else json.dumps(to_jsonable(result.payload))
    else:
        # The envelope is the failure signal, and the body IS the problem document (section 1.3),
        # so say so on the way out rather than leaving a reader to sniff the shape.
        headers["content-type"] = "application/problem+json"
        body = json.dumps(error_payload(result))

    # isSuccessful is REQUIRED (section 1.2) and is the authoritative signal: a receiver MUST prefer
    # it over anything it derives from statusCode text. That matters most for an application-defined
    # status, which is outside the shared vocabulary and means nothing to a receiver classifying by
    # string alone - exactly the case where omitting this member makes a success look like a failure.
    return {
        "statusCode": result.status,
        "isSuccessful": result.is_successful,
        "headers": headers,
        "body": body,
    }


def decode_response(response: Mapping[str, Any]) -> Result[Any]:
    """The inverse of :func:`encode_response`: a response envelope ``{statusCode, headers, body}``
    back into a :class:`~benzene.results.Result`.

    For a **transport whose response Payload/body IS the Benzene response envelope verbatim** — an
    in-process dispatch, a direct AWS Lambda invoke of another Benzene function, or any bespoke
    caller that speaks the wire envelope directly rather than translating through HTTP status codes
    or gRPC codes — this is the one decode step needed, no reverse status-code table involved (unlike
    ``benzene.http``'s ``from_http`` or ``benzene.grpc``'s ``code_to_status``, whose peers speak a
    *different* status vocabulary on the wire). An empty ``body`` maps to a ``None`` payload; a failure
    body in :func:`error_payload`'s shape (an RFC 9457 problem document) has its ``errors`` member read
    back into the result's ``errors`` tuple, falling back to ``detail`` as a single opaque message when
    the producer sent no ``errors``; a body that isn't valid JSON becomes ``unexpected-error`` rather
    than raising, matching this envelope's "never crash the caller" rule everywhere else.
    """
    status = response.get("statusCode") or Status.UNEXPECTED_ERROR
    body = response.get("body") or ""
    if not body:
        return Result(status, None)

    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return Result.unexpected_error(f"response body is not valid JSON: {body!r}")

    if not is_successful(status) and isinstance(parsed, dict) and _looks_like_problem(parsed):
        # errors, when present, is authoritative and ordered (section 1.3). Only when it is absent
        # does detail stand in, and then as ONE opaque message: splitting it on ", " was withdrawn
        # by the RFC 9457 revision because error messages contain commas.
        raw_errors = parsed.get("errors")
        if isinstance(raw_errors, list):
            # Each entry is decoded whole, not flattened to its message: a peer that sent a field
            # and a code went to the trouble of knowing them, and a client that re-raises or
            # re-renders the failure should still have them.
            errors = tuple(
                BenzeneError.coerce(item if isinstance(item, (dict, str)) else str(item))
                for item in raw_errors
            )
            return Result(status, None, tuple(e for e in errors if e.message))

        detail = parsed.get("detail") or ""
        return Result(status, None, (BenzeneError(detail),) if detail else ())

    return Result(status, parsed)


def _looks_like_problem(parsed: dict[str, Any]) -> bool:
    """Whether a failed response's parsed body is a problem document rather than a domain payload.

    Any of the three members this profile's documents always or usually carry is enough. Accepting
    the withdrawn ``status``-string shape too would be wrong - that member is now the integer HTTP
    code - so a legacy peer's body simply falls through and is surfaced as the payload, which is
    honest about not understanding it rather than silently mis-reading a number as a status.
    """
    return "benzeneStatus" in parsed or "errors" in parsed or "detail" in parsed
