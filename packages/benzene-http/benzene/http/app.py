"""The inbound HTTP binding — an ASGI application (transport-bindings.md sections 1 and 2, HTTP).

``BenzeneHttpApp`` satisfies the binding contract for HTTP:

- **Topic** — resolved from route/method conventions via an :class:`HttpRouter`.
- **Headers** — HTTP headers flow in and out, both directions.
- **Status** — the handler's Benzene status maps to an HTTP code via wire-contracts §4.1
  (:func:`to_http`).
- **Scope** — one DI scope and exactly one pipeline invocation per request (delegated to the
  underlying :class:`~benzene.core.BenzeneMessageApplication`).
- **Failure** — an uncaught handler error becomes a Benzene ``service-unavailable`` (mapped to
  503); a body that is not valid JSON becomes ``bad-request`` (400); an unmatched route becomes
  ``not-found`` (404). The host is never crashed by request content.

The request the handler sees is the JSON body object merged with the query string and then the
captured path parameters (path wins, then query, then body), so ``/orders/{id}`` delivers ``id``
as a request field. It is exposed both as a plain :meth:`handle` coroutine (easy to unit-test) and
as a raw ASGI ``__call__`` for hosting under uvicorn/hypercorn/etc.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl

from benzene.core import (
    VERSION_HEADER,
    AppDefinition,
    BenzeneMessageApplication,
    Container,
    HttpMapping,
    MiddlewarePipeline,
    Registry,
    application_from,
    error_payload,
    resolve_version,
)
from benzene.results import Result, Status, is_successful

from .routing import HttpRouter
from .standard import NATIVE_SPEC_TYPE, StandardPaths
from .status import to_http


@dataclass(frozen=True)
class HttpResponse:
    """A transport-neutral HTTP response (status code, headers, pre-serialized body)."""

    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


class BenzeneHttpApp:
    """A standard ASGI application hosting Benzene handlers over HTTP (transport-bindings §2).

    Call :meth:`handle` for a mapped :class:`HttpResponse` (convenient in tests), or mount the
    instance itself as an ASGI app under uvicorn/hypercorn.
    """

    def __init__(
        self,
        router: HttpRouter,
        application: BenzeneMessageApplication | None = None,
        pipeline: MiddlewarePipeline | None = None,
        container: Container | None = None,
        *,
        standard_paths: StandardPaths | None = None,
    ) -> None:
        self._router = router
        if application is None:
            application = BenzeneMessageApplication(
                Registry.from_definitions(router), pipeline, container
            )
        self._application = application
        #: The Cloud Service Profile well-known surfaces (/benzene/invoke|health|spec), if enabled.
        self._standard = standard_paths

    @classmethod
    def from_definition(cls, definition: AppDefinition) -> BenzeneHttpApp:
        """Build the ASGI host from a composition root's :class:`AppDefinition` (one-line wiring)."""
        return cls(
            definition.router,
            application=application_from(definition),
            standard_paths=definition.standard_paths,
        )

    async def handle(
        self,
        method: str,
        path: str,
        query_string: str = "",
        headers: dict[str, str] | None = None,
        body: str = "",
    ) -> HttpResponse:
        """Handle one HTTP request end-to-end, returning the mapped :class:`HttpResponse`."""
        if self._standard is not None:
            standard = await self._handle_standard(method, path, query_string, headers or {}, body)
            if standard is not None:
                return standard

        match = self._router.match(method, path)
        if match is None:
            return self._error(Status.NOT_FOUND, f"No route for {method.upper()} {path}")
        endpoint, path_params = match
        # A ``{version}`` path segment (e.g. /v{version}/orders/{id}) drives handler selection, so
        # pull it out of the captured params before they merge into the request body (versioning.md §2).
        route_version = path_params.pop("version", None)

        query_data = dict(parse_qsl(query_string or ""))

        if body:
            try:
                parsed = json.loads(body)
            except (ValueError, TypeError):
                return self._error(Status.BAD_REQUEST, "Request body is not valid JSON")
        else:
            parsed = {}

        # Merge sources into the handler's request: body < query < path (most specific wins).
        if not query_data and not path_params:
            request_data: Any = parsed
        else:
            base = parsed if isinstance(parsed, dict) else {}
            request_data = {**base, **query_data, **path_params}

        request_headers = {k.lower(): v for k, v in (headers or {}).items()}
        # Version precedence: a {version} route segment is authoritative; else the route's static
        # version fills in unless the caller pinned one via any recognised version header — checked
        # through resolve_version so a fallback header (version/x-version) counts too (versioning.md §2).
        if route_version is not None:
            request_headers[VERSION_HEADER] = route_version
        elif endpoint.version and not resolve_version(request_headers):
            request_headers[VERSION_HEADER] = endpoint.version

        envelope = {
            "topic": endpoint.topic,
            "headers": request_headers,
            "body": json.dumps(request_data),
        }
        response = await self._application.handle(envelope)
        return _to_http_response(response)

    async def _handle_standard(
        self, method: str, path: str, query_string: str, headers: dict[str, str], body: str
    ) -> HttpResponse | None:
        """Serve a well-known profile surface, or ``None`` if the request is not one (→ route it)."""
        std = self._standard
        assert std is not None
        verb = method.upper()

        # R4 — /benzene/invoke: the request body *is* a message envelope; hand it to the application
        # verbatim and return the response envelope. HTTP 200 means "processed"; the domain outcome is
        # in the envelope's statusCode (a malformed envelope is a transport-level 400).
        if std.invoke and verb == "POST" and path == std.invoke_path:
            try:
                envelope = json.loads(body) if body else {}
            except (ValueError, TypeError):
                envelope = None
            if not isinstance(envelope, dict):
                return self._error(
                    Status.BAD_REQUEST,
                    "Request body must be a JSON message envelope: {topic, headers, body}",
                )
            response = await self._application.handle(
                {
                    "topic": envelope.get("topic") or "",
                    "headers": envelope.get("headers") or {},
                    "body": envelope.get("body") or "",
                }
            )
            return HttpResponse(200, {"content-type": "application/json"}, json.dumps(response))

        # R3 — /benzene/health: the full {isHealthy, healthChecks} aggregate, 200 healthy / 503 not.
        # Run the checks directly so the aggregate survives (the envelope drops a failure payload).
        if std.health is not None and verb == "GET" and path == std.health_path:
            report = await std.health.run()
            status_code = 200 if report.is_healthy else 503
            return HttpResponse(
                status_code, {"content-type": "application/json"}, json.dumps(report.to_payload())
            )

        # R5 — /benzene/spec: the derived spec document. ?type= picks the format (the R5 spelling is
        # ?type=benzene&format=json): the Contract Document by default — the shape every language's
        # client generator parses, and what a caller asking for nothing must get — with this port's
        # native {service, topics} payload still reachable at ?type=native. `format` is accepted and
        # ignored: JSON is the only rendering this port produces.
        if (
            (std.spec is not None or std.contract is not None)
            and verb == "GET"
            and path == std.spec_path
        ):
            query = dict(parse_qsl(query_string or ""))
            if query.get("type", "").strip().lower() == NATIVE_SPEC_TYPE:
                spec = std.resolved_spec()
                document = None if spec is None else spec.to_payload()
            else:
                contract = std.resolved_contract(_http_mappings(self._router))
                document = None if contract is None else contract.to_payload()
            if document is not None:
                return HttpResponse(
                    200, {"content-type": "application/json"}, json.dumps(document)
                )

        return None

    def _error(self, status: str, detail: str) -> HttpResponse:
        return http_problem_response(Result.failure(status, detail))

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI entry point (HTTP scope only)."""
        if scope.get("type") != "http":
            raise ValueError(
                f"BenzeneHttpApp only handles 'http' scopes, got {scope.get('type')!r}"
            )

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        body = await _read_body(receive)

        response = await self.handle(
            method=scope.get("method", "GET"),
            path=scope.get("path", "/"),
            query_string=scope.get("query_string", b"").decode("latin-1"),
            headers=headers,
            body=body,
        )

        await send(
            {
                "type": "http.response.start",
                "status": response.status_code,
                "headers": [
                    [k.encode("latin-1"), v.encode("latin-1")] for k, v in response.headers.items()
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": response.body.encode("utf-8"),
            }
        )


async def _read_body(receive: Any) -> str:
    """Drain an ASGI ``http.request`` message stream into a single decoded string."""
    chunks: list[bytes] = []
    more = True
    while more:
        message = await receive()
        if message.get("type") != "http.request":
            break
        chunks.append(message.get("body", b"") or b"")
        more = message.get("more_body", False)
    raw = b"".join(chunks)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        # The host must never crash on request content: a non-UTF-8 body can't be a valid Benzene
        # payload, so hand back a string the downstream JSON guard rejects as bad-request instead.
        return raw.decode("utf-8", errors="replace")


def http_problem_response(result: Result[Any]) -> HttpResponse:
    """Render a failed :class:`~benzene.results.Result` as an HTTP problem response (§4.1).

    Two things this adds to the transport-neutral document ``error_payload`` builds, both of which
    §4.1 makes mandatory for an HTTP failure whose negotiated format is JSON:

    * the ``status`` member, which RFC 9457 defines as the integer HTTP response code and which
      MUST equal the code actually sent. It is absent from the neutral document precisely because
      most transports have no HTTP response for it to equal.
    * ``content-type: application/problem+json`` rather than ``application/json``. Clients must
      accept either, so this is safe for existing readers, but emitting it is what makes the
      response a standards-conformant problem response rather than merely a JSON body.
    """
    http_status = to_http(result.status, result.is_successful)
    payload = error_payload(result)
    payload["status"] = http_status
    return HttpResponse(
        status_code=http_status,
        headers={"content-type": "application/problem+json"},
        body=json.dumps(payload),
    )


def _http_mappings(router: HttpRouter) -> dict[tuple[str, str], list[HttpMapping]]:
    """This host's routes as a Contract Document ``httpMappings`` table, keyed by (topic, version).

    A route resolves to one handler, so the key is the registry's own ``(topic, version)`` and not
    the topic alone — a topic served at two versions on two routes must not advertise both routes on
    both entries. Registration order is preserved, which is also the order the router matches in.
    """
    mappings: dict[tuple[str, str], list[HttpMapping]] = {}
    for endpoint in router.endpoints():
        mappings.setdefault((endpoint.topic, endpoint.version), []).append(
            HttpMapping(method=endpoint.method, path=endpoint.path)
        )
    return mappings


def _to_http_response(envelope: Mapping[str, Any]) -> HttpResponse:
    """Render a Benzene response envelope as an HTTP response.

    On success this is the envelope's own body and headers with the status translated. On failure
    the body is already the transport-neutral problem document (§1.3), and §4.1 requires two HTTP
    specifics on top: the ``status`` member equal to the HTTP code actually being sent, and
    ``content-type: application/problem+json``. A failure body that is not a JSON object (a legacy
    peer, or an empty body) is passed through untouched rather than guessed at.
    """
    status = envelope.get("statusCode") or ""
    # isSuccessful is authoritative and MUST be preferred over anything derived from the status
    # text (section 1.2). It is what distinguishes a health report answered with
    # service-unavailable - a 503 for the probe, but a body that is the report and not a problem
    # document - from an ordinary failure at the same status. Falling back to the status class
    # covers a sender that predates the member.
    stated = envelope.get("isSuccessful")
    successful = is_successful(status) if stated is None else bool(stated)
    http_status = to_http(status, successful)
    headers = dict(envelope.get("headers") or {})
    body = envelope.get("body") or ""

    if successful or not body:
        return HttpResponse(status_code=http_status, headers=headers, body=body)

    try:
        problem = json.loads(body)
    except (ValueError, TypeError):
        return HttpResponse(status_code=http_status, headers=headers, body=body)

    if not isinstance(problem, dict):
        return HttpResponse(status_code=http_status, headers=headers, body=body)

    problem["status"] = http_status
    headers["content-type"] = "application/problem+json"
    return HttpResponse(status_code=http_status, headers=headers, body=json.dumps(problem))
