"""The inbound HTTP binding — an ASGI application (transport-bindings.md sections 1 and 2, HTTP).

``BenzeneHttpApp`` satisfies the binding contract for HTTP:

- **Topic** — resolved from route/method conventions via an :class:`HttpRouter`.
- **Headers** — HTTP headers flow in and out, both directions.
- **Status** — the handler's Benzene status maps to an HTTP code via wire-contracts §4.1
  (:func:`to_http`). A status mapping to a bodiless code (``updated``/``deleted`` → 204) is sent
  without content, as RFC 9110 requires — so a payload on those Results does not cross an HTTP hop.
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
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl

from benzene.core import (
    VERSION_HEADER,
    AppDefinition,
    BenzeneMessageApplication,
    Container,
    MiddlewarePipeline,
    Registry,
    application_from,
    error_payload,
    resolve_version,
)
from benzene.results import Result, Status

from .routing import HttpRouter
from .standard import StandardPaths
from .status import to_http

#: HTTP codes that must be sent without content (RFC 9110 sections 15.3.5 and 15.4.5).
_BODILESS_CODES = frozenset({204, 304})


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
            standard = await self._handle_standard(method, path, headers or {}, body)
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
        return HttpResponse(
            status_code=to_http(response["statusCode"]),
            headers=dict(response["headers"]),
            body=response["body"],
        )

    async def _handle_standard(
        self, method: str, path: str, headers: dict[str, str], body: str
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

        # R5 — /benzene/spec: the derived spec document (topics + payload schemas from the registry).
        if std.spec is not None and verb == "GET" and path == std.spec_path:
            spec = std.resolved_spec()
            if spec is not None:
                return HttpResponse(
                    200, {"content-type": "application/json"}, json.dumps(spec.to_payload())
                )

        return None

    def _error(self, status: str, detail: str) -> HttpResponse:
        payload = error_payload(Result.failure(status, detail))
        return HttpResponse(
            status_code=to_http(status),
            headers={"content-type": "application/json"},
            body=json.dumps(payload),
        )

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI entry point: an ``http`` request, or the ``lifespan`` startup/shutdown protocol."""
        if scope.get("type") == "lifespan":
            await _serve_lifespan(receive, send)
            return
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
        # RFC 9110: a 204/304 response carries no content. Benzene's ``updated``/``deleted`` statuses
        # map to 204 and may still carry a payload (other transports deliver it), so the body is
        # dropped here — h11/uvicorn abort the whole response if one is written.
        bodiless = response.status_code in _BODILESS_CODES
        await send(
            {
                "type": "http.response.body",
                "body": b"" if bodiless else response.body.encode("utf-8"),
            }
        )


async def _serve_lifespan(receive: Any, send: Any) -> None:
    """Answer the ASGI lifespan protocol so ``uvicorn --lifespan on`` starts (and stops) cleanly.

    The app holds no startup/shutdown state of its own — the container and its scopes are built by
    the composition root — so startup and shutdown are simply acknowledged.
    """
    while True:
        message = await receive()
        kind = message.get("type")
        if kind == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif kind == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


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
