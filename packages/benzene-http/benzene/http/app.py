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
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl

from benzene.core import (
    VERSION_HEADER,
    BenzeneMessageApplication,
    Container,
    MiddlewarePipeline,
    Registry,
    error_payload,
)
from benzene.results import Result, Status

from .routing import HttpRouter
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
    ) -> None:
        self._router = router
        if application is None:
            registry = Registry()
            for definition in router.definitions():
                registry.add_definition(definition)
            application = BenzeneMessageApplication(registry, pipeline, container)
        self._application = application

    async def handle(
        self,
        method: str,
        path: str,
        query_string: str = "",
        headers: dict[str, str] | None = None,
        body: str = "",
    ) -> HttpResponse:
        """Handle one HTTP request end-to-end, returning the mapped :class:`HttpResponse`."""
        match = self._router.match(method, path)
        if match is None:
            return self._error(
                Status.NOT_FOUND, f"No route for {method.upper()} {path}"
            )
        endpoint, path_params = match

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
        # The route's version drives handler selection unless the caller pinned one explicitly.
        if endpoint.version and VERSION_HEADER not in request_headers:
            request_headers[VERSION_HEADER] = endpoint.version

        envelope = {
            "topic": endpoint.topic,
            "headers": request_headers,
            "body": json.dumps(request_data),
        }
        response = await self._application.handle_async(envelope)
        return HttpResponse(
            status_code=to_http(response["statusCode"]),
            headers=dict(response["headers"]),
            body=response["body"],
        )

    def _error(self, status: str, detail: str) -> HttpResponse:
        payload = error_payload(Result.failure(status, detail))
        return HttpResponse(
            status_code=to_http(status),
            headers={"content-type": "application/json"},
            body=json.dumps(payload),
        )

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        """ASGI entry point (HTTP scope only)."""
        if scope.get("type") != "http":
            raise ValueError(f"BenzeneHttpApp only handles 'http' scopes, got {scope.get('type')!r}")

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
                    [k.encode("latin-1"), v.encode("latin-1")]
                    for k, v in response.headers.items()
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
    return b"".join(chunks).decode("utf-8")
