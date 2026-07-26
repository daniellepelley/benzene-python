"""The inbound HTTP transport binding (ASGI).

An idiomatic Python implementation of the HTTP binding from the Benzene specification
(transport-bindings.md §2, HTTP): topic resolved from route/method conventions, HTTP headers both
directions, status via wire-contracts §4.1, one scope per request.

    from benzene import message, Result
    from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

    @http_endpoint("GET", "/greet/{name}")
    @message("say:hello")
    async def hello(request: dict) -> Result:
        return Result.ok({"greeting": f"Hello {request['name']}"})

    app = BenzeneHttpApp(HttpRouter().add(hello))  # a standard ASGI app
"""

from __future__ import annotations

from .app import BenzeneHttpApp, HttpResponse
from .routing import HttpEndpoint, HttpRouter, http_endpoint, routes_of

__all__ = [
    "BenzeneHttpApp",
    "HttpEndpoint",
    "HttpResponse",
    "HttpRouter",
    "http_endpoint",
    "routes_of",
]
