"""``benzene.http`` — the inbound HTTP transport binding (ASGI).

The top layer of the Benzene Python port shipped so far (distribution ``benzene-http``): an
idiomatic Python implementation of the HTTP binding from the specification (transport-bindings §2,
HTTP). Topic resolved from route/method conventions, HTTP headers both directions, status via
wire-contracts §4.1, one scope per request. Depends on ``benzene-core``.

    pip install benzene-http

    from benzene.core import message
    from benzene.results import Result
    from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

    @http_endpoint("GET", "/greet/{name}")
    @message("say:hello")
    async def hello(request: dict) -> Result:
        return Result.ok({"greeting": f"Hello {request['name']}"})

    app = BenzeneHttpApp(HttpRouter().add(hello))  # a standard ASGI app

Contributes the ``benzene.http`` subpackage to the shared ``benzene`` namespace. Mirrors .NET's
``Benzene.Http``.
"""

from __future__ import annotations

from .app import BenzeneHttpApp, HttpResponse
from .client import HttpMessageSender, HttpReply, HttpTransport, stdlib_transport
from .routing import HttpEndpoint, HttpRouter, http_endpoint, routes_of
from .status import from_http, to_http

__all__ = [
    "BenzeneHttpApp",
    "HttpEndpoint",
    "HttpMessageSender",
    "HttpReply",
    "HttpResponse",
    "HttpRouter",
    "HttpTransport",
    "from_http",
    "http_endpoint",
    "routes_of",
    "stdlib_transport",
    "to_http",
]
