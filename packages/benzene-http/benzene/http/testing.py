"""In-memory test host for the inbound HTTP binding (mirrors the cloud ``*.testing`` modules).

Drive a :class:`~benzene.http.BenzeneHttpApp` exactly as a real HTTP request would — a method, a
path, JSON body, headers — but **in memory, no socket**: :meth:`HttpTestHost.send_http` runs the
real ASGI application's :meth:`~benzene.http.BenzeneHttpApp.handle` coroutine and hands back the
mapped :class:`~benzene.http.HttpResponse`, so an example's tests dogfood the actual binding with the
same one-specialization-step setup as every cloud host
(``create_test_host(StartUp).with_services(...).build_http()``)::

    host = create_test_host(OrdersStartUp).with_services(overrides).build_http()
    response = host.send_http("POST", "/orders", body={"sku": "A", "quantity": 2})
    assert response.status_code == 201        # the HTTP status, mapped from the Benzene status
    assert fake.last_topic == "orders:created"

This is the transport-neutral HTTP front door an adopter writes app tests against; it matches the
AWS/GCP/Azure test hosts' ``send_http`` shape cell-for-cell so a service can be re-hosted without
rewriting its tests.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from benzene.core import encode_body

from .app import BenzeneHttpApp, HttpResponse

if TYPE_CHECKING:
    from benzene.core import Scope


#: What a sync ``send_*`` says when it is called from inside a running event loop.
_ASYNC_TEST_HINT = (
    "send_http() is synchronous — call it from a plain 'def' test. From an async test, drive the "
    "app directly: `await host._app.handle(...)`."
)


def _body(value: Any) -> str:
    """Serialize a request body the way a real HTTP peer would (the wire policy, camelCase).

    A string passes through as the caller's exact bytes; everything else goes through
    :func:`benzene.core.encode_body`, so a dataclass field ``order_id`` arrives as ``orderId`` —
    exactly what a .NET/Go/TS caller sends. Encoding with ``asdict`` instead would let a raw-dict
    handler pass in memory and break against a real peer.
    """
    if isinstance(value, str):
        return value
    return encode_body(value)


class HttpTestHost:
    """Wraps a :class:`BenzeneHttpApp` for in-memory tests of the HTTP ingress."""

    #: The resolved root scope, set by ``create_test_host(...).build_http()`` for assertions.
    scope: Scope | None = None

    def __init__(self, app: BenzeneHttpApp) -> None:
        self._app = app

    def send_http(
        self,
        method: str,
        path: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Send one HTTP request through the real ASGI app; returns the mapped :class:`HttpResponse`.

        Synchronous — call it from a plain ``def`` test. From an ``async`` test, await
        ``host._app.handle(...)`` instead (there is no event loop to nest).
        """
        request = self._app.handle(
            method=method,
            path=path,
            query_string=urlencode(query) if query else "",
            headers=dict(headers or {}),
            body=_body(body) if body is not None else "",
        )
        try:
            return asyncio.run(request)
        except RuntimeError as ex:
            if "cannot be called from a running event loop" not in str(ex):
                raise
            request.close()  # the coroutine never started; don't leave it un-awaited
            raise RuntimeError(_ASYNC_TEST_HINT) from ex
