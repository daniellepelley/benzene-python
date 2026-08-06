"""Native-event builder + test host for the gRPC binding (mirrors the cloud ``*.testing`` modules).

Drive a Benzene gRPC service exactly as a real unary call would — method = topic, metadata = headers,
a bytes body — but **in memory, no socket**: :meth:`GrpcTestHost.send_grpc` feeds the real
:class:`~benzene.grpc.server.BenzeneGrpcHandler` through a fake ``grpc.ServicerContext``, so an
example's tests dogfood the actual binding with the same one-specialization-step setup as every cloud
host (``create_test_host(StartUp).with_services(...).build_grpc()``).

    host = create_test_host(OrdersStartUp).with_services(overrides).build_grpc()
    reply = host.send_grpc("orders:place", body={"sku": "A", "quantity": 2})
    assert reply.status == "created"          # the benzene-status trailer, verbatim vocabulary
    assert fake.last_topic == "orders:created"

The real-channel round trip (the ``GrpcMessageSender`` client over a live socket) is a *transport*
concern, covered in ``tests/test_grpc_transport.py``; this harness exercises the ingress → handler →
egress path an adopter writes app tests against, matching the cloud examples cell-for-cell.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from benzene.core import encode_body

from .server import BenzeneGrpcHandler, method_for
from .status import BENZENE_STATUS_TRAILER

if TYPE_CHECKING:
    from benzene.core import Scope


@dataclass
class GrpcResponse:
    """The outcome of a unary call, as the transport surfaces it.

    ``status`` is the raw Benzene status from the ``benzene-status`` trailer (the full vocabulary, not
    collapsed to a gRPC code); ``payload`` is the parsed response body. ``code`` / ``details`` are the
    ``grpc.StatusCode`` and detail string the server set on a failure (``None`` on success), so a test
    can assert the wire-code mapping without a socket.
    """

    status: str
    payload: Any
    code: Any = None
    details: str | None = None


class _CaptureContext:
    """A minimal fake of ``grpc.ServicerContext`` — the only gRPC-specific piece of the harness.

    Answers ``invocation_metadata()`` with the request headers and captures whatever the handler sets
    (``set_code`` / ``set_details`` / ``set_trailing_metadata``), exactly the surface
    :meth:`BenzeneGrpcHandler._invoke` touches.
    """

    def __init__(self, metadata: tuple[tuple[str, str], ...]) -> None:
        self._metadata = metadata
        self.code: Any = None
        self.details: str | None = None
        self.trailing: tuple[tuple[str, str], ...] = ()

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return self._metadata

    def set_code(self, code: Any) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, trailing: Any) -> None:
        self.trailing = tuple(trailing)


class GrpcTestHost:
    """Wraps a :class:`BenzeneGrpcHandler` for in-memory tests of the gRPC ingress."""

    #: The resolved root scope, set by ``create_test_host(...).build_grpc()`` for assertions.
    scope: Scope | None = None

    def __init__(self, handler: BenzeneGrpcHandler) -> None:
        self._handler = handler

    def send_grpc(
        self, topic: str, body: Any = None, headers: dict[str, str] | None = None
    ) -> GrpcResponse:
        """Invoke ``topic`` as a unary call — ``headers`` become request metadata — with no socket."""
        request = encode_body({} if body is None else body).encode("utf-8")
        metadata = tuple((str(k), str(v)) for k, v in (headers or {}).items())
        context = _CaptureContext(metadata)
        # Drive the real handler exactly as gRPC would: resolve the method for the topic, then call it.
        method = self._handler.service(SimpleNamespace(method=method_for(topic)))
        out = method.unary_unary(request, context)
        status = next(
            (value for key, value in context.trailing if key == BENZENE_STATUS_TRAILER), "ok"
        )
        return GrpcResponse(status, _parse(out.decode("utf-8")), context.code, context.details)


def _parse(body: str) -> Any:
    if not body:
        return None
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return body
