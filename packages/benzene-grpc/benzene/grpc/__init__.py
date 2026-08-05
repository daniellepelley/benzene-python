"""``benzene.grpc`` — the gRPC edge of the Benzene wire contract.

Ships the Benzene ↔ gRPC **status mapping** (wire-contracts.md §4.2): :func:`to_grpc` /
:func:`from_grpc` and the ``benzene-status`` trailer rule that preserves the exact status across the
codes that collapse to one gRPC code. This is the foundation of a gRPC transport binding; the
server/client transport over ``grpcio`` builds on top and is the next step.

    pip install benzene-grpc

Depends on ``benzene-core``. Contributes the ``benzene.grpc`` subpackage to the shared ``benzene``
namespace. Mirrors .NET's ``Benzene.Grpc``.
"""

from __future__ import annotations

from .status import BENZENE_STATUS_TRAILER, from_grpc, to_grpc

try:  # the server/client transport needs grpcio (the [transport] extra); the mapping never does.
    from .client import GrpcMessageSender
    from .server import BenzeneGrpcHandler, add_benzene_handler, method_for, topic_for
except ImportError:  # pragma: no cover - exercised only without grpcio installed

    def _needs_grpc(*_args, **_kwargs):
        raise ImportError(
            "The Benzene gRPC transport requires grpcio — install it with "
            "'pip install benzene-grpc[transport]'."
        )

    GrpcMessageSender = _needs_grpc  # type: ignore[assignment]
    BenzeneGrpcHandler = _needs_grpc  # type: ignore[assignment]
    add_benzene_handler = _needs_grpc  # type: ignore[assignment]
    method_for = _needs_grpc  # type: ignore[assignment]
    topic_for = _needs_grpc  # type: ignore[assignment]

__all__ = [
    "BENZENE_STATUS_TRAILER",
    "BenzeneGrpcHandler",
    "GrpcMessageSender",
    "add_benzene_handler",
    "from_grpc",
    "method_for",
    "to_grpc",
    "topic_for",
]
