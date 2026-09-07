"""``benzene.grpc`` — the gRPC edge of the Benzene wire contract.

Two layers, installed together and used apart:

* the **status mapping** (wire-contracts.md §4.2): :func:`to_grpc` / :func:`from_grpc` and the
  ``benzene-status`` trailer rule that preserves the exact status across the codes that collapse to
  one gRPC code. Dependency-free — it maps to and from gRPC status-code *names*, never ``grpcio``.
* the **transport binding** over ``grpcio``: :class:`~benzene.grpc.server.BenzeneGrpcHandler` /
  :func:`~benzene.grpc.server.add_benzene_handler` serve every topic as a generic unary method, and
  :class:`~benzene.grpc.client.GrpcMessageSender` calls one; a failure's structured errors
  cross as a ``google.rpc.BadRequest`` on the ``GRPC_DETAILS_TRAILER``. Needs the ``[transport]``
  extra; the names above import as stubs that raise a pointed ``ImportError`` without it.

::

    pip install benzene-grpc              # the mapping alone
    pip install "benzene-grpc[transport]" # + the server/client binding

Depends on ``benzene-core``. Contributes the ``benzene.grpc`` subpackage to the shared ``benzene``
namespace. Mirrors .NET's ``Benzene.Grpc``.
"""

from __future__ import annotations

from .details import GRPC_DETAILS_TRAILER
from .status import BENZENE_STATUS_TRAILER, from_grpc, to_grpc

# The server/client transport needs grpcio (the [transport] extra); the mapping above never does.
# Guard on grpcio specifically so a real import error *inside* the transport modules surfaces as
# itself, rather than being mistaken for a missing optional dependency.
try:
    import grpc as _grpc  # noqa: F401
except ImportError:  # pragma: no cover - exercised only without grpcio installed
    _grpc = None  # type: ignore[assignment]

if _grpc is not None:
    from .client import GrpcMessageSender
    from .server import BenzeneGrpcHandler, add_benzene_handler, method_for, topic_for
else:

    def _needs_grpc(*_args, **_kwargs):
        raise ImportError(
            "The Benzene gRPC transport requires grpcio — install it with "
            "'pip install benzene-grpc[transport]'."
        )

    GrpcMessageSender = _needs_grpc  # type: ignore[assignment,misc]  # class name reused as a stub
    BenzeneGrpcHandler = _needs_grpc  # type: ignore[assignment,misc]  # class name reused as a stub
    add_benzene_handler = _needs_grpc  # type: ignore[assignment]
    method_for = _needs_grpc  # type: ignore[assignment]
    topic_for = _needs_grpc  # type: ignore[assignment]

__all__ = [
    "BENZENE_STATUS_TRAILER",
    "GRPC_DETAILS_TRAILER",
    "BenzeneGrpcHandler",
    "GrpcMessageSender",
    "add_benzene_handler",
    "from_grpc",
    "method_for",
    "to_grpc",
    "topic_for",
]
