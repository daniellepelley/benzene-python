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

__all__ = ["BENZENE_STATUS_TRAILER", "from_grpc", "to_grpc"]
