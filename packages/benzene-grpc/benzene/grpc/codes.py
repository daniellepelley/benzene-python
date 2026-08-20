"""``grpc.StatusCode`` ↔ Benzene status, bridging the name-based mapping to the ``grpcio`` enum.

:mod:`benzene.grpc.status` maps to/from gRPC status-code **names** (no ``grpcio`` dependency). The
transport binding needs the actual :class:`grpc.StatusCode` members, so this module bridges the two —
importing ``grpcio`` — and is used only by the server/client transport.
"""

from __future__ import annotations

import grpc

from .status import from_grpc, to_grpc

# gRPC status-code name (what to_grpc emits / from_grpc reads) <-> the grpc.StatusCode member.
_NAME_TO_CODE: dict[str, grpc.StatusCode] = {
    "OK": grpc.StatusCode.OK,
    "InvalidArgument": grpc.StatusCode.INVALID_ARGUMENT,
    "Unauthenticated": grpc.StatusCode.UNAUTHENTICATED,
    "PermissionDenied": grpc.StatusCode.PERMISSION_DENIED,
    "NotFound": grpc.StatusCode.NOT_FOUND,
    "AlreadyExists": grpc.StatusCode.ALREADY_EXISTS,
    "Unimplemented": grpc.StatusCode.UNIMPLEMENTED,
    "Unavailable": grpc.StatusCode.UNAVAILABLE,
    "ResourceExhausted": grpc.StatusCode.RESOURCE_EXHAUSTED,
    "DeadlineExceeded": grpc.StatusCode.DEADLINE_EXCEEDED,
    "Internal": grpc.StatusCode.INTERNAL,
    "Cancelled": grpc.StatusCode.CANCELLED,
    "DataLoss": grpc.StatusCode.DATA_LOSS,
}

_CODE_TO_NAME: dict[grpc.StatusCode, str] = {code: name for name, code in _NAME_TO_CODE.items()}


def status_to_code(status: str, is_result_successful: bool | None = None) -> grpc.StatusCode:
    """A Benzene status → the ``grpc.StatusCode`` a server sets (via the conformant name mapping).

    ``is_result_successful`` carries the result's own success classification through to
    :func:`~benzene.grpc.status.to_grpc`, which needs it to decide an **application-defined**
    status: one marked successful is ``OK``, not ``Internal``. Dropping it here would leave that
    section 4.2 rule implemented but unreachable from the transport.
    """
    return _NAME_TO_CODE.get(to_grpc(status, is_result_successful), grpc.StatusCode.INTERNAL)


def code_to_status(code: grpc.StatusCode) -> str:
    """A ``grpc.StatusCode`` → a Benzene status (client fallback when no ``benzene-status`` trailer).

    Delegates to the single, name-based reverse table in :mod:`benzene.grpc.status`, so the two
    directions of the wire mapping have one source of truth.
    """
    return from_grpc(_CODE_TO_NAME.get(code, ""))
