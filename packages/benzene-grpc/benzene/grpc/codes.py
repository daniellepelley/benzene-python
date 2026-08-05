"""``grpc.StatusCode`` ↔ Benzene status, bridging the name-based mapping to the ``grpcio`` enum.

:mod:`benzene.grpc.status` maps to/from gRPC status-code **names** (no ``grpcio`` dependency). The
transport binding needs the actual :class:`grpc.StatusCode` members, so this module bridges the two —
importing ``grpcio`` — and is used only by the server/client transport.
"""

from __future__ import annotations

import grpc

from benzene.results import Status

from .status import to_grpc

# gRPC status-code name (what to_grpc emits) -> the grpc.StatusCode member.
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

# grpc.StatusCode member -> Benzene status, for the client's fallback when no benzene-status trailer.
_CODE_TO_STATUS: dict[grpc.StatusCode, str] = {
    grpc.StatusCode.OK: Status.OK,
    grpc.StatusCode.INVALID_ARGUMENT: Status.BAD_REQUEST,
    grpc.StatusCode.UNAUTHENTICATED: Status.UNAUTHORIZED,
    grpc.StatusCode.PERMISSION_DENIED: Status.FORBIDDEN,
    grpc.StatusCode.NOT_FOUND: Status.NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS: Status.CONFLICT,
    grpc.StatusCode.UNIMPLEMENTED: Status.NOT_IMPLEMENTED,
    grpc.StatusCode.UNAVAILABLE: Status.SERVICE_UNAVAILABLE,
    grpc.StatusCode.CANCELLED: Status.SERVICE_UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED: Status.TOO_MANY_REQUESTS,
    grpc.StatusCode.DEADLINE_EXCEEDED: Status.TIMEOUT,
    grpc.StatusCode.DATA_LOSS: Status.UNEXPECTED_ERROR,
    grpc.StatusCode.INTERNAL: Status.UNEXPECTED_ERROR,
}


def status_to_code(status: str) -> grpc.StatusCode:
    """A Benzene status → the ``grpc.StatusCode`` a server sets (via the conformant name mapping)."""
    return _NAME_TO_CODE.get(to_grpc(status), grpc.StatusCode.INTERNAL)


def code_to_status(code: grpc.StatusCode) -> str:
    """A ``grpc.StatusCode`` → a Benzene status (client fallback when no ``benzene-status`` trailer)."""
    return _CODE_TO_STATUS.get(code, Status.UNEXPECTED_ERROR)
