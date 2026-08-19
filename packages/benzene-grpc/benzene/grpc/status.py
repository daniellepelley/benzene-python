"""Benzene ↔ gRPC status mapping (wire-contracts.md section 4.2).

The gRPC edge of the wire contract: a Benzene status maps to a gRPC ``StatusCode`` on the way out and
back on the way in. Several Benzene statuses collapse to one gRPC code (all success statuses → ``OK``),
so a gRPC server MUST also attach a ``benzene-status`` **trailer** carrying the raw status string
verbatim — and a client, seeing that trailer, uses it in preference to re-deriving from the code. This
module is the mapping tables plus that trailer rule; the gRPC *transport* binding (server/client over
``grpcio``) builds on top and is the next step.

gRPC status codes are represented by their canonical **names** (``"OK"``, ``"InvalidArgument"``, …) as
strings, so the mapping needs no ``grpcio`` dependency — a transport binding translates these to
``grpc.StatusCode`` members at its edge.
"""

from __future__ import annotations

from benzene.results import Status, is_successful

#: The trailer a gRPC server attaches carrying the raw Benzene status verbatim (wins on the way back).
BENZENE_STATUS_TRAILER = "benzene-status"

# Forward: Benzene status -> gRPC StatusCode name. Success statuses go through is_successful -> OK;
# an unknown/missing failure status -> Internal.
_FORWARD: dict[str, str] = {
    Status.BAD_REQUEST: "InvalidArgument",
    Status.VALIDATION_ERROR: "InvalidArgument",
    Status.UNAUTHORIZED: "Unauthenticated",
    Status.FORBIDDEN: "PermissionDenied",
    Status.NOT_FOUND: "NotFound",
    Status.CONFLICT: "AlreadyExists",
    Status.NOT_IMPLEMENTED: "Unimplemented",
    Status.SERVICE_UNAVAILABLE: "Unavailable",
    Status.TOO_MANY_REQUESTS: "ResourceExhausted",
    Status.TIMEOUT: "DeadlineExceeded",
    Status.UNEXPECTED_ERROR: "Internal",
}

# Reverse: gRPC StatusCode name -> Benzene status, when no benzene-status trailer is present.
_REVERSE: dict[str, str] = {
    "OK": Status.OK,
    "InvalidArgument": Status.BAD_REQUEST,
    "Unauthenticated": Status.UNAUTHORIZED,
    "PermissionDenied": Status.FORBIDDEN,
    "NotFound": Status.NOT_FOUND,
    "AlreadyExists": Status.CONFLICT,
    "Unimplemented": Status.NOT_IMPLEMENTED,
    "Unavailable": Status.SERVICE_UNAVAILABLE,
    "Cancelled": Status.SERVICE_UNAVAILABLE,
    "ResourceExhausted": Status.TOO_MANY_REQUESTS,
    "DeadlineExceeded": Status.TIMEOUT,
    "DataLoss": Status.UNEXPECTED_ERROR,
    "Internal": Status.UNEXPECTED_ERROR,
}


def to_grpc(status: str, is_result_successful: bool | None = None) -> str:
    """Map a Benzene status to a gRPC ``StatusCode`` name (server side), wire-contracts.md 4.2.

    A status in the section 3 vocabulary maps by its own row. ``is_result_successful`` decides only
    for an **application-defined** status, which has no row: a failed one falls to ``Internal``, and
    one carried on a result explicitly marked successful (the ``Set(status, payload, isSuccessful)``
    escape hatch) maps to ``OK`` rather than being reported as a server error. Left unset, an unknown
    status is treated as a failure - the safe default for a caller that cannot tell. The same rule
    the HTTP binding's ``to_http`` applies, kept symmetrical on purpose.
    """
    if is_successful(status):
        return "OK"
    known = _FORWARD.get(status)
    if known is not None:
        return known
    return "OK" if is_result_successful else "Internal"


def from_grpc(code: str, trailer: str | None = None) -> str:
    """Map a gRPC ``StatusCode`` name back to a Benzene status (client side).

    A ``benzene-status`` ``trailer``, when present, **wins verbatim** (that is what preserves the exact
    status across the codes that collapse to one gRPC code); otherwise the code is mapped, with an
    unrecognised code falling back to ``unexpected-error``.
    """
    if trailer:
        return trailer
    return _REVERSE.get(code, Status.UNEXPECTED_ERROR)
