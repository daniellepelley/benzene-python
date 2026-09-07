"""Structured errors over gRPC — the ``grpc-status-details-bin`` trailer (wire-contracts.md §4.2).

There is no JSON problem document over gRPC: a non-OK call carries no body, so everything §1.3 puts
in one — the status, the detail, and the **structured errors** — has to travel in gRPC's own error
model instead. The ``benzene-status`` trailer already carries ``benzeneStatus``; this module is the
other half, mapping a result's ``errors`` onto a ``google.rpc.BadRequest`` (one ``FieldViolation``
per error) packed into a ``google.rpc.Status`` on the ``grpc-status-details-bin`` trailer.

Without it a ``field``/``code`` that a validator knew — and that survives an HTTP hop intact — is
flattened to one prose string by the gRPC hop, which is exactly the loss the structured errors were
added to prevent.

The mapping is pinned across the ports, so all four agree on the wire:

* ``BenzeneError.message`` → ``FieldViolation.description``
* ``BenzeneError.field``   → ``FieldViolation.field`` (left **unset**, not empty, when absent)
* ``BenzeneError.code``    → nowhere. The spec does not say where it goes, and a port inventing a
  home for it is a divergence dressed up as a feature. (``FieldViolation`` does have a ``reason``
  member that reads like the natural candidate — deciding that is a spec change, not a port's call.)

The ``google.rpc`` messages come from ``googleapis-common-protos`` (pulled in by ``grpcio-status``),
which is part of the ``[transport]`` extra rather than a hard dependency. Everything here degrades to
"attach nothing / read nothing" without it, so a peer that sends no details — or a deployment that
installed ``grpcio`` alone — behaves exactly as it did before this trailer existed.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from benzene.results import BenzeneError

#: The gRPC-defined binary trailer carrying a serialized ``google.rpc.Status``. The ``-bin`` suffix
#: is what marks a metadata value as bytes; the key is fixed by gRPC (``grpc_status`` exposes the
#: same string as ``GRPC_DETAILS_METADATA_KEY``) and is not ours to name.
GRPC_DETAILS_TRAILER = "grpc-status-details-bin"


def _load_protos() -> Any:
    """The ``google.rpc`` message modules, or ``None`` when they are not installed.

    A function rather than a bare ``try:`` at module scope so the optional import needs no
    ``# type: ignore`` gymnastics to rebind the names to ``None``: the one return type is the one
    thing every caller checks.
    """
    try:
        from google.protobuf import any_pb2
        from google.protobuf.message import DecodeError
        from google.rpc import error_details_pb2, status_pb2
    except ImportError:  # pragma: no cover - exercised only without googleapis-common-protos
        return None

    class _Protos:
        any = any_pb2
        decode_error = DecodeError
        errors = error_details_pb2
        status = status_pb2

    return _Protos


_PROTOS = _load_protos()

#: Whether structured details can be written and read at all (the protos are installed).
DETAILS_SUPPORTED = _PROTOS is not None


def details_trailer(
    code: int, message: str, errors: Sequence[BenzeneError]
) -> tuple[str, bytes] | None:
    """The ``grpc-status-details-bin`` trailer entry for a non-OK outcome, or ``None`` if unavailable.

    ``code`` is the **numeric** gRPC status code and ``message`` the same detail string the call
    sets, because a reader (``grpc_status.rpc_status.from_call`` among them) rejects a
    ``google.rpc.Status`` whose code or message disagrees with the call's own.

    A ``BadRequest`` is packed in whenever the result carries errors — not only for
    ``validation-error``. §4.2's sentence is unconditional, and a ``not-found`` or ``conflict``
    naming the field it was about has exactly the same information to convey.
    """
    if _PROTOS is None:
        return None

    rich_status = _PROTOS.status.Status(code=code, message=message)
    if errors:
        bad_request = _PROTOS.errors.BadRequest()
        for error in errors:
            violation = _PROTOS.errors.BadRequest.FieldViolation(description=error.message)
            # Set only when the error is actually scoped to a field: proto3 has no "absent" for a
            # string, so an empty one would read as "the field named ''" to the far side.
            if error.field:
                violation.field = error.field
            bad_request.field_violations.append(violation)
        packed = _PROTOS.any.Any()
        packed.Pack(bad_request)
        rich_status.details.append(packed)

    return (GRPC_DETAILS_TRAILER, rich_status.SerializeToString())


def errors_from_trailers(trailing_metadata: Any) -> tuple[BenzeneError, ...]:
    """The structured errors a peer put on ``grpc-status-details-bin``, or ``()`` when there are none.

    ``()`` is the answer for every "no details" case — no trailer, no protos installed, a trailer
    that doesn't parse, a ``google.rpc.Status`` carrying no ``BadRequest`` — so the caller has one
    condition to branch on and keeps its pre-existing message-only behaviour untouched for a peer
    that sends nothing.
    """
    if _PROTOS is None:
        return ()

    for key, value in trailing_metadata or ():
        if key != GRPC_DETAILS_TRAILER or not isinstance(value, bytes):
            continue
        try:
            rich_status = _PROTOS.status.Status.FromString(value)
        except _PROTOS.decode_error:
            # A malformed trailer is the peer's defect; the caller still gets its status and detail.
            return ()
        return _violations(rich_status)
    return ()


def _violations(rich_status: Any) -> tuple[BenzeneError, ...]:
    """Every ``BadRequest.FieldViolation`` in a ``google.rpc.Status``, in order, as Benzene errors."""
    errors: list[BenzeneError] = []
    for detail in rich_status.details:
        if not detail.Is(_PROTOS.errors.BadRequest.DESCRIPTOR):
            continue
        bad_request = _PROTOS.errors.BadRequest()
        detail.Unpack(bad_request)
        errors.extend(
            BenzeneError(message=violation.description, field=violation.field or None)
            for violation in bad_request.field_violations
        )
    return tuple(errors)
