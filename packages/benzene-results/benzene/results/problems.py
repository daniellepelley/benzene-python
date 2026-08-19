"""The problem-type registry (wire-contracts.md section 3.1).

One row per **failure** status in the section 3 vocabulary, so this introduces no second taxonomy:
the registry is keyed by the status strings :mod:`benzene.results.status` already defines. Success
statuses have no row, because problem documents (section 1.3) exist only on failure.

Every URI lives under ``https://benzene.app/problems/``. These are **opaque identifiers, not live
pages** — a reader compares them by string equality and never dereferences one. The titles are fixed
per type and are deliberately never asserted by the conformance fixtures: wording is free, identity
is not.

This module is pure data plus two lookups and depends on nothing outside this package, so a
transport binding, a client, or a test can ask for a status's problem identity without pulling in
``benzene.core``.
"""

from __future__ import annotations

from .status import Status

_PROBLEM_BASE = "https://benzene.app/problems/"

# status -> (type slug, title, HTTP status). The HTTP column is carried here so the one table
# matches the spec's one table; only an HTTP binding uses it (section 1.3: `status` is omitted
# entirely where no HTTP response exists).
_REGISTRY: dict[str, tuple[str, str, int]] = {
    Status.BAD_REQUEST: ("bad-request", "Bad request", 400),
    Status.UNAUTHORIZED: ("unauthorized", "Unauthorized", 401),
    Status.FORBIDDEN: ("forbidden", "Forbidden", 403),
    Status.NOT_FOUND: ("not-found", "Not found", 404),
    Status.CONFLICT: ("conflict", "Conflict", 409),
    Status.VALIDATION_ERROR: ("validation-error", "Validation failed", 422),
    Status.TOO_MANY_REQUESTS: ("too-many-requests", "Too many requests", 429),
    Status.UNEXPECTED_ERROR: ("unexpected-error", "Unexpected error", 500),
    Status.NOT_IMPLEMENTED: ("not-implemented", "Not implemented", 501),
    Status.SERVICE_UNAVAILABLE: ("service-unavailable", "Service unavailable", 503),
    Status.TIMEOUT: ("timeout", "Timeout", 504),
}


def problem_type(status: str) -> str | None:
    """The registry ``type`` URI for ``status``, or ``None`` for an application-defined status.

    ``None`` is the correct answer for a status this registry does not know: section 3.1 says an
    application-defined failure carries its own URI or omits the member, and the framework has no
    business inventing one under the ``benzene.app`` namespace on the application's behalf.
    """
    row = _REGISTRY.get(status)
    return _PROBLEM_BASE + row[0] if row is not None else None


def problem_title(status: str) -> str | None:
    """The registry ``title`` for ``status``, or ``None`` for an application-defined status."""
    row = _REGISTRY.get(status)
    return row[1] if row is not None else None


def problem_http_status(status: str) -> int:
    """The HTTP code for ``status`` (section 4.1). Unknown/application-defined falls to 500."""
    row = _REGISTRY.get(status)
    return row[2] if row is not None else 500
