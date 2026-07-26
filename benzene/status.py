"""The Benzene status vocabulary (wire-contracts.md section 3).

Statuses are lowercase-kebab-case *strings*, not an enum, so applications can extend them; an
unknown status is treated as a failure and maps to each transport's generic-error code.
"""

from __future__ import annotations

# Success-class statuses. Everything else (known failure statuses and any application extension)
# is a failure.
SUCCESS_STATUSES: frozenset[str] = frozenset(
    {"ok", "created", "accepted", "updated", "deleted", "ignored"}
)

FAILURE_STATUSES: frozenset[str] = frozenset(
    {
        "bad-request",
        "validation-error",
        "unauthorized",
        "forbidden",
        "not-found",
        "conflict",
        "too-many-requests",
        "timeout",
        "not-implemented",
        "service-unavailable",
        "unexpected-error",
    }
)

#: The closed set of framework-defined statuses (success + failure).
KNOWN_STATUSES: frozenset[str] = SUCCESS_STATUSES | FAILURE_STATUSES


class Status:
    """String constants for the framework-defined statuses (convenience, not a closed enum)."""

    OK = "ok"
    CREATED = "created"
    ACCEPTED = "accepted"
    UPDATED = "updated"
    DELETED = "deleted"
    IGNORED = "ignored"
    BAD_REQUEST = "bad-request"
    VALIDATION_ERROR = "validation-error"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not-found"
    CONFLICT = "conflict"
    TOO_MANY_REQUESTS = "too-many-requests"
    TIMEOUT = "timeout"
    NOT_IMPLEMENTED = "not-implemented"
    SERVICE_UNAVAILABLE = "service-unavailable"
    UNEXPECTED_ERROR = "unexpected-error"


def is_successful(status: str) -> bool:
    """A status is successful iff it is in the success class; unknown statuses are failures."""
    return status in SUCCESS_STATUSES
