"""Benzene ↔ HTTP status mapping (wire-contracts.md section 4.1).

Used by an HTTP transport binding to translate a Benzene status to an HTTP code (forward) and by an
HTTP client to recover a Benzene status from an HTTP code (reverse).
"""

from __future__ import annotations

from benzene.results import Status

_BENZENE_TO_HTTP: dict[str, int] = {
    Status.OK: 200,
    Status.IGNORED: 200,
    Status.CREATED: 201,
    Status.ACCEPTED: 202,
    Status.UPDATED: 204,
    Status.DELETED: 204,
    Status.BAD_REQUEST: 400,
    Status.UNAUTHORIZED: 401,
    Status.FORBIDDEN: 403,
    Status.NOT_FOUND: 404,
    Status.CONFLICT: 409,
    Status.VALIDATION_ERROR: 422,
    Status.TOO_MANY_REQUESTS: 429,
    Status.UNEXPECTED_ERROR: 500,
    Status.NOT_IMPLEMENTED: 501,
    Status.SERVICE_UNAVAILABLE: 503,
    Status.TIMEOUT: 504,
}

_HTTP_TO_BENZENE: dict[int, str] = {
    200: Status.OK,
    201: Status.CREATED,
    202: Status.ACCEPTED,
    204: Status.DELETED,
    400: Status.BAD_REQUEST,
    401: Status.UNAUTHORIZED,
    403: Status.FORBIDDEN,
    404: Status.NOT_FOUND,
    408: Status.TIMEOUT,
    409: Status.CONFLICT,
    422: Status.VALIDATION_ERROR,
    429: Status.TOO_MANY_REQUESTS,
    501: Status.NOT_IMPLEMENTED,
    502: Status.SERVICE_UNAVAILABLE,
    503: Status.SERVICE_UNAVAILABLE,
    504: Status.TIMEOUT,
}


def to_http(status: str) -> int:
    """Benzene status → HTTP code. Unknown/missing → 500."""
    return _BENZENE_TO_HTTP.get(status, 500)


def from_http(http_code: int) -> str:
    """HTTP code → Benzene status. Anything unmapped → unexpected-error."""
    return _HTTP_TO_BENZENE.get(http_code, Status.UNEXPECTED_ERROR)
