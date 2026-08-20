"""``benzene.results`` — the Result type and the status vocabulary.

The bottom layer of the Benzene Python port (distribution ``benzene-results``). A domain handler
author needs only this: return ``Result.ok(...)`` / ``Result.not_found(...)`` and never raise for
an expected outcome. Zero third-party dependencies.

Mirrors .NET's ``Benzene.Results`` plus the status vocabulary from ``Benzene.Abstractions``.
"""

from __future__ import annotations

from .result import BenzeneError, ErrorLike, ProblemDetails, Result, result_with_errors
from .status import (
    FAILURE_STATUSES,
    KNOWN_STATUSES,
    SUCCESS_STATUSES,
    Status,
    is_successful,
)

__all__ = [
    "BenzeneError",
    "ErrorLike",
    "FAILURE_STATUSES",
    "KNOWN_STATUSES",
    "ProblemDetails",
    "Result",
    "SUCCESS_STATUSES",
    "Status",
    "is_successful",
    "result_with_errors",
]
