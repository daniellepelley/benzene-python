"""The Result type (core-concepts.md section 5).

Every handler invocation produces a Result — a value, never an exception, for domain outcomes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from .status import Status, is_successful

T = TypeVar("T")


@dataclass(frozen=True)
class Result(Generic[T]):
    """The outcome of a handler invocation.

    Attributes:
        status: a status-vocabulary value (or an application extension).
        payload: the response payload, present on success (and optionally on failure).
        errors: zero or more human-readable error messages, populated on failure.
    """

    status: str
    payload: T | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def is_successful(self) -> bool:
        return is_successful(self.status)

    # --- success factories -----------------------------------------------------------------
    @staticmethod
    def ok(payload: T | None = None) -> Result[T]:
        return Result(Status.OK, payload)

    @staticmethod
    def created(payload: T | None = None) -> Result[T]:
        return Result(Status.CREATED, payload)

    @staticmethod
    def accepted(payload: T | None = None) -> Result[T]:
        return Result(Status.ACCEPTED, payload)

    @staticmethod
    def updated(payload: T | None = None) -> Result[T]:
        return Result(Status.UPDATED, payload)

    @staticmethod
    def deleted(payload: T | None = None) -> Result[T]:
        return Result(Status.DELETED, payload)

    @staticmethod
    def ignored(payload: T | None = None) -> Result[T]:
        return Result(Status.IGNORED, payload)

    # --- failure factories -----------------------------------------------------------------
    @staticmethod
    def failure(status: str, *errors: str) -> Result[Any]:
        return Result(status, None, tuple(errors))

    @staticmethod
    def bad_request(*errors: str) -> Result[Any]:
        return Result.failure(Status.BAD_REQUEST, *errors)

    @staticmethod
    def validation_error(*errors: str) -> Result[Any]:
        return Result.failure(Status.VALIDATION_ERROR, *errors)

    @staticmethod
    def unauthorized(*errors: str) -> Result[Any]:
        return Result.failure(Status.UNAUTHORIZED, *errors)

    @staticmethod
    def forbidden(*errors: str) -> Result[Any]:
        return Result.failure(Status.FORBIDDEN, *errors)

    @staticmethod
    def not_found(*errors: str) -> Result[Any]:
        return Result.failure(Status.NOT_FOUND, *errors)

    @staticmethod
    def conflict(*errors: str) -> Result[Any]:
        return Result.failure(Status.CONFLICT, *errors)

    @staticmethod
    def too_many_requests(*errors: str) -> Result[Any]:
        return Result.failure(Status.TOO_MANY_REQUESTS, *errors)

    @staticmethod
    def timeout(*errors: str) -> Result[Any]:
        return Result.failure(Status.TIMEOUT, *errors)

    @staticmethod
    def not_implemented(*errors: str) -> Result[Any]:
        return Result.failure(Status.NOT_IMPLEMENTED, *errors)

    @staticmethod
    def service_unavailable(*errors: str) -> Result[Any]:
        return Result.failure(Status.SERVICE_UNAVAILABLE, *errors)

    @staticmethod
    def unexpected_error(*errors: str) -> Result[Any]:
        return Result.failure(Status.UNEXPECTED_ERROR, *errors)


def result_with_errors(status: str, errors: Sequence[str]) -> Result[Any]:
    """Build a failure result from a status and an errors sequence (used by the wire decoder)."""
    return Result(status, None, tuple(errors))
