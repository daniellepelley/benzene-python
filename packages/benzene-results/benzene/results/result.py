"""The Result type (core-concepts.md section 5).

Every handler invocation produces a Result — a value, never an exception, for domain outcomes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, TypeAlias, TypeVar

from .status import Status, is_successful

T = TypeVar("T")


def problem_errors(document: Mapping[str, Any]) -> tuple[BenzeneError, ...]:
    """The structured errors of a peer's problem document, in wire-contracts.md section 1.3's order
    of precedence.

    ``errors``, when present, is **authoritative and ordered**. Only when it is absent does
    ``detail`` stand in, and then as ONE opaque message - splitting it on ``", "`` was withdrawn by
    the RFC 9457 revision, because error messages contain commas.

    One function rather than the rule written out at each decode site: the envelope decoder had it
    right and the HTTP client read only ``detail``, so a peer's ``errors`` array - the authoritative
    half - was dropped depending on which client you happened to call through.
    """
    raw_errors = document.get("errors")
    if isinstance(raw_errors, list):
        decoded = tuple(
            BenzeneError.coerce(item if isinstance(item, (dict, str)) else str(item))
            for item in raw_errors
        )
        return tuple(error for error in decoded if error.message)

    detail = document.get("detail") or ""
    return (BenzeneError(str(detail)),) if detail else ()


#: What every failure factory accepts: a plain message, a structured error, or the mapping the wire
#: decoder produces. One parameter type rather than a parallel set of ``*_with`` factories - Python
#: takes the union where a language without overloads has to add a second function per status.
ErrorLike: TypeAlias = "str | BenzeneError | Mapping[str, Any]"


@dataclass(frozen=True)
class BenzeneError:
    """One structured error on a failed Result (wire-contracts.md section 1.3).

    ``message`` is the only required member and is what a plain-string failure carries. ``field`` is
    the producer's own property path and ``code`` its machine-readable rule identifier; the framework
    emits both verbatim and never normalizes or rewords them.

    A validator that knows a message, the field it came from and the rule that rejected it can say
    all three, and they travel to the caller's problem document intact. Without them a consumer gets
    prose to parse, which is the difference between an error a UI can attach to an input and one it
    can only print.
    """

    message: str
    field: str | None = None
    code: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """This error as a problem-document ``errors`` entry, omitting the members it does not have."""
        payload: dict[str, Any] = {"message": self.message}
        if self.field is not None:
            payload["field"] = self.field
        if self.code is not None:
            payload["code"] = self.code
        return payload

    @staticmethod
    def coerce(error: ErrorLike) -> BenzeneError:
        """Accept whichever of the three shapes a caller has and return a BenzeneError.

        A plain string becomes a message-only error, which is what keeps ``Result.bad_request("no")``
        exactly as cheap as it was. A mapping is what the wire decoder hands back.
        """
        if isinstance(error, BenzeneError):
            return error
        if isinstance(error, str):
            return BenzeneError(error)
        message = error.get("message")
        return BenzeneError(
            message="" if message is None else str(message),
            field=None if error.get("field") is None else str(error["field"]),
            code=None if error.get("code") is None else str(error["code"]),
        )


@dataclass(frozen=True)
class ProblemDetails:
    """An application-authored RFC 9457 problem document (wire-contracts.md section 1.3).

    The escape hatch for a service that owns its own problem vocabulary and wants its own ``type``
    URI on the wire rather than the registry URI Benzene would derive from the status. Hand one to
    :meth:`Result.problem` and the wire edge emits it verbatim.

    ``status`` - RFC 9457's integer HTTP code - is not something an application authors: an HTTP
    binding sets it to the code it is actually sending, and it is absent on every other transport.
    """

    benzene_status: str
    type: str | None = None
    title: str | None = None
    detail: str | None = None
    instance: str | None = None
    errors: tuple[BenzeneError, ...] = field(default_factory=tuple)

    def to_payload(self) -> dict[str, Any]:
        """This document as the wire body, omitting every member it does not carry."""
        payload: dict[str, Any] = {}
        for name, value in (
            ("type", self.type),
            ("title", self.title),
            ("detail", self.detail),
            ("instance", self.instance),
        ):
            if value is not None:
                payload[name] = value
        payload["benzeneStatus"] = self.benzene_status
        if self.errors:
            payload["errors"] = [error.to_payload() for error in self.errors]
        return payload


@dataclass(frozen=True)
class Result(Generic[T]):
    """The outcome of a handler invocation.

    Attributes:
        status: a status-vocabulary value (or an application extension).
        payload: the response payload, present on success (and optionally on failure).
        errors: zero or more structured errors, populated on failure. The failure factories take
            plain strings and wrap them, so a message-only failure stays a one-liner.
        problem_document: an application-authored problem document the wire edge emits verbatim, set
            only by :meth:`problem`. ``None`` for every other result, and then the document is
            derived from the status and ``errors`` as usual.
        successful: an explicit success classification that overrides the one derived from the
            status. ``None`` - the normal case - means derive it. Set it only through :meth:`set`.
    """

    status: str
    payload: T | None = None
    errors: tuple[BenzeneError, ...] = field(default_factory=tuple)
    problem_document: ProblemDetails | None = None
    successful: bool | None = None

    def __post_init__(self) -> None:
        """Coerce ``errors`` so every construction path produces structured errors.

        ``Result.failure`` and friends coerce their arguments, but ``Result(status, None, ("boom",))``
        is a perfectly natural thing to write - and a dataclass field annotation is documentation,
        not enforcement, so without this it would quietly store raw strings and every consumer of
        ``.message`` would fail somewhere far away. Coercing here means the annotation is true.
        """
        if any(not isinstance(error, BenzeneError) for error in self.errors):
            object.__setattr__(
                self, "errors", tuple(BenzeneError.coerce(error) for error in self.errors)
            )
        elif not isinstance(self.errors, tuple):
            object.__setattr__(self, "errors", tuple(self.errors))

    @property
    def is_successful(self) -> bool:
        """Whether this result is a success - the authoritative signal (wire-contracts.md 1.2).

        Derived from the status class unless :meth:`set` stated it outright.
        """
        if self.successful is not None:
            return self.successful
        return is_successful(self.status)

    @property
    def messages(self) -> tuple[str, ...]:
        """The error messages alone, for the callers that only ever wanted prose."""
        return tuple(error.message for error in self.errors)

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

    @staticmethod
    def set(status: str, payload: Any = None, successful: bool | None = None) -> Result[Any]:
        """A result whose success classification is stated outright, decoupled from its status.

        The intended use is the reserved health check returning ``service-unavailable`` so an HTTP
        probe sees a 503 and a load balancer drains the instance, while still rendering its report
        body rather than a problem document - the carve-out wire-contracts.md section 1.3 names,
        where the branch is on ``isSuccessful`` and not on status class.

        For ordinary results prefer ``ok`` / ``failure`` and the status-derived default; reach for
        this only when the transport outcome and the body's meaning genuinely diverge.
        """
        return Result(status, payload, successful=successful)

    # --- failure factories -----------------------------------------------------------------
    @staticmethod
    def failure(status: str, *errors: ErrorLike) -> Result[Any]:
        return Result(status, None, tuple(BenzeneError.coerce(error) for error in errors))

    @staticmethod
    def problem(document: ProblemDetails) -> Result[Any]:
        """A failure carrying an application-authored problem document, emitted verbatim.

        Use it when the service owns its own problem vocabulary and wants its own ``type`` URI to
        reach the caller; every other factory derives the right document from the section 3.1
        registry. Raises ``ValueError`` on a document with no ``benzene_status``: nothing downstream
        could classify it.
        """
        if not document.benzene_status:
            raise ValueError(
                "ProblemDetails.benzene_status is required - a problem document with no status "
                "cannot be classified by anything downstream"
            )
        return Result(document.benzene_status, None, tuple(document.errors), document, successful=False)

    @staticmethod
    def bad_request(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.BAD_REQUEST, *errors)

    @staticmethod
    def validation_error(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.VALIDATION_ERROR, *errors)

    @staticmethod
    def unauthorized(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.UNAUTHORIZED, *errors)

    @staticmethod
    def forbidden(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.FORBIDDEN, *errors)

    @staticmethod
    def not_found(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.NOT_FOUND, *errors)

    @staticmethod
    def conflict(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.CONFLICT, *errors)

    @staticmethod
    def too_many_requests(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.TOO_MANY_REQUESTS, *errors)

    @staticmethod
    def timeout(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.TIMEOUT, *errors)

    @staticmethod
    def not_implemented(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.NOT_IMPLEMENTED, *errors)

    @staticmethod
    def service_unavailable(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.SERVICE_UNAVAILABLE, *errors)

    @staticmethod
    def unexpected_error(*errors: ErrorLike) -> Result[Any]:
        return Result.failure(Status.UNEXPECTED_ERROR, *errors)


def result_with_errors(status: str, errors: Sequence[ErrorLike]) -> Result[Any]:
    """Build a failure result from a status and an errors sequence (used by the wire decoder)."""
    return Result(status, None, tuple(BenzeneError.coerce(error) for error in errors))
