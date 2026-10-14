"""The issues feed — deduplicated failure signatures with classification (mesh.md §, Issues).

A service reports *failures* to the collector as a batch of **issues**: one deduplicated signature per
``(topic, version, classification, discriminator)`` with a delta count since the last flush, a first/
last-seen window, and a few exemplar trace ids. The collector merges these by ``fingerprint`` across
instances into a fleet-wide failure view.

Two pieces are normative and live here so a Python service produces the *same* signatures a .NET one
would (cross-language merge depends on it):

- :func:`classify` — the closed classification vocabulary and its precedence order.
- :func:`issue_fingerprint` — the exact SHA-256-derived signature.

:class:`IssueAggregator` is the pit of success: ``record(...)`` each failure, ``flush()`` to a
:class:`IssueBatch` (which resets the window, so ``count`` is always a delta).

An issue may also carry its **error detail** (mesh.md §4.1): the ``(field, code)`` pairs behind the
failures it counts — never the message — as :class:`IssueError` delta counts. :func:`merge_issue_errors`
is the one merge-and-bound rule both sides share (the aggregator at its flush, the collector on
ingest), and :func:`decode_issue_errors` the collector's lenient reading of the wire member.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from benzene.results import BenzeneError, ErrorLike

# The closed classification vocabulary (mesh.md). `contract-drift` is never produced by classify() —
# it is reserved for collector/reader-derived issues (registration-hash mismatch, schema divergence).
CLASSIFICATIONS: frozenset[str] = frozenset(
    {"exception", "validation", "config-wiring", "dependency", "contract-drift", "unclassified"}
)

_MAX_EXEMPLARS = 5

#: An issue's error detail keeps at most this many entries, on both sides of the feed (mesh.md §4.1).
MAX_ISSUE_ERRORS = 10
# The distinct (field, code) pairs one issue accumulates in a flush window. The bound to
# MAX_ISSUE_ERRORS is applied only at the flush, so a pair that turns frequent late in the window is
# not evicted early; past this many, a new pair is not admitted, which caps the window's memory.
_MAX_PENDING_ISSUE_ERRORS = 100


def classify(status: str, exception_type: str | None = None) -> str:
    """Classify a failure by its status + optional exception type, in the spec's precedence order."""
    if status in ("bad-request", "validation-error"):
        return "validation"
    if exception_type:
        return "exception"
    if status in ("not-found", "unauthorized", "forbidden", "not-implemented") or not status:
        return "config-wiring"
    if status in ("service-unavailable", "timeout", "too-many-requests"):
        return "dependency"
    if status == "unexpected-error":
        return "exception"
    return "unclassified"


def issue_fingerprint(
    service: str, topic: str, version: str, classification: str, discriminator: str
) -> str:
    """``lowercase-hex(sha256(service|topic|version|classification|discriminator)[:16])`` (mesh.md).

    ``version`` is the empty string when absent; ``discriminator`` is the exception type when present,
    else the status. Two instances of the same build hash the same signature identically.
    """
    raw = "|".join([service, topic, version or "", classification, discriminator])
    return hashlib.sha256(raw.encode("utf-8")).digest()[:16].hex()


@dataclass(frozen=True)
class IssueError:
    """One entry of an issue's error detail (mesh.md §4.1).

    Its identity is the ``(field, code)`` pair. An empty string is the same as an absent member
    (``None``), and an absent member is distinct from every non-empty string; :func:`merge_issue_errors`
    and :func:`decode_issue_errors` normalise ``""`` to ``None``. ``count`` is a delta on the wire,
    exactly as the issue's own is.
    There is deliberately no ``message``: it is free text a validator may have built from the payload.
    """

    field: str | None = None
    code: str | None = None
    count: int = 1

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.field is not None:
            payload["field"] = self.field
        if self.code is not None:
            payload["code"] = self.code
        payload["count"] = self.count
        return payload


def _issue_error_order(entry: IssueError) -> tuple[int, int, str, int, str]:
    """Count descending, then field, then code — ordinal, an absent member first (mesh.md §4.1)."""
    return (
        -entry.count,
        entry.field is not None,
        entry.field or "",
        entry.code is not None,
        entry.code or "",
    )


def merge_issue_errors(
    existing: Iterable[IssueError], delta: Iterable[IssueError]
) -> tuple[IssueError, ...]:
    """Add ``delta`` into ``existing`` by ``(field, code)`` identity and bound the result (mesh.md §4.1).

    The :data:`MAX_ISSUE_ERRORS` highest counts survive, listed count descending, then by field, then
    by code (ordinal, absent first). An empty member is an absent one, and an entry with neither is
    dropped. The aggregator bounds a flush window's detail with it and the
    collector merges a batch's deltas into an issue with it, so both sides keep the same entries in the
    same order.
    """
    counts: dict[tuple[str | None, str | None], int] = {}
    for entry in (*existing, *delta):
        key = (entry.field or None, entry.code or None)  # "" is absent (mesh.md §4.1)
        if key == (None, None):
            continue
        counts[key] = counts.get(key, 0) + entry.count
    merged = sorted(
        (IssueError(field=f, code=c, count=n) for (f, c), n in counts.items()),
        key=_issue_error_order,
    )
    return tuple(merged[:MAX_ISSUE_ERRORS])


def decode_issue_errors(raw: Any) -> tuple[IssueError, ...]:
    """Read an issue's wire ``errors`` member leniently (mesh.md §4.1).

    An entry that is not an object, has neither ``field`` nor ``code`` (an empty string being an
    absent member), carries a non-string one, or
    whose ``count`` is not a positive integer is skipped — one malformed entry never costs the issue,
    or the batch, it arrived in. A member that is not an array is no detail at all. A JSON ``true`` is
    not a count (``bool`` is an ``int`` in Python), and neither is ``1.5`` nor ``2.0``.
    """
    if not isinstance(raw, list):
        return ()
    entries: list[IssueError] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        count = item.get("count")
        if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
            continue
        field_value, code = item.get("field"), item.get("code")
        if field_value is not None and not isinstance(field_value, str):
            continue
        if code is not None and not isinstance(code, str):
            continue
        field_value, code = field_value or None, code or None  # "" is absent (mesh.md §4.1)
        if field_value is None and code is None:
            continue
        entries.append(IssueError(field=field_value, code=code, count=count))
    return tuple(entries)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Issue:
    """One deduplicated failure signature (mesh.md). ``to_payload`` is its camelCase wire form."""

    fingerprint: str
    classification: str
    service: str
    topic: str
    status: str
    version: str = ""
    transport: str | None = None
    exception_type: str | None = None
    count: int = 1
    first_seen: str | None = None
    last_seen: str | None = None
    exemplar_trace_ids: tuple[str, ...] = ()
    resolution_hint: str | None = None
    # The (field, code) detail behind the failures counted — bounded, ordered, delta counts (§4.1).
    errors: tuple[IssueError, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "fingerprint": self.fingerprint,
            "classification": self.classification,
            "service": self.service,
            "topic": self.topic,
            "status": self.status,
            "count": self.count,
        }
        if self.version:
            payload["version"] = self.version
        optional = {
            "transport": self.transport,
            "exceptionType": self.exception_type,
            "firstSeen": self.first_seen,
            "lastSeen": self.last_seen,
            "resolutionHint": self.resolution_hint,
        }
        payload.update({k: v for k, v in optional.items() if v is not None})
        if self.exemplar_trace_ids:
            payload["exemplarTraceIds"] = list(self.exemplar_trace_ids)
        if self.errors:
            payload["errors"] = [error.to_payload() for error in self.errors]
        return payload


@dataclass
class IssueBatch:
    """A batch of issues for one service (mesh.md). An empty ``issues`` list is a liveness assertion."""

    service: str  # REQUIRED at the batch level
    issues: list[Issue] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {"service": self.service, "issues": [issue.to_payload() for issue in self.issues]}


class IssueAggregator:
    """Accumulates failures by fingerprint; :meth:`flush` returns a delta-counted :class:`IssueBatch`.

    ``record(...)`` each observed failure (typically from a trace or a handler result); ``flush()``
    drains everything seen since the previous flush and resets, so every reported ``count`` is a
    delta, never a cumulative total (mesh.md). Safe to flush an empty aggregator — that batch is the
    feed's liveness beat.

    Pass the failing result's ``errors`` to ``record`` and the issue carries its error detail: each
    error's ``(field, code)`` pair is counted, its message never kept, and an error with neither is not
    counted. The window's pairs are bounded to :data:`MAX_ISSUE_ERRORS` at the flush (mesh.md §4.1).
    """

    def __init__(self, service: str) -> None:
        self._service = service
        self._issues: dict[str, Issue] = {}
        # fingerprint -> {(field, code): count} for the current window; bounded only at the flush.
        self._errors: dict[str, dict[tuple[str | None, str | None], int]] = {}

    def record(
        self,
        *,
        topic: str,
        status: str,
        version: str = "",
        transport: str | None = None,
        exception_type: str | None = None,
        trace_id: str | None = None,
        resolution_hint: str | None = None,
        at: str | None = None,
        errors: Iterable[ErrorLike] = (),
    ) -> Issue:
        """Record one failure occurrence, merging into its fingerprint's running issue.

        ``errors`` is the failing result's structured errors (``result.errors``, or the mappings a
        problem document decodes to): only each one's ``field`` and ``code`` are counted.
        """
        classification = classify(status, exception_type)
        discriminator = exception_type or status
        fingerprint = issue_fingerprint(self._service, topic, version, classification, discriminator)
        when = at or _now_iso()

        issue = self._issues.get(fingerprint)
        if issue is None:
            issue = Issue(
                fingerprint=fingerprint,
                classification=classification,
                service=self._service,
                topic=topic,
                status=status,
                version=version,
                transport=transport,
                exception_type=exception_type,
                count=0,
                first_seen=when,
                last_seen=when,
                resolution_hint=resolution_hint,
            )
            self._issues[fingerprint] = issue
        issue.count += 1
        issue.last_seen = when
        if trace_id and len(issue.exemplar_trace_ids) < _MAX_EXEMPLARS:
            issue.exemplar_trace_ids = (*issue.exemplar_trace_ids, trace_id)
        self._count_errors(fingerprint, errors)
        return issue

    def _count_errors(self, fingerprint: str, errors: Iterable[ErrorLike]) -> None:
        pending = self._errors.setdefault(fingerprint, {})
        for error in errors:
            coerced = BenzeneError.coerce(error)
            # Never the message; an empty member is an absent one (mesh.md §4.1).
            pair = (coerced.field or None, coerced.code or None)
            if pair == (None, None):
                continue  # an error with neither member says nothing an issue can count
            if pair in pending or len(pending) < _MAX_PENDING_ISSUE_ERRORS:
                pending[pair] = pending.get(pair, 0) + 1

    def flush(self) -> IssueBatch:
        """Return everything accumulated since the last flush and reset (so ``count`` stays a delta).

        Each issue's error detail is bounded and ordered here, at the flush (mesh.md §4.1).
        """
        for fingerprint, issue in self._issues.items():
            pending = self._errors.get(fingerprint, {})
            issue.errors = merge_issue_errors(
                (), (IssueError(field=f, code=c, count=n) for (f, c), n in pending.items())
            )
        batch = IssueBatch(service=self._service, issues=list(self._issues.values()))
        self._issues = {}
        self._errors = {}
        return batch
