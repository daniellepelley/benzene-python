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
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# The closed classification vocabulary (mesh.md). `contract-drift` is never produced by classify() —
# it is reserved for collector/reader-derived issues (descriptor-hash mismatch, schema divergence).
CLASSIFICATIONS: frozenset[str] = frozenset(
    {"exception", "validation", "config-wiring", "dependency", "contract-drift", "unclassified"}
)

_MAX_EXEMPLARS = 5


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
    """

    def __init__(self, service: str) -> None:
        self._service = service
        self._issues: dict[str, Issue] = {}

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
    ) -> Issue:
        """Record one failure occurrence, merging into its fingerprint's running issue."""
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
        return issue

    def flush(self) -> IssueBatch:
        """Return everything accumulated since the last flush and reset (so ``count`` stays a delta)."""
        batch = IssueBatch(service=self._service, issues=list(self._issues.values()))
        self._issues = {}
        return batch
