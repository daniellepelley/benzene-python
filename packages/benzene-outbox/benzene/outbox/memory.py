"""The in-process store: always available, and honest about being single-process."""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable, Iterable, Sequence

from .envelope import OutboxEnvelope, OutboxStatus

Clock = Callable[[], float]


class InMemoryOutboxStore:
    """An :class:`~benzene.outbox.OutboxStore` in a dict — for tests and single-process services.

    **Single-process, and that is the whole caveat.** Envelopes captured in this process are only
    ever dispatched by this process, and they die with it: the durability an outbox exists to
    provide is exactly what an in-memory store cannot give. Use it to develop against, to test
    against, and in a single-instance service that can tolerate losing in-flight sends on restart;
    use :class:`~benzene.outbox.SqlOutboxStore` (or another durable store) anywhere else.

    Claims are atomic by construction: every claim reads and writes ``_envelopes`` with no ``await``
    in between, so no other task can interleave on the single-threaded event loop — the same
    argument :meth:`benzene.resilience.InMemoryIdempotencyStore.put_if_absent` documents.

    ``clock`` is injectable so a test drives leases, backoff and retention without sleeping.
    """

    def __init__(self, *, clock: Clock = time.time) -> None:
        self._clock = clock
        self._envelopes: dict[str, OutboxEnvelope] = {}
        self._leases: dict[str, float] = {}

    def ids(self) -> Iterable[str]:
        """Every envelope id currently held, in insertion order (introspection, not dispatch)."""
        return tuple(self._envelopes)

    async def add(self, envelopes: Sequence[OutboxEnvelope]) -> None:
        for envelope in envelopes:
            self._envelopes[envelope.id] = envelope

    async def claim_due(self, batch_size: int, lease: float) -> list[OutboxEnvelope]:
        now = self._clock()
        due = sorted(
            (e for e in self._envelopes.values() if self._is_due(e, now)),
            key=lambda e: e.created_at,
        )
        return [self._lease(envelope, now + lease) for envelope in due[: max(0, batch_size)]]

    async def claim(self, envelope_id: str, lease: float) -> OutboxEnvelope | None:
        now = self._clock()
        envelope = self._envelopes.get(envelope_id)
        if envelope is None or not self._is_due(envelope, now):
            return None
        return self._lease(envelope, now + lease)

    async def get(self, envelope_id: str) -> OutboxEnvelope | None:
        return self._envelopes.get(envelope_id)

    async def mark_dispatched(self, envelope_id: str) -> None:
        self._settle(envelope_id, status=OutboxStatus.DISPATCHED, dispatched_at=self._clock())

    async def reschedule(
        self, envelope_id: str, attempt_count: int, delay: float, error: str
    ) -> None:
        self._settle(
            envelope_id,
            status=OutboxStatus.PENDING,
            attempt_count=attempt_count,
            next_attempt_at=self._clock() + delay,
            last_error=error,
        )

    async def park(self, envelope_id: str, error: str) -> None:
        self._settle(envelope_id, status=OutboxStatus.PARKED, last_error=error)

    async def delete_dispatched_before(self, cutoff: float) -> int:
        retired = [
            envelope_id
            for envelope_id, envelope in self._envelopes.items()
            if envelope.status is OutboxStatus.DISPATCHED
            and envelope.dispatched_at is not None
            and envelope.dispatched_at <= cutoff
        ]
        for envelope_id in retired:
            del self._envelopes[envelope_id]
        return len(retired)

    # --- internals -------------------------------------------------------------------------------

    def _is_due(self, envelope: OutboxEnvelope, now: float) -> bool:
        """Pending, its backoff elapsed, and nobody else's live lease on it."""
        return (
            envelope.status is OutboxStatus.PENDING
            and (envelope.next_attempt_at is None or envelope.next_attempt_at <= now)
            and self._leases.get(envelope.id, now) <= now
        )

    def _lease(self, envelope: OutboxEnvelope, until: float) -> OutboxEnvelope:
        self._leases[envelope.id] = until
        return envelope

    def _settle(self, envelope_id: str, **changes: object) -> None:
        """Apply a lifecycle transition and release the lease. Unknown envelope: a no-op."""
        envelope = self._envelopes.get(envelope_id)
        if envelope is None:
            return  # retention may have removed it between the claim and the report
        self._envelopes[envelope_id] = dataclasses.replace(envelope, **changes)  # type: ignore[arg-type]
        self._leases.pop(envelope_id, None)
