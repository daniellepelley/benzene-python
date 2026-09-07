"""The store port — where captured envelopes live, and the one rule every implementation must keep.

The port is deliberately small and deliberately *claim-shaped*. There is no "list the pending ones"
method that hands an envelope to a caller without also reserving it: an unreserved list invites two
dispatchers to read the same envelope and both forward it, which is the one failure mode a store can
actually prevent. Reading is therefore :meth:`OutboxStore.claim_due` (claim a batch) or
:meth:`OutboxStore.claim` (claim one by id); :meth:`OutboxStore.get` reads a single envelope without
claiming it, for operators looking at parked evidence and for tests — never as a dispatch path.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from .envelope import OutboxEnvelope


@runtime_checkable
class OutboxStore(Protocol):
    """Durable storage for captured sends.

    **The hard requirement: :meth:`claim` and :meth:`claim_due` must be atomic per envelope.** A
    claim is a conditional transition — "if this envelope is pending, due, and not currently leased,
    lease it to me" — evaluated and applied in one indivisible operation (one SQL ``UPDATE ... WHERE``
    whose ``rowcount`` *is* the decision; one DynamoDB conditional ``UpdateItem``; in memory, a read
    and a write with no ``await`` between them). Everything the dispatcher promises rests on this,
    exactly as :class:`benzene.resilience.IdempotencyStore`'s ``put_if_absent`` does.

    **Why that makes dispatch idempotent enough to be safe.** A claim is a lease, not a lock: it
    lasts ``claim_lease`` seconds and then lapses, so a dispatcher that dies mid-forward does not
    strand the envelope — someone else picks it up. Combined, the two properties give:

    * *no concurrent double-send* — while a lease is live no other claimer can have the envelope;
    * *no loss* — a lapsed lease returns the envelope to the pending pool;
    * *at-least-once, never exactly-once* — a crash in the window between "the broker accepted" and
      :meth:`mark_dispatched` re-forwards the envelope on the next sweep. The duplicate is not
      hidden: it carries the same ``idempotency-key`` (the envelope id) as the first copy, so the
      consumer collapses it. Deduplication is the far end's job, by design.

    Lifecycle calls for an envelope that no longer exists are a **no-op**, never an error: retention
    may have deleted it between the claim and the report.
    """

    async def add(self, envelopes: Sequence[OutboxEnvelope]) -> None:
        """Persist newly captured envelopes as ``pending`` and due immediately.

        Takes a sequence because a transactional commit drains several at once; the capture point
        passes one.
        """
        ...

    async def claim_due(self, batch_size: int, lease: float) -> list[OutboxEnvelope]:
        """Atomically claim up to ``batch_size`` pending, due envelopes for ``lease`` seconds.

        Ordered by ``created_at`` where the store can offer that cheaply — best-effort only. It is
        not an ordering *guarantee*: retries reorder, and a stream-triggered relay has no order at
        all.
        """
        ...

    async def claim(self, envelope_id: str, lease: float) -> OutboxEnvelope | None:
        """Atomically claim one envelope by id, or ``None`` if it is gone, not pending, or leased."""
        ...

    async def get(self, envelope_id: str) -> OutboxEnvelope | None:
        """Read one envelope without claiming it — for operators and tests, never for dispatch."""
        ...

    async def mark_dispatched(self, envelope_id: str) -> None:
        """Record a successful send: ``dispatched``, lease released, retention clock started."""
        ...

    async def reschedule(
        self, envelope_id: str, attempt_count: int, delay: float, error: str
    ) -> None:
        """Record a failed attempt: stay ``pending``, release the lease, become due in ``delay``."""
        ...

    async def park(self, envelope_id: str, error: str) -> None:
        """Record terminal failure: ``parked``, never claimed again, never auto-deleted."""
        ...

    async def delete_dispatched_before(self, cutoff: float) -> int:
        """Delete ``dispatched`` envelopes dispatched at or before ``cutoff``; return how many.

        Never touches ``parked`` envelopes — they are the operator's evidence and only a human
        removes them. A store with native TTL may implement this as a no-op returning ``0``.
        """
        ...
