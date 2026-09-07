"""The one options object both halves of the outbox read — capture, and dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from benzene.core import DEFAULT_RETRYABLE

#: The header a captured envelope's id is stamped into, so an outbox producer and a consumer running
#: :func:`benzene.resilience.idempotency_interception` click together with no configuration. A
#: deliberate value duplicate of .NET's ``OutboxDefaults.IdempotencyKeyHeaderName`` and of
#: ``benzene.resilience.DEFAULT_KEY_HEADERS[0]``: this package takes no dependency on
#: ``benzene-resilience`` for one string. If you change one, change the other.
IDEMPOTENCY_KEY_HEADER = "idempotency-key"

#: How the capture point disposes of an envelope. See :class:`OutboxOptions.write_mode`.
OutboxWriteMode = Literal["immediate", "transactional"]


@dataclass(frozen=True)
class OutboxOptions:
    """Capture and dispatch settings, at .NET's defaults.

    **The two write modes, and what each honestly buys** (the same boundary
    ``Benzene.Outbox/CLAUDE.md`` states — neither is exactly-once):

    - ``"immediate"`` (default) — **store-and-forward.** Capture writes the envelope straight to the
      :class:`~benzene.outbox.OutboxStore`. The send now survives process death and broker outages
      and is retried with backoff, so it can no longer be silently swallowed by a ``try/except`` —
      but the envelope write and your state write are still two independent writes. Both are durable;
      neither is atomic with the other.
    - ``"transactional"`` — **the atomic story, and it needs your transaction to be real.** Capture
      hands the envelope to an :class:`~benzene.outbox.OutboxStage` instead of writing it, and *your*
      commit persists your state and the envelope together. With
      :class:`~benzene.outbox.SqlOutboxStage` that is one database transaction on your own
      connection; with :func:`~benzene.outbox.outbox_transaction`'s buffered stage it is whatever
      your ``commit`` callable does. An envelope staged and never committed is discarded — consistent
      by construction, because your state write was not committed either.

    ``max_attempts`` and ``retry_on`` together bound how long a poison envelope is retried; see
    :meth:`budget_for`. ``backoff_base``/``backoff_cap`` shape the delay between attempts
    (``min(base * 2 ** (attempt - 1), cap)``), ``claim_lease`` is the crash-recovery window (how long
    after a dispatcher dies mid-forward before another may take the envelope over), ``retention`` is
    how long a dispatched envelope is kept for audit, and ``poll_interval`` paces the dispatcher
    loop. Every duration is in **seconds**.
    """

    max_attempts: int = 10
    backoff_base: float = 30.0
    backoff_cap: float = 3_600.0
    retention: float = 604_800.0
    batch_size: int = 25
    claim_lease: float = 120.0
    poll_interval: float = 5.0
    stamp_idempotency_key: bool = True
    write_mode: OutboxWriteMode = "immediate"
    #: The failure statuses worth trying again at all — the same set the core's retry decorator and
    #: the Kafka dead-letter bound use.
    retry_on: frozenset[str] = DEFAULT_RETRYABLE

    def budget_for(self, status: str | None) -> int:
        """Attempts this envelope gets before it is parked — one, if retrying cannot help.

        The same rule the Kafka consumer's dead-letter bound applies, for the same reason: a send
        the broker *refused* on the merits (``bad-request``, ``not-found``) will be refused
        identically on every retry, so burning ten attempts and an hour of backoff on it only delays
        the operator's evidence. A failure inside :attr:`retry_on` — or a raised exception, which is
        a transport/infrastructure fault and is where ``status`` is ``None`` — gets the full budget.
        """
        if status is not None and status not in self.retry_on:
            return 1
        return max(1, self.max_attempts)

    def backoff_for(self, attempt: int) -> float:
        """The delay before ``attempt`` is retried: exponential from ``backoff_base``, capped.

        The exponent is clamped before the power is taken, so a store that somehow hands back a huge
        ``attempt_count`` produces the cap rather than an overflow.
        """
        exponent = min(max(0, attempt - 1), 32)
        return min(self.backoff_base * (2.0**exponent), self.backoff_cap)
