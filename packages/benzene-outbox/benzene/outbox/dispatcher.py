"""The forward half: claim due envelopes, send them for real, and decide what happens when that fails."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from benzene.core import MessageSender
from benzene.results import Result

from .envelope import OutboxEnvelope
from .options import OutboxOptions
from .store import OutboxStore


class OutboxDispatchOutcome(str, Enum):
    """What one envelope's dispatch attempt did."""

    DISPATCHED = "dispatched"
    RESCHEDULED = "rescheduled"
    PARKED = "parked"
    #: Someone else holds the lease, or the envelope is gone or no longer pending. Not a failure.
    CLAIM_REFUSED = "claim-refused"


@dataclass(frozen=True)
class OutboxDispatchResult:
    """What one :meth:`OutboxDispatcher.run_once` did — the numbers worth logging or alerting on."""

    dispatched: int = 0
    rescheduled: int = 0
    parked: int = 0
    deleted: int = 0


class OutboxDispatcher:
    """Drains pending envelopes through a real :class:`~benzene.core.MessageSender`.

    **What this guarantees.** Every captured envelope is sent *at least once*, eventually, or ends up
    ``parked`` with the reason recorded. Failures are retried with exponential backoff that survives
    process restarts (it lives in the store, not in memory), and a lease means two dispatchers do not
    forward the same envelope concurrently.

    **What it does not.** Not exactly-once: the window between "the broker accepted" and
    ``mark_dispatched`` is real, and a crash inside it re-forwards the envelope. That is why capture
    stamps the envelope id as ``idempotency-key`` — pair the outbox with
    :func:`benzene.resilience.idempotency_interception` at the consumer and the duplicate collapses
    there. Not ordered, either: ``claim_due`` sweeps by ``created_at`` best-effort, but a retried
    envelope moves behind newer ones and a stream-triggered relay has no order at all. If your
    consumer needs order, it must establish it from the payload.

    ``sender`` must be the **real transport** — never the :class:`~benzene.outbox.OutboxMessageSender`
    that captured the envelope, which would re-capture it into an endless loop. Wrap it in
    ``with_retry``/``with_circuit_breaker`` here if you want in-attempt guarding.
    """

    def __init__(
        self,
        store: OutboxStore,
        sender: MessageSender,
        *,
        options: OutboxOptions | None = None,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ) -> None:
        self._store = store
        self._sender = sender
        self._options = options or OutboxOptions()
        self._clock = clock
        self._logger = logger or logging.getLogger("benzene.outbox")

    async def run_once(self) -> OutboxDispatchResult:
        """Claim a batch of due envelopes, forward each, then retire what retention has aged out.

        Each envelope is dispatched independently, so one that fails delays only itself: its own
        backoff pushes it behind the others rather than holding the batch up. That, plus parking at
        the attempt budget, is why a poison envelope cannot block the queue.
        """
        counts = dict.fromkeys(OutboxDispatchOutcome, 0)
        for envelope in await self._store.claim_due(
            self._options.batch_size, self._options.claim_lease
        ):
            counts[await self._dispatch(envelope)] += 1
        deleted = await self._store.delete_dispatched_before(
            self._clock() - self._options.retention
        )
        return OutboxDispatchResult(
            dispatched=counts[OutboxDispatchOutcome.DISPATCHED],
            rescheduled=counts[OutboxDispatchOutcome.RESCHEDULED],
            parked=counts[OutboxDispatchOutcome.PARKED],
            deleted=deleted,
        )

    async def dispatch_one(self, envelope_id: str) -> OutboxDispatchOutcome:
        """Claim and forward one envelope — the change-stream relay path (DynamoDB Streams, a CDC
        feed, an ``INSERT`` trigger).

        A stream fires once per insert, so it cannot drive retries: run it *alongside* a low-frequency
        :meth:`run_once` sweep, which is what retries, parks and retires. Returns
        :attr:`~OutboxDispatchOutcome.CLAIM_REFUSED` when another dispatcher already holds it — the
        normal, uninteresting outcome of a stream racing a sweep, not an error.
        """
        envelope = await self._store.claim(envelope_id, self._options.claim_lease)
        if envelope is None:
            return OutboxDispatchOutcome.CLAIM_REFUSED
        return await self._dispatch(envelope)

    async def _dispatch(self, envelope: OutboxEnvelope) -> OutboxDispatchOutcome:
        status: str | None = None
        try:
            # The envelope's stored headers win over anything the relay host would stamp: the
            # correlation id and traceparent belong to when the send was captured, not to now.
            result = await self._sender.send_message(
                envelope.topic, json.loads(envelope.payload), dict(envelope.headers)
            )
        except Exception as exc:  # a transport/infrastructure fault — assumed transient
            error = f"{type(exc).__name__}: {exc}"
        else:
            if result.is_successful:
                await self._store.mark_dispatched(envelope.id)
                return OutboxDispatchOutcome.DISPATCHED
            # Python senders answer with a Result rather than raising, so an unsuccessful Result is
            # a failed attempt just as an exception is — and its status tells us whether retrying
            # could ever help (a deliberate divergence from .NET, whose sender only ever throws).
            status = result.status
            error = _describe(result)
        return await self._failed(envelope, status, error)

    async def _failed(
        self, envelope: OutboxEnvelope, status: str | None, error: str
    ) -> OutboxDispatchOutcome:
        attempt = envelope.attempt_count + 1
        if attempt >= self._options.budget_for(status):
            self._logger.error(
                "outbox envelope %s for topic %r failed on attempt %d (%s); parking it — it will "
                "not be retried or deleted automatically",
                envelope.id,
                envelope.topic,
                attempt,
                error,
            )
            await self._store.park(envelope.id, error)
            return OutboxDispatchOutcome.PARKED
        delay = self._options.backoff_for(attempt)
        self._logger.warning(
            "outbox envelope %s for topic %r failed on attempt %d/%d (%s); retrying in %.1fs",
            envelope.id,
            envelope.topic,
            attempt,
            self._options.budget_for(status),
            error,
            delay,
        )
        await self._store.reschedule(envelope.id, attempt, delay, error)
        return OutboxDispatchOutcome.RESCHEDULED


def _describe(result: Result) -> str:
    """A failed send's status and prose, for the envelope's ``last_error``."""
    detail = "; ".join(result.messages)
    return f"{result.status}: {detail}" if detail else result.status
