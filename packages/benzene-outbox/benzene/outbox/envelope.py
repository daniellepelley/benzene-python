"""The captured send, and the three states it can be in.

An :class:`OutboxEnvelope` is a *deferred* send: everything the dispatcher needs to perform it later
— topic, the already-encoded wire body, the headers as they stood at capture time — plus the retry
bookkeeping that decides when it is next tried and when it stops being tried at all.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum


class OutboxStatus(str, Enum):
    """Where an envelope is in its lifecycle.

    The whole state machine is three states and two terminal ones::

        pending ──(sent, store told)──▶ dispatched   (retained, then deleted)
           │
           └──(attempt budget spent)──▶ parked       (terminal; the operator's evidence)

    ``pending`` is the only claimable state, so a ``dispatched`` or ``parked`` envelope can never be
    picked up again — which is what stops a poison message from occupying the queue forever, and
    what stops a dispatched one from being re-sent by a later sweep.
    """

    PENDING = "pending"
    DISPATCHED = "dispatched"
    PARKED = "parked"


@dataclass(frozen=True)
class OutboxEnvelope:
    """One captured send, as a store hands it back: an immutable snapshot, never a live handle.

    ``payload`` is the JSON wire body produced by :func:`benzene.core.encode_body` — byte-identical
    to what an inline send would have put on the wire, so relaying is a re-send and not a
    re-serialization. ``headers`` is the post-stamping snapshot taken at capture: relaying replays
    the *business-time* ``traceparent``/``x-correlation-id``/``idempotency-key``, not the relay
    host's own ambient values.

    ``id`` is also the default ``idempotency-key`` value (see
    :attr:`~benzene.outbox.OutboxOptions.stamp_idempotency_key`) and is stable across every
    redelivery of this envelope — that stability is what lets a consumer collapse the duplicates
    that at-least-once delivery inevitably produces.
    """

    id: str
    topic: str
    payload: str
    headers: Mapping[str, str] = field(default_factory=dict)
    created_at: float = 0.0
    attempt_count: int = 0
    next_attempt_at: float | None = None
    status: OutboxStatus = OutboxStatus.PENDING
    last_error: str | None = None
    #: Set when the envelope reached :attr:`OutboxStatus.DISPATCHED`; the retention sweep's clock.
    dispatched_at: float | None = None
