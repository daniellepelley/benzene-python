"""``benzene.outbox`` — the transactional outbox (distribution ``benzene-outbox``).

**The problem.** A handler that writes state and then sends a message performs two independent
operations. If the send fails after the write commits, the message is lost and nothing records that
it was owed; if the process dies between them, the same. Sending *before* the write is no better —
now the message describes state that was never written. There is no ordering of two independent
writes that makes them agree, which is why the answer is not an ordering but a third thing: record
the send **as data, next to the state write**, and forward it afterwards.

**What this package does.** ``with_outbox`` wraps the one outbound seam
(:class:`benzene.core.MessageSender`), so a send at the call site is unchanged but becomes a durable
record and an ``accepted`` answer. :class:`OutboxDispatcher` later drains those records through the
real transport, retrying with backoff that survives restarts, and parks what can never succeed.

**What it guarantees, exactly.** It converts *"lost send"* into *"delayed, at-least-once send"*. It
does **not** make the send exactly-once, and nothing here claims to:

* Duplicates are possible and expected — a crash between the broker accepting and the store being
  told re-forwards the envelope. Capture therefore stamps the envelope id as ``idempotency-key``, so
  pairing the outbox with :func:`benzene.resilience.idempotency_interception` at the consumer makes
  the *effect* happen once. Deduplication is the far end's job, by design.
* There is **no ordering guarantee**. Sweeps are ``created_at``-ordered best-effort; retries and
  stream-triggered relays reorder freely.
* Poison envelopes are **parked, not dead-lettered**: after a bounded number of attempts an envelope
  becomes terminal evidence for an operator, and Benzene never auto-retries or auto-deletes it.
* :class:`InMemoryOutboxStore` is **single-process** — it is for tests and single-instance services;
  a real deployment needs a shared, durable store.
* Atomicity with your own state write comes only from ``write_mode="transactional"`` plus a stage on
  your own transaction (:class:`SqlOutboxStage`). The default ``"immediate"`` mode makes both writes
  durable, not atomic — see :class:`OutboxOptions`.

**What it deliberately is not.** It is not a database abstraction. Python has no ambient transaction
and this package does not invent one: the transactional seam is a stage you hand *your* connection
to. Benzene never opens, commits or rolls back a transaction of yours.

Mirrors .NET's ``Benzene.Outbox`` in semantics, in Python's idiom: a decorator over ``MessageSender``
rather than an outbound-route middleware, a ``ContextVar`` rather than a scoped DI service, and
stores as Protocols with injectable clocks and clients.
"""

from __future__ import annotations

from .dispatcher import OutboxDispatcher, OutboxDispatchOutcome, OutboxDispatchResult
from .envelope import OutboxEnvelope, OutboxStatus
from .memory import InMemoryOutboxStore
from .middleware import outbox_interception
from .options import IDEMPOTENCY_KEY_HEADER, OutboxOptions, OutboxWriteMode
from .sender import OutboxMessageSender, with_outbox
from .sql import (
    CREATE_TABLE_SQL,
    DEFAULT_TABLE,
    SqlOutboxStage,
    SqlOutboxStore,
    create_table_sql,
)
from .stage import (
    BufferedOutboxStage,
    OutboxCommit,
    OutboxNotStagedError,
    OutboxStage,
    bind_stage,
    current_stage,
    outbox_transaction,
    unbind_stage,
)
from .store import OutboxStore
from .worker import outbox_dispatcher_worker, run_outbox_dispatcher_loop

__all__ = [
    "BufferedOutboxStage",
    "CREATE_TABLE_SQL",
    "DEFAULT_TABLE",
    "IDEMPOTENCY_KEY_HEADER",
    "InMemoryOutboxStore",
    "OutboxCommit",
    "OutboxDispatchOutcome",
    "OutboxDispatchResult",
    "OutboxDispatcher",
    "OutboxEnvelope",
    "OutboxMessageSender",
    "OutboxNotStagedError",
    "OutboxOptions",
    "OutboxStage",
    "OutboxStatus",
    "OutboxStore",
    "OutboxWriteMode",
    "SqlOutboxStage",
    "SqlOutboxStore",
    "bind_stage",
    "create_table_sql",
    "current_stage",
    "outbox_dispatcher_worker",
    "outbox_interception",
    "outbox_transaction",
    "run_outbox_dispatcher_loop",
    "unbind_stage",
    "with_outbox",
]
