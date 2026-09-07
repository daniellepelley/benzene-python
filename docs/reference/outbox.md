# `benzene.outbox`

The **transactional outbox**: record a send as data alongside your state write, and forward it
afterwards, so the two facts cannot disagree. **Distribution: `benzene-outbox` (depends only on
`benzene-core`).**

```bash
pip install benzene-outbox          # the whole engine + the in-memory store
pip install "benzene-outbox[sql]"   # + SQLAlchemy, for the relational store
```

## Overview

A handler that writes state and then sends a message performs two independent operations:

```python
async def place_order(request):
    await orders.save(request)                        # committed
    await sender.send_message("orders:placed", ...)   # broker down → lost, silently
    return Result.ok()
```

If the send fails after the write commits, the message is gone and nothing anywhere records that it
was owed. Sending first is no better — then the message describes state that was never written. This
is the dual-write problem, and no ordering of two independent writes solves it. The answer is a third
thing: **write the intended send as data** (where you can make it durable, or even atomic, with your
state write), and have something forward it afterwards, retrying until the broker accepts.

That is what this package is. It has three parts:

| Part | What it is | Where it lives |
|---|---|---|
| **Capture** | `with_outbox(sender, store)` — a decorator over the one `MessageSender` seam. Records the send and answers `accepted`; never performs it. | `sender.py` |
| **Staging** | `SqlOutboxStage` (your connection, your transaction) or `outbox_transaction` / `outbox_interception` (a buffer drained by your commit). | `stage.py`, `middleware.py` |
| **Dispatch** | `OutboxDispatcher` — claims due envelopes, sends them for real, retries with durable backoff, parks what can never succeed. | `dispatcher.py`, `worker.py` |

Mirrors .NET's `Benzene.Outbox` in semantics, in Python's idiom: capture is a `MessageSender`
decorator rather than an outbound-route middleware (Python's outbound side is decorators — `with_retry`,
`with_correlation_id`), the staging scope is a `ContextVar` rather than a scoped DI service, and
stores are Protocols with injectable clocks and clients.

## What it guarantees — and what it does not

The outbox converts **"lost send"** into **"delayed, at-least-once send"**. Read the rest of this
section before adopting it; every line is a real constraint, not a caveat for the footnotes.

- **Not exactly-once.** There is a window between the broker accepting a message and the store being
  told about it. A crash inside that window re-forwards the envelope. Capture therefore stamps the
  envelope id into the `idempotency-key` header, so pairing this with
  [`benzene.resilience`](resilience.md)'s `idempotency_interception` at the consumer makes the
  *effect* happen once. Deduplication is the far end's job, by design.
- **No ordering guarantee.** Sweeps claim in `created_at` order best-effort only; a retried envelope
  moves behind newer ones, and a change-stream relay has no order at all. If your consumer needs
  ordering, it must establish it from the payload.
- **Poison envelopes are parked, not dead-lettered.** After a bounded number of attempts an envelope
  becomes `parked`: terminal, never auto-retried, never auto-deleted. It is the operator's evidence.
  There is no dead-letter forwarding here — parking is the deliberate boundary.
- **`InMemoryOutboxStore` is single-process.** Envelopes captured in one process are dispatched only
  by that process and die with it. It is for tests and single-instance services.
- **The default write mode is durable, not atomic.** `"immediate"` writes the envelope to the store
  at capture: both writes are now durable, but they are still two writes. Only
  `"transactional"` plus a stage on your own transaction makes them one.
- **An outboxed send is fire-and-forget.** The caller is told `accepted` — never the downstream's
  real answer, which does not exist yet. Request/response topics must not be outboxed.
- **It is not a database abstraction.** Python has no ambient transaction and this package does not
  invent one; see [the capability matrix](../capability-matrix.md)'s standing "no" to state-store
  abstractions. The transactional seam takes *your* connection and issues one `INSERT` on it.

## Capture

```python
from benzene.outbox import InMemoryOutboxStore, with_outbox

store = InMemoryOutboxStore()            # or SqlOutboxStore(engine)
sender = with_outbox(sqs_sender, store)  # is a MessageSender

await sender.send_message("orders:placed", order)   # → Result.accepted()
```

Capture builds an `OutboxEnvelope` — id, topic, the body encoded with `benzene.core.encode_body`
(byte-identical to an inline send), and a lower-cased snapshot of the headers as they stood — and
either writes it to the store (`"immediate"`) or hands it to a stage (`"transactional"`). It **never
calls the inner sender**: that sender is held only so the dispatcher can be wired from the same
object (`OutboxDispatcher(store, sender.inner)`). A store failure propagates, so a caller learns the
send was not recorded, exactly as they would learn a transport refused it.

### Composition order is load-bearing

```python
sender = with_correlation_id(with_outbox(sqs_sender, store))   # correct
sender = with_outbox(with_correlation_id(sqs_sender), store)   # wrong — never runs
```

Header-stamping decorators go **outside** the outbox, so what they stamp at business time is what the
envelope captures and what the relay replays. Transport guards go on the sender the **dispatcher**
holds — `OutboxDispatcher(store, with_retry(sqs_sender))` — never on the capture chain: capture writes
to a database, and the dispatcher already retries with durable, cross-process backoff.

### The idempotency-key handshake

Unless a caller supplied one, capture stamps the envelope's own id into `idempotency-key` (the exact
header .NET's `OutboxDefaults.IdempotencyKeyHeaderName` and `benzene.resilience`'s
`DEFAULT_KEY_HEADERS[0]` use). Every redelivery of that envelope carries the same value, so an
outbox producer and an idempotent consumer click together with no configuration. Disable it with
`OutboxOptions(stamp_idempotency_key=False)` if you key on something of your own.

## Staging: atomic with your own state write

`write_mode="transactional"` makes capture hand the envelope to a stage instead of writing it. The
honest shape — and the only one that is genuinely atomic — is a stage that holds **your** connection:

```python
from benzene.outbox import OutboxOptions, SqlOutboxStage, outbox_transaction, with_outbox

sender = with_outbox(sqs_sender, store, options=OutboxOptions(write_mode="transactional"))

async with connection.begin():                        # your transaction, your connection
    await connection.execute(insert_order)            # your state write
    async with outbox_transaction(stage=SqlOutboxStage(connection)):
        await sender.send_message("orders:placed", order)
# your commit makes the order and the recorded send real, together; your rollback discards both
```

`SqlOutboxStage` issues one `INSERT` and nothing else — it never begins, commits or rolls back a
transaction, and has no opinion about the rest of your schema. The only thing your data and the
outbox share is the connection you passed in.

The buffered alternative, when the atomic commit is something else you own:

```python
async with outbox_transaction(commit=my_unit_of_work.commit_with):
    await sender.send_message("orders:placed", order)   # buffered
# clean exit → commit(envelopes) once; an exception → the buffer is discarded
```

A stage that is dropped with envelopes never drained **logs a warning** (`benzene.outbox`): staging
with no committer means the write never happens, which is a misconfiguration, not a smaller
guarantee. A transactional capture with no stage in scope raises `OutboxNotStagedError` naming the
fix, rather than dropping the send silently.

### The middleware

```python
from benzene.outbox import outbox_interception

definition.middleware += [outbox_interception(store)]   # ahead of the message router
```

The pipeline already has a unit-of-work boundary — one invocation — so the middleware scopes a stage
to it: sends the handler makes are buffered, and committed in one call **only if the invocation
succeeded**. A failed or raising handler discards them, and if your state write shared the same
transaction, nothing was written either. Pass `commit=` to route that commit through your own
transaction; the default `store.add` still leaves you with two durable writes rather than one atomic
one.

## The store port

```python
class OutboxStore(Protocol):
    async def add(self, envelopes: Sequence[OutboxEnvelope]) -> None: ...
    async def claim_due(self, batch_size: int, lease: float) -> list[OutboxEnvelope]: ...
    async def claim(self, envelope_id: str, lease: float) -> OutboxEnvelope | None: ...
    async def get(self, envelope_id: str) -> OutboxEnvelope | None: ...
    async def mark_dispatched(self, envelope_id: str) -> None: ...
    async def reschedule(self, envelope_id: str, attempt_count: int, delay: float, error: str) -> None: ...
    async def park(self, envelope_id: str, error: str) -> None: ...
    async def delete_dispatched_before(self, cutoff: float) -> int: ...
```

The state machine is three states, two of them terminal:

```
pending ──(sent, and the store told)──▶ dispatched ──(retention)──▶ deleted
   │
   └──(attempt budget spent)──────────▶ parked      (terminal; a human decides)
```

`pending` is the only claimable state, which is what stops a dispatched envelope being re-sent by a
later sweep and a parked one from being picked up forever.

**The hard requirement: `claim` and `claim_due` must be atomic per envelope.** A claim is a
conditional transition — *pending, due, and not currently leased → lease it to me* — evaluated and
applied indivisibly: one SQL `UPDATE ... WHERE` whose `rowcount` **is** the decision, one DynamoDB
conditional `UpdateItem`, or in memory a read and a write with no `await` between them (the same
argument `InMemoryIdempotencyStore.put_if_absent` documents). Everything below rests on it.

Notice there is no unclaimed "list the pending ones" method. An unreserved list invites two
dispatchers to read the same envelope and both forward it, which is the one failure a store can
actually prevent; `get` reads one envelope without claiming it, for operators looking at parked
evidence and for tests — never as a dispatch path.

### Why that makes dispatch safe

A claim is a **lease**, not a lock: it lasts `claim_lease` seconds and then lapses. So:

- while a lease is live, no other claimer can have that envelope — no concurrent double-send;
- when a dispatcher dies mid-forward, the lease lapses and the envelope returns to the pending pool —
  nothing is lost;
- the crash window between "the broker accepted" and `mark_dispatched` re-sends the envelope — and
  that duplicate is not hidden, it carries the same `idempotency-key` as the first copy.

Lifecycle calls for an envelope that no longer exists are a no-op, never an error: retention may have
removed it between the claim and the report.

## Stores

**`InMemoryOutboxStore(clock=time.time)`** — a dict, always available, atomic by construction on the
event loop, and single-process. For tests, local development and single-instance services.

**`SqlOutboxStore(engine, table="benzene_outbox", clock=time.time)`** — the durable store, and the
natural home of "the same transaction as my state write". `engine` is anything with SQLAlchemy 2.x's
async shape (`async with engine.begin() as conn`, `await conn.execute(statement, parameters)`), so
asyncpg, psycopg, aiosqlite and aiomysql all work and this package depends on none of them. Build one
from a URL with `SqlOutboxStore.from_url(...)` — that path imports SQLAlchemy lazily and raises an
`ImportError` naming the `benzene-outbox[sql]` extra if it is absent.

The claim is a single conditional `UPDATE`, so the database is the arbiter and no
optimistic-concurrency retry loop sits on top:

```sql
UPDATE benzene_outbox SET lease_until = :lease
 WHERE id = :id AND status = 'pending'
   AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
   AND (lease_until     IS NULL OR lease_until     <= :now)
```

`delete_dispatched_before` deletes only `dispatched` rows past retention; parked rows are never in
its scope. The DDL is exported as `CREATE_TABLE_SQL` (or `create_table_sql(table)`) **for your own
migration tool** — the store never creates or migrates anything.

## Dispatch

```python
from benzene.outbox import OutboxDispatcher, outbox_dispatcher_worker

dispatcher = OutboxDispatcher(store, sqs_sender)          # the REAL transport, never the capture one

result = await dispatcher.run_once()   # OutboxDispatchResult(dispatched, rescheduled, parked, deleted)
outcome = await dispatcher.dispatch_one(envelope_id)      # the change-stream relay path
```

`run_once` claims a batch of due envelopes, forwards each **independently**, then retires dispatched
envelopes past retention. Independence is what stops head-of-line blocking: a failing envelope's own
backoff pushes it behind the others instead of holding the batch up.

A send that raises *or* returns an unsuccessful `Result` is a failed attempt — a deliberate
divergence from .NET, whose sender only ever throws; Python senders answer with a `Result`, so the
dispatcher checks `is_successful` as well as catching.

### The poison bound

```python
OutboxOptions(max_attempts=10, backoff_base=30.0, backoff_cap=3600.0, retry_on=DEFAULT_RETRYABLE)
```

`budget_for(status)` decides how many attempts an envelope gets, and it is the same rule the Kafka
consumer's dead-letter bound applies for the same reason: a failure **outside** `retry_on`
(`bad-request`, `not-found` — the broker refused it on the merits) will be refused identically every
time, so it parks on its **first** failure rather than burning ten attempts and hours of backoff
before the operator sees it. A retryable failure, or a raised exception (a transport/infrastructure
fault), gets the full `max_attempts`, with `min(backoff_base * 2 ** (attempt - 1), backoff_cap)`
between attempts. Either way the budget is finite: a staged message that can never be sent ends up
`parked` with its last error recorded, and the queue moves on.

### Relay hosts

```python
from benzene.core import WorkerHost
from benzene.outbox import outbox_dispatcher_worker, run_outbox_dispatcher_loop

host = WorkerHost()
host.add("http", http_worker)
host.add("outbox", outbox_dispatcher_worker(dispatcher))
await host.run()
```

`run_outbox_dispatcher_loop(dispatcher, should_continue=..., poll_interval=5.0, sleep=asyncio.sleep)`
is the loop, in the same shape as the SQS and Kafka consumer loops; `outbox_dispatcher_worker` adapts
it to `WorkerHost`, so the relay winds down with the rest of the process on SIGTERM. **A run that
raises is logged and survived** — the envelopes are durable, so the next poll picks up where this one
stopped.

On Lambda there is no background thread, so the loop does not apply. The pattern there is a
change-stream relay (DynamoDB Streams) calling `dispatch_one` per inserted envelope **plus** a
low-frequency scheduled sweep calling `run_once`: a stream fires once per insert and cannot drive
retries, parking or retention on its own.

## Testing

Everything takes an injectable clock, and every seam is duck-typed, so the whole surface is exercised
in memory: `tests/test_outbox.py` drives leases, backoff, parking and retention on a manual clock
with no broker, no sleeping and no third-party package — including the SQL store, against stdlib
`sqlite3` through a thirty-line adapter presenting SQLAlchemy's shape. That the durable store is
testable that way is the point: it abstracts nothing about your database.
