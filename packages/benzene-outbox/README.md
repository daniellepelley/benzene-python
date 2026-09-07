# benzene-outbox

The **transactional outbox** for [Benzene Python](https://github.com/daniellepelley/benzene-python):
record a send next to your state write, forward it afterwards, and stop losing messages to the gap
between the two. Depends only on `benzene-core`.

```bash
pip install benzene-outbox          # in-memory store + the whole engine
pip install benzene-outbox[sql]     # + SQLAlchemy, for the relational store
```

## The problem it removes

A handler that writes state and then sends a message does two independent things. If the send fails
after the write commits, the message is gone and nothing records that it was owed:

```python
async def place_order(request):
    await orders.save(request)                     # committed
    await sender.send_message("orders:placed", …)  # broker down → lost, forever
    return Result.ok()
```

With the outbox in front of the sender, the same handler stages the send durably and answers
`accepted`; a dispatcher forwards it once the broker is back.

```python
from benzene.outbox import InMemoryOutboxStore, OutboxDispatcher, with_outbox

store = InMemoryOutboxStore()          # or SqlOutboxStore(engine)
sender = with_outbox(sqs_sender, store)   # a MessageSender; the call site is unchanged

# elsewhere — in this process's worker, or a scheduled sweep
await OutboxDispatcher(store, sqs_sender).run_once()
```

Stamping decorators go **outside** the outbox so what they stamp is captured
(`with_correlation_id(with_outbox(sqs_sender, store))`); retry and circuit breakers go on the sender
the **dispatcher** holds, never on the capture chain.

## Atomic with your own write

`write_mode="transactional"` stages the envelope instead of writing it, and *your* commit persists
it. `SqlOutboxStage` holds the connection you hand it and issues one `INSERT` — it never opens,
commits or rolls back a transaction, and knows nothing about the rest of your schema:

```python
async with connection.begin():                       # your transaction
    await connection.execute(insert_order)           # your state write
    async with outbox_transaction(stage=SqlOutboxStage(connection)):
        await sender.send_message("orders:placed", order)   # same transaction
```

## What it guarantees, exactly

It converts a **lost send** into a **delayed, at-least-once send**. It does *not* make the send
exactly-once:

- duplicates happen (a crash between the broker accepting and the store being told), so capture
  stamps the envelope id as `idempotency-key` — pair it with `benzene-resilience`'s
  `idempotency_interception` at the consumer and the effect happens once;
- there is **no ordering guarantee**;
- poison envelopes are **parked** after a bounded number of attempts — terminal evidence for an
  operator, never auto-retried, never auto-deleted, and never blocking the envelopes behind them;
- `InMemoryOutboxStore` is **single-process**;
- the default `"immediate"` write mode makes both writes durable, not atomic.

It is not a database abstraction: Python has no ambient transaction and this package does not invent
one. Full reference: [`docs/reference/outbox.md`](../../docs/reference/outbox.md).
