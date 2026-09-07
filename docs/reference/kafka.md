# `benzene.kafka`

Host Benzene handlers on **Apache Kafka** — a self-hosted consumer and a Kafka-produce outbound
client. **Distribution: `benzene-kafka` (depends only on `benzene-core`).**

```bash
pip install benzene-kafka             # the binding + testing helpers
pip install "benzene-kafka[kafka]"    # + confluent-kafka for the real clients
```

## Overview

Kafka is a queue/stream transport, so the binding follows the same shape as the SQS/SNS/Pub/Sub
bindings (transport-bindings §"Kafka"):

- **Topic** — resolved from the record's `topic` header (the reserved metadata key; the cross-port
  convention of wire-contracts §2), not the physical Kafka topic name.
- **Headers** — the record's other Kafka headers, UTF-8 decoded, become the Benzene headers.
- **Body** — the record value, decoded as the UTF-8 JSON body.
- **Scope** — one record is one pipeline invocation and one DI scope (never per batch).
- **Result** — there is **no response channel**: result mapping is acknowledge/log only. The consumer
  loop commits the offset on a successful result (at-least-once). Because a Kafka commit is a
  watermark rather than a per-message ack, a failed record additionally **blocks** its partition: the
  loop seeks back to it and commits nothing beyond it until it succeeds, so a later success on the
  same partition cannot bury it. `DeadLetterOptions` bounds that block so a record that can never
  succeed does not wedge the partition either.
- **Failure** — a malformed body or a handler fault becomes a failure *result*, never an exception out
  of the loop, so a poison record cannot crash the host.

The binding is duck-typed against `confluent-kafka`, so decode, dispatch, and send run in memory with
fakes — no broker, no SDK. Only the default consumer/producer clients need the `[kafka]` extra.

## `KafkaConsumerApp` — inbound

```python
from benzene.core import application_from, build_application
from benzene.kafka import KafkaConsumerApp, run_consumer_loop

definition, _ = build_application(OrdersStartUp)
app = KafkaConsumerApp(application_from(definition))

await app.handle_message(record)          # one record -> one invocation; returns the mapped Result
await run_consumer_loop(app, consumer)    # the self-hosted worker: poll -> dispatch -> commit
```

- `KafkaConsumerApp(application)` wraps a `BenzeneMessageApplication`; `handle_message(record)` decodes
  the record, runs the pipeline, and returns the mapped `Result` (for the loop to act on).
- `run_consumer_loop(app, consumer, *, poll_timeout=1.0, should_continue=..., commit=True, on_result=None, dead_letter=None)`
  drives a duck-typed consumer (`poll(timeout)` → a record or `None`; `commit(message=...)`). A record
  carrying a broker error (`record.error()`) is skipped; with `commit=True` the offset is committed
  only after a successful result, and never past an uncommitted failure on the same partition (the
  loop seeks back to it — scoping is per `(topic, partition)`, so one partition never stalls
  another). A record that always fails is therefore re-served until `dead_letter` retires it (below).
  `should_continue` bounds the loop (a real worker loops forever).
- `build_kafka_consumer(*, bootstrap_servers, group_id, topics, auto_offset_reset="earliest", **config)`
  constructs and subscribes a real `confluent_kafka.Consumer` configured to match the loop above —
  most importantly `enable.auto.commit=False`, which is what makes the loop's at-least-once rule true.
  Any `**config` (dotted `confluent-kafka` keys) wins over the defaults. Needs the `[kafka]` extra.
- `kafka_consumer_worker(app, consumer, *, close=True, **loop_options)` returns a
  [`benzene.core.WorkerHost`](core.md#workerhost--running-n-transports-in-one-process) leg wrapping the
  loop above, for a process that also serves HTTP. It is a closure over
  `run_consumer_loop(..., should_continue=stop.should_continue)` plus the `finally: consumer.close()`
  — reach for it only when there is more than one transport.
- `decode_kafka_message(record)` is the pure decode step (record → `{topic, headers, body}`), exposed
  for custom loops.

### `DeadLetterOptions` — the bound on the block

Blocking a partition is the right answer to a *transient* failure and the wrong one to a permanent
one: a malformed body or a permanently-unknown topic fails identically on every redelivery, so an
unbounded block trades silent data loss for a partition that never advances again. Pass
`dead_letter=` to give the block an exit (the shape mirrors .NET's `KafkaDeadLetterOptions`):

```python
from confluent_kafka import Producer
from benzene.kafka import DeadLetterOptions, run_consumer_loop

await run_consumer_loop(app, consumer, dead_letter=DeadLetterOptions(
    topic="orders.DLT",                                  # where poison records go
    producer=Producer({"bootstrap.servers": "..."}),      # you build and own it (auth included)
    max_attempts=3,                                       # deliveries of one record before routing
))
```

| Field | Default | Meaning |
|---|---|---|
| `topic` | — | the dead-letter topic the original record is re-produced to |
| `producer` | — | any object with `produce(topic, *, value, key, headers, on_delivery)` + `flush(timeout)` — a real `confluent_kafka.Producer` needs no wrapper, and a non-Kafka destination is a few lines |
| `max_attempts` | `3` | deliveries of one `(topic, partition, offset)` before it is dead-lettered (floored at 1) |
| `retry_on` | `DEFAULT_RETRYABLE` | statuses worth re-serving at all; a failure outside it is dead-lettered on its **first** failure |
| `flush_timeout` | `10.0` | seconds to wait for the broker to acknowledge the dead-letter produce |

- **The record round-trips unmodified** so it can be replayed: the original key, value and headers
  are re-produced verbatim, plus four diagnostics — `x-dlt-reason` (the Benzene *status* string only:
  never an exception message or any part of the payload), `x-dlt-original-topic`,
  `x-dlt-original-partition`, `x-dlt-original-offset`. The header names are exported
  (`DLT_REASON_HEADER`, …) and match .NET byte for byte.
- **Then the partition advances**: the block is released and the loop commits past the record.
- **A final status short-circuits the budget.** `bad-request`/`not-found` cannot become good, so they
  are routed on the first failure without a seek — the same line the
  [RabbitMQ consumer](rabbitmq.md) draws between requeue and drop-to-DLX.
- **Attempt state cannot leak**: the count lives on the per-`(topic, partition)` block entry, which
  is created on the first failure and deleted when the record succeeds or is dead-lettered.
- **A dead-letter that never lands is not a routed record.** If the produce raises, reports a
  delivery error, or is still in flight when `flush_timeout` expires, the loop logs at `error`,
  leaves the offset **uncommitted** and returns, so the record is redelivered on restart rather than
  buried by the next commit — availability traded for no-loss, matching .NET.
- **Omitting `dead_letter` keeps the unbounded block.** With no seam configured there is nowhere to
  put the record, and dropping it would be the silent loss the watermark rule exists to prevent.

## `KafkaMessageSender` — outbound

```python
from benzene.kafka import KafkaMessageSender

sender = KafkaMessageSender("orders-events", bootstrap_servers="localhost:9092")
await sender.send_message("orders:created", order, headers={"x-correlation-id": "abc"})
```

Implements the `benzene.core.MessageSender` port over a Kafka producer: it serializes the message to
the JSON body, forwards the header dictionary onto the record's Kafka headers (so correlation/trace
propagation rides across the hop), and carries the Benzene topic in the `topic` header. All Benzene
topics are produced to the one configured Kafka topic, header-routed. A produce/flush failure (or a
delivery error reported to the callback) maps to `service-unavailable`. Inject a `producer` for tests;
otherwise a `confluent_kafka.Producer` is created lazily from `bootstrap_servers`.

## Testing

`benzene.kafka.testing` provides a native-record builder and an in-memory test host, and the shared
harness specializes to Kafka in one call:

```python
from benzene.testing import create_test_host

host = create_test_host(OrdersStartUp).with_services(overrides).build_kafka()
result = await host.send_kafka("orders:place", body={"sku": "ABC", "quantity": 2})
```

`RecordingKafkaConsumer` (a replay consumer that records committed offsets and seeks) and
`RecordingKafkaProducer` (a dead-letter producer that records what was routed, and can fail the three
ways a real one can) let a test assert the loop's at-least-once behaviour — and its dead-letter bound
— without a broker. See the runnable [`examples/kafka_orders/`](https://github.com/daniellepelley/benzene-python/tree/main/examples/kafka_orders).

## Exports

`KafkaConsumerApp`, `KafkaMessageSender`, `KafkaMessage`, `DeadLetterOptions`, `DeadLetterProducer`,
`TOPIC_HEADER`, `DLT_REASON_HEADER`, `DLT_ORIGINAL_TOPIC_HEADER`, `DLT_ORIGINAL_PARTITION_HEADER`,
`DLT_ORIGINAL_OFFSET_HEADER`, `build_kafka_consumer`, `decode_kafka_message`, `kafka_consumer_worker`,
`run_consumer_loop`; and from `benzene.kafka.testing`: `KafkaTestHost`, `KafkaMessageBuilder`,
`FakeKafkaMessage`, `RecordingKafkaConsumer`, `RecordingKafkaProducer`.

## See also

- [Transport bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the language-neutral binding contract this implements.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` port this builds on.
