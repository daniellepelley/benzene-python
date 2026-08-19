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
  loop commits the offset on a successful result (at-least-once) and leaves a failed record for
  redelivery.
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
- `run_consumer_loop(app, consumer, *, poll_timeout=1.0, should_continue=..., commit=True, on_result=None)`
  drives a duck-typed consumer (`poll(timeout)` → a record or `None`; `commit(message=...)`). A record
  carrying a broker error (`record.error()`) is skipped; with `commit=True` the offset is committed
  only after a successful result. `should_continue` bounds the loop (a real worker loops forever).
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

`RecordingKafkaConsumer` (a replay consumer that records committed offsets) lets a test assert the
loop's at-least-once behaviour without a broker. See the runnable [`examples/kafka_orders/`](https://github.com/daniellepelley/benzene-python/tree/main/examples/kafka_orders).

## Exports

`KafkaConsumerApp`, `KafkaMessageSender`, `KafkaMessage`, `TOPIC_HEADER`, `build_kafka_consumer`,
`decode_kafka_message`, `kafka_consumer_worker`, `run_consumer_loop`; and from `benzene.kafka.testing`: `KafkaTestHost`, `KafkaMessageBuilder`,
`FakeKafkaMessage`, `RecordingKafkaConsumer`.

## See also

- [Transport bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the language-neutral binding contract this implements.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` port this builds on.
