# `benzene.rabbitmq`

Host Benzene handlers on **RabbitMQ** — a self-hosted consumer and a RabbitMQ-publish outbound
client. **Distribution: `benzene-rabbitmq` (depends only on `benzene-core`).**

```bash
pip install benzene-rabbitmq              # the binding + testing helpers
pip install "benzene-rabbitmq[rabbitmq]"  # + pika for the real clients
```

## Overview

RabbitMQ is a queue transport, so the binding follows the same shape as the Kafka/SQS/SNS/Pub/Sub
bindings (transport-bindings §"RabbitMQ"):

- **Topic** — resolved from the `topic` header carried inside the delivery's AMQP
  `properties.headers` (the reserved metadata key; the cross-port convention of wire-contracts §2),
  not the physical exchange or routing key.
- **Headers** — the delivery's other AMQP application headers, UTF-8 decoded, become the Benzene
  headers.
- **Body** — the delivery body, decoded as the UTF-8 JSON body.
- **Scope** — one delivery is one pipeline invocation and one DI scope (never per batch).
- **Result** — there is **no response channel**: result mapping is acknowledge/log only. The consumer
  loop acks the delivery on a successful result (at-least-once) and nacks a failed one for
  redelivery.
- **Failure** — a malformed body or a handler fault becomes a failure *result*, never an exception out
  of the loop, so a poison delivery cannot crash the host.

The binding is duck-typed against `pika` (a delivery's `method` / `properties.headers` / `body`
triple; `channel.basic_get()` / `channel.basic_ack()` / `channel.basic_nack()`), so decode, dispatch,
and publish run in memory with fakes — no broker, no SDK. Only the default client needs the
`[rabbitmq]` extra. Mirrors .NET's `Benzene.RabbitMq`.

## `RabbitMqConsumerApp` — inbound

```python
from benzene.core import application_from, build_application
from benzene.rabbitmq import RabbitMqConsumerApp, run_consumer_loop

definition, _ = build_application(OrdersStartUp)
app = RabbitMqConsumerApp(application_from(definition))

await app.handle_message(method, properties, body)     # one delivery -> one invocation; returns Result
await run_consumer_loop(app, channel, queue="orders")  # the self-hosted worker: pull -> dispatch -> ack
```

- `RabbitMqConsumerApp(application)` wraps a `BenzeneMessageApplication`; `handle_message(method,
  properties, body)` decodes the pika delivery triple, runs the pipeline, and returns the mapped
  `Result` (for the loop to act on). `RabbitMqConsumerApp.from_definition(definition)` is the one-line
  wiring from a composition root's `AppDefinition`.
- `run_consumer_loop(app, channel, *, queue, should_continue=..., ack=True, requeue=True,
  idle_sleep=1.0, on_result=None)` drives a duck-typed channel: `basic_get(queue)` returns a `(method, properties,
  body)` triple or `(None, None, None)` when the queue is empty (pika's callback model flattened to a
  poll so the loop mirrors the Kafka binding). With `ack=True` (the default, at-least-once) a
  **successful** result acks the delivery (`basic_ack(delivery_tag=...)`). A failed one is nacked,
  but only a *retryable* status (`service-unavailable`, `timeout`, `too-many-requests`) is requeued;
  a final failure such as `bad-request` or `not-found` is nacked with `requeue=False` so it leaves
  the queue instead of spinning at the head of it forever. Bind a dead-letter exchange
  (`x-dead-letter-exchange`) to the queue to keep those deliveries. `idle_sleep` is how long the loop
  waits when the queue is empty, so an idle worker does not busy-poll the broker. Pass `ack=False` for manual acknowledgement and act from `on_result(method,
  result)`. `should_continue` bounds the loop (a real worker loops forever; a test stops after N
  pulls).
- `decode_rabbitmq_message(method, properties, body)` is the pure decode step (delivery →
  `{topic, headers, body}`), exposed for custom loops.

## `RabbitMqMessageSender` — outbound

```python
from benzene.rabbitmq import RabbitMqMessageSender

sender = RabbitMqMessageSender("orders-events", host="localhost")
await sender.send_message("orders:created", order, headers={"x-correlation-id": "abc"})
```

Implements the `benzene.core.MessageSender` port over a RabbitMQ channel: it serializes the message to
the JSON body, forwards the header dictionary onto the AMQP `properties.headers` (so correlation/trace
propagation rides across the hop), and carries the Benzene topic in the `topic` header (`TOPIC_HEADER`)
— the same convention the consumer reads. All Benzene topics are published to the one configured
`exchange` / `routing_key`, header-routed. A publish failure maps to `service-unavailable`. Inject a
`channel` (any object exposing `basic_publish(exchange, routing_key, body, properties)`) for tests;
otherwise a `pika` blocking connection is opened lazily from `host` (default `localhost`) on first use.

## Testing

`benzene.rabbitmq.testing` provides a native-delivery builder and an in-memory test host, and the
shared harness specializes to RabbitMQ in one call:

```python
from benzene.testing import create_test_host

host = create_test_host(OrdersStartUp).with_services(overrides).build_rabbitmq()
result = await host.send_rabbitmq("orders:place", body={"sku": "ABC", "quantity": 2})
```

`RecordingRabbitMqChannel` (a replay channel that records `acked` / `nacked` delivery tags and
`published` messages) lets a test assert the loop's at-least-once behaviour — a failed delivery is
nacked, not acked — without a broker. See the runnable
[`examples/rabbitmq_orders/`](https://github.com/daniellepelley/benzene-python/tree/main/examples/rabbitmq_orders).

## Exports

`RabbitMqConsumerApp`, `RabbitMqMessageSender`, `TOPIC_HEADER`, `decode_rabbitmq_message`,
`run_consumer_loop`; and from `benzene.rabbitmq.testing`: `RabbitMqTestHost`, `RabbitMqMessageBuilder`,
`FakeRabbitMqMessage`, `FakeRabbitMqMethod`, `FakeRabbitMqProperties`, `RecordingRabbitMqChannel`.

## See also

- [Transport bindings](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md)
  — the language-neutral binding contract this implements.
- [`benzene.kafka`](kafka.md) — the sibling queue/stream binding this mirrors.
- [`benzene.core`](core.md) — the registry, pipeline, and `MessageSender` port this builds on.
</content>
</invoke>
