# benzene-rabbitmq

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **RabbitMQ** —
a self-hosted consumer and a RabbitMQ-publish outbound client — the same handlers, no rewrite. Depends
only on `benzene-core`.

```bash
pip install benzene-rabbitmq               # the binding + testing helpers
pip install "benzene-rabbitmq[rabbitmq]"   # + pika for the real clients
```

```python
from benzene.core import application_from, build_application
from benzene.rabbitmq import RabbitMqConsumerApp, RabbitMqMessageSender, run_consumer_loop

# Inbound: one delivery -> one pipeline invocation -> one scope; ack/log, no reply.
definition, _ = build_application(OrdersStartUp)
app = RabbitMqConsumerApp(application_from(definition))
await run_consumer_loop(app, channel, queue="orders")  # channel: a pika channel

# Outbound: publish to an exchange, Benzene topic carried in the `topic` header.
sender = RabbitMqMessageSender("orders-events", host="localhost")
await sender.send_message("orders:created", order, headers={"x-correlation-id": "abc"})
```

- **Consumer** — the Benzene topic comes from the `topic` header carried in the delivery's AMQP
  `properties.headers` (the cross-port convention); the other headers are the Benzene headers (UTF-8),
  the body is the JSON body. One delivery is one scope; there is **no response channel**, so the result
  is acknowledge/log only — `run_consumer_loop(app, channel, queue=...)` (the queue name is required)
  acks the delivery on success (at-least-once), requeues a **transient** failure
  (`service-unavailable`/`timeout`/`too-many-requests`) for redelivery, and nacks any other failure with
  `requeue=False` so a poison delivery leaves the queue instead of spinning the loop — declare the queue
  with an `x-dead-letter-exchange` and the broker routes those drops there. An empty queue backs off for
  `idle_sleep` seconds (default 1.0) instead of busy-polling. A poison delivery can never crash the loop.
- **Outbound** — `RabbitMqMessageSender` implements the `benzene.core.MessageSender` port over
  `pika` (optional extra), forwarding the header dictionary onto the AMQP `properties.headers` so
  correlation/trace propagation rides across the hop. Connection *and* publish run on a worker thread
  under one lock, because a pika channel is not thread-safe; a missing `pika` raises a teaching
  `ImportError` naming `pip install "benzene-rabbitmq[rabbitmq]"` rather than failing every publish.

The binding is duck-typed against `pika`, so decode, dispatch, and publish are exercised in memory with
fakes — no broker, no SDK. Test through `benzene.rabbitmq.testing` (a native-delivery builder + a test
host) or the shared harness (`create_test_host(StartUp).build_rabbitmq()` + `send_rabbitmq`) — see the
runnable `examples/rabbitmq_orders/`. Mirrors .NET's `Benzene.RabbitMq`, and contributes the
`benzene.rabbitmq` subpackage to the shared `benzene` namespace.
