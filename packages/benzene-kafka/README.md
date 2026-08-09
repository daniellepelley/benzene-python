# benzene-kafka

Host [Benzene Python](https://github.com/daniellepelley/benzene-python) handlers on **Apache Kafka** —
a self-hosted consumer and a Kafka-produce outbound client — the same handlers, no rewrite. Depends
only on `benzene-core`.

```bash
pip install benzene-kafka             # the binding + testing helpers
pip install "benzene-kafka[kafka]"    # + confluent-kafka for the real clients
```

```python
from benzene.core import application_from, build_application
from benzene.kafka import KafkaConsumerApp, KafkaMessageSender, run_consumer_loop

# Inbound: one record -> one pipeline invocation -> one scope; ack/log, no reply.
definition, _ = build_application(OrdersStartUp)
app = KafkaConsumerApp(application_from(definition))
await run_consumer_loop(app, consumer)          # consumer: a confluent_kafka.Consumer

# Outbound: publish to a Kafka topic, Benzene topic carried in the `topic` header.
sender = KafkaMessageSender("orders-events", bootstrap_servers="localhost:9092")
await sender.send_message("orders:created", order, headers={"x-correlation-id": "abc"})
```

- **Consumer** — the Benzene topic comes from the record's `topic` header (the cross-port convention);
  the other Kafka headers are the Benzene headers (UTF-8), the value is the JSON body. One record is
  one scope; there is **no response channel**, so the result is acknowledge/log only — `run_consumer_loop`
  commits the offset on success (at-least-once) and leaves a failed record for redelivery. A poison
  record can never crash the loop.
- **Outbound** — `KafkaMessageSender` implements the `benzene.core.MessageSender` port over
  `confluent-kafka` (optional extra), forwarding the header dictionary onto the record's Kafka headers
  so correlation/trace propagation rides across the hop.

The binding is duck-typed against `confluent-kafka`, so decode, dispatch, and send are exercised in
memory with fakes — no broker, no SDK. Test through `benzene.kafka.testing` (a native-record builder +
a test host) or the shared harness (`create_test_host(StartUp).build_kafka()` + `send_kafka`) — see the
runnable `examples/kafka_orders/`. Mirrors .NET's `Benzene.Kafka.Core`, and contributes the
`benzene.kafka` subpackage to the shared `benzene` namespace.
