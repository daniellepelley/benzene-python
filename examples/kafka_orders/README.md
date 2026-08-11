# `kafka_orders` — the order domain on Apache Kafka

Mounts the shared [`orders_domain`](../orders_domain) on the **Kafka** binding: a self-hosted consumer
that turns each record into one pipeline invocation, and a `KafkaMessageSender` that publishes
`orders:created` downstream. The same handlers as every other host — only [`host.py`](host.py) is
Kafka-specific.

Unlike the cloud hosts (triggered by a runtime), a Kafka service owns its own loop:
`run_consumer_loop` subscribes a consumer, dispatches one record at a time, and commits the offset on
success (at-least-once). There is no HTTP surface — the domain is reached over records whose `topic`
header names the Benzene topic.

**Worth using even if Kafka is the only transport this service ever has.** Unlike HTTP, where a real
ASGI framework already gives you routing and cross-cutting middleware for free (see
[Why not just a minimal ASGI app?](../../docs/getting-started.md#why-not-just-a-minimal-asgi-app)),
`confluent-kafka`'s `Consumer.poll()` on its own hands you a raw record and stops — deserializing the
payload, dispatching on whatever identifies its type, and every cross-cutting concern (validation,
correlation, retries, structured logging) is code you'd otherwise write yourself, usually inline in
the consume loop. `KafkaConsumerApp` + the middleware pipeline is that missing layer, for Kafka
specifically.

## Run it against a broker

```bash
pip install "benzene-kafka[kafka]"
export BENZENE_KAFKA_BOOTSTRAP=localhost:9092
export BENZENE_KAFKA_CONSUME_TOPIC=orders-in     # the physical Kafka topic to consume
export BENZENE_KAFKA_TOPIC=orders-events         # where orders:created is produced
python -m kafka_orders.host
```

## Test it (no broker)

The tests dogfood the real binding through the shared harness — the same one-specialization-step setup
as the AWS/GCP/Azure suites, only `.build_kafka()` differs:

```python
host = create_test_host(OrdersStartUp).with_services(overrides).build_kafka()
result = await host.send_kafka("orders:place", body={"sku": "ABC", "quantity": 2})
assert result.status == "created"
assert fake.last_topic == "orders:created"
```

```bash
pytest examples/kafka_orders
```
