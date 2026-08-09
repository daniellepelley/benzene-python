# `kafka_orders` — the order domain on Apache Kafka

Mounts the shared [`orders_domain`](../orders_domain) on the **Kafka** binding: a self-hosted consumer
that turns each record into one pipeline invocation, and a `KafkaMessageSender` that publishes
`orders:created` downstream. The same handlers as every other host — only [`host.py`](host.py) is
Kafka-specific.

Unlike the cloud hosts (triggered by a runtime), a Kafka service owns its own loop:
`run_consumer_loop` subscribes a consumer, dispatches one record at a time, and commits the offset on
success (at-least-once). There is no HTTP surface — the domain is reached over records whose `topic`
header names the Benzene topic.

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
