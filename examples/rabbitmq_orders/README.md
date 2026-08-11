# `rabbitmq_orders` — the order domain on RabbitMQ

Mounts the shared [`orders_domain`](../orders_domain) on the **RabbitMQ** binding: a self-hosted
consumer that turns each delivery into one pipeline invocation, and a `RabbitMqMessageSender` that
publishes `orders:created` downstream. The same handlers as every other host — only [`host.py`](host.py)
is RabbitMQ-specific.

Unlike the cloud hosts (triggered by a runtime), a RabbitMQ service owns its own loop:
`run_consumer_loop` pulls a delivery off a queue, dispatches one at a time, and acks it on success
(at-least-once — a failed delivery is nacked for redelivery). There is no HTTP surface — the domain is
reached over deliveries whose `topic` header (carried in the AMQP `properties.headers`) names the
Benzene topic.

## Run it against a broker

```bash
pip install "benzene-rabbitmq[rabbitmq]"
export BENZENE_RABBITMQ_URL=localhost
export BENZENE_RABBITMQ_QUEUE=orders-in            # the queue to consume
export BENZENE_RABBITMQ_EXCHANGE=orders-events     # where orders:created is published (header-routed)
export BENZENE_RABBITMQ_ROUTING_KEY=               # optional routing key for that exchange
python -m rabbitmq_orders.host
```

## Test it (no broker)

The tests dogfood the real binding through the shared harness — the same one-specialization-step setup
as the AWS/GCP/Azure suites, only `.build_rabbitmq()` differs:

```python
host = create_test_host(OrdersStartUp).with_services(overrides).build_rabbitmq()
result = await host.send_rabbitmq("orders:place", body={"sku": "ABC", "quantity": 2})
assert result.status == "created"
assert fake.last_topic == "orders:created"
```

```bash
pytest examples/rabbitmq_orders
```
