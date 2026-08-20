# `sqs_orders` — the order domain on AWS SQS (self-hosted)

Mounts the shared [`orders_domain`](../orders_domain) on the **self-hosted SQS consumer** binding
(`benzene.aws.run_consumer_loop`) — distinct from `aws_orders`, which reaches SQS via a *Lambda
event source*. This one polls the queue itself, the shape you want for a long-running worker, a
container, or a Kubernetes Deployment. The same handlers as every other host — only
[`host.py`](host.py) is SQS-specific.

Unlike the cloud hosts (triggered by a runtime), this service owns its own loop:
`run_consumer_loop` long-polls a queue, dispatches one message at a time, and deletes it on
success (at-least-once — a failed message is left on the queue for redelivery/DLQ redrive). There is
no HTTP surface — the domain is reached over messages whose `topic` message attribute names the
Benzene topic.

**Worth using even if SQS is the only transport this service ever has.** Unlike HTTP, where a real
ASGI framework already gives you routing and cross-cutting middleware for free (see
[Why not just a minimal ASGI app?](../../docs/getting-started.md#why-not-just-a-minimal-asgi-app)),
`boto3`'s `receive_message` on its own hands you a raw message and stops — deserializing the payload,
dispatching on whatever identifies its type, and every cross-cutting concern (validation, correlation,
retries, structured logging) is code you'd otherwise write yourself, usually inline in the poll loop.
`SqsConsumerApp` + the middleware pipeline is that missing layer, for SQS specifically.

## Run it against a real queue

```bash
pip install "benzene-aws[boto3]"
export BENZENE_SQS_CONSUME_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-in
export BENZENE_SQS_EVENTS_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/<account-id>/orders-events
python -m sqs_orders.host
```

Set `SQS_ENDPOINT_URL` too (e.g. `http://localhost:4566` for LocalStack) to run against an emulator
instead of real AWS — see [`examples/k8s_orders`](../k8s_orders) for that wired up end to end alongside
the Kafka and HTTP legs of this same domain.

## Test it (no queue)

The tests dogfood the real binding through the shared harness — the same one-specialization-step setup
as the Kafka/AWS/GCP/Azure suites, only `.build_sqs_consumer()` differs:

```python
host = create_test_host(OrdersStartUp).with_services(overrides).build_sqs_consumer()
result = await host.send_sqs_consumer("orders:place", body={"sku": "ABC", "quantity": 2})
assert result.status == "created"
assert fake.last_topic == "orders:created"
```

```bash
pytest examples/sqs_orders
```
