# AWS orders example

The shared [`orders_domain`](../orders_domain) hosted on **AWS Lambda**, in one function handling
**multiple event sources**:

- **API Gateway (HTTP)** — `POST /orders` and `GET /orders/{id}`.
- **SQS** — the `orders.created` subscriber (partial-batch-failure aware).
- **SNS** — the same subscriber, over SNS.
- **Egress** — placing an order publishes `orders.created` via an SNS outbound client.

Only [`host.py`](host.py) is AWS-specific; the handlers and topics come from `orders_domain`.

## Run the tests (no cloud needed)

```bash
pytest examples/aws_orders
```

The tests dogfood `benzene.testing` + `benzene.aws.testing`, driving the real bindings in memory
across all three event sources.

## Deploy (sketch)

Package `examples/` + the `benzene-*` deps, set the Lambda handler to `main.handler`, set
`BENZENE_SNS_TOPIC_ARN`, and wire the triggers (API Gateway, an SQS event-source mapping, an SNS
subscription). The single handler dispatches by event shape.
