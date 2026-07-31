# Hosting on AWS Lambda (API Gateway + SQS + SNS)

Host one set of Benzene handlers on AWS Lambda across **three event sources** — API Gateway, SQS,
and SNS — and publish events back out over SNS. One function, one pipeline, the handlers unchanged.

## Prerequisites

- Python 3.10+
- `pip install benzene-aws` (add `[boto3]` for the real outbound clients)
- Runnable reference: [`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders)

## Build the host and entry point

The domain wiring (`router` + `registry`) is shared with every other cloud; only the host is
AWS-specific:

```python
# main.py
from benzene.aws import AwsLambdaApp, SnsMessageSender, to_lambda_handler
from orders_domain import OrderService, build_orders

wiring = build_orders(OrderService(), SnsMessageSender("arn:aws:sns:...:orders"))
app = AwsLambdaApp(http_router=wiring.router, registry=wiring.registry)
handler = to_lambda_handler(app)     # point your Lambda at main.handler
```

The single `handler` dispatches by event shape:

- an **API Gateway** event → route → topic → handler, Benzene status → HTTP code;
- an **SQS** event → one scope per record (topic from the `topic` message attribute); failed
  records come back as `batchItemFailures` for redelivery;
- an **SNS** event → one scope per record; a failure raises so Lambda retries.

## Test every source in memory (dogfooded)

```python
from benzene.aws.testing import AwsLambdaTestHost, SqsEventBuilder
from benzene.testing import FakeMessageSender

sender = FakeMessageSender()
host = AwsLambdaTestHost(build_app(sender=sender))

# API Gateway ingress -> handler -> SNS egress
resp = host.send_http("POST", "/orders", body={"sku": "ABC"})
assert resp.status_code == 201 and sender.last_topic == "orders:created"

# SQS + SNS ingress
assert host.send_sqs("orders:created", {"id": "1", "sku": "A"}).batch_item_failures == []
host.send_sns("orders:created", {"id": "2", "sku": "B"})

# SQS partial-batch failure: only the bad record is reported
event = (SqsEventBuilder()
         .with_message("orders:created", {"id": "3", "sku": "C"}, message_id="m1")
         .with_message("orders:unknown", {}, message_id="m2")
         .build())
assert host.send_sqs_event(event).batch_item_failures == [{"itemIdentifier": "m2"}]
```

## Deploy (sketch)

Package `examples/` with the `benzene-*` dependencies, set the handler to `main.handler` and
`BENZENE_SNS_TOPIC_ARN`, then attach the triggers: an API Gateway proxy integration, an SQS
event-source mapping, and an SNS subscription. All three flow into the same handler.

## See also

- [`benzene.aws` reference](../reference/aws.md), [`benzene.testing` reference](../reference/testing.md)
