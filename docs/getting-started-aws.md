# Getting started: Benzene on AWS Lambda

Take a set of transport-neutral Benzene handlers and host them on **AWS Lambda** — reached over
**API Gateway** (HTTP), **SQS**, and **SNS**, all in one function — and publish events back out over
SNS. One function, one pipeline, the handlers unchanged. Adding a source is a line of host wiring,
never a change to your logic.

This guide goes from `pip install benzene-aws` to a Lambda handler that answers `POST /orders`,
consumes the `orders:created` event over both SQS and SNS, and republishes it — all exercised
in-memory with no AWS account. It builds on the base tutorial: read
[Getting started](getting-started.md) first for the handler / `Result` / `@message` fundamentals;
here we only add the AWS host.

> **Runnable version:** [`examples/aws_orders`](https://github.com/daniellepelley/benzene-python/tree/main/examples/aws_orders)
> is exactly this guide — the shared
> [`orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain)
> hosted on Lambda, with dogfooded tests that push a native API Gateway, SQS, and SNS event through
> the real bindings. Read it alongside this page.

## Prerequisites

- **Python 3.10+**, `pip`, and a virtual environment (see [Getting started](getting-started.md)).
- An AWS account with the [AWS CLI](https://aws.amazon.com/cli/) configured, **only if you want to
  deploy** — everything up to and including local testing needs no cloud.

## 1. Install the package

```bash
pip install benzene-aws            # add [boto3] for the real outbound clients
```

> **Not on PyPI yet.** Until the first release these names don't resolve — install the
> `benzene-*` layers from a local checkout of this repo instead, then carry on with the guide
> unchanged:
> `git clone https://github.com/daniellepelley/benzene-python && cd benzene-python && pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http -e 'packages/benzene-aws[boto3]'`

The distribution is **`benzene-aws`**. It depends on `benzene-core` (the pipeline and message
handlers) and `benzene-http` (the API Gateway binding reuses the HTTP router), so a single install
pulls in everything the inbound bindings and the in-memory test host need. `boto3` is only required
by the real SNS/SQS *outbound* clients and is an optional extra:

```bash
pip install "benzene-aws[boto3]"   # when you publish events from Lambda
```

The whole thing is importable from one module:

```python
from benzene.aws import AwsLambdaApp, SnsMessageSender, SqsMessageSender, to_lambda_handler
```

## 2. Write handlers (transport-neutral)

Business logic lives in plain `async` handlers that never see AWS — the same handlers you'd host over
a standalone HTTP server, GCP, or Azure. In the example they live in the shared
[`orders_domain`](https://github.com/daniellepelley/benzene-python/tree/main/examples/orders_domain)
package. The shape is the one from [Getting started](getting-started.md): a factory closes over the
handler's collaborators (an order store, an outbound `MessageSender`) and returns the `async`
function.

```python
# orders_domain/handlers.py  (excerpt)
from benzene.core import Handler, MessageSender
from benzene.results import Result

from .model import ORDER_CREATED_TOPIC, OrderCreated, PlaceOrder


def make_place_order(service: OrderService, sender: MessageSender) -> Handler:
    async def place_order(request: PlaceOrder) -> Result:
        if not request.sku:
            return Result.bad_request("sku is required")
        order = service.place(request.sku, request.quantity)
        await sender.send_message(ORDER_CREATED_TOPIC, OrderCreated(id=order.id, sku=order.sku))
        return Result.created(order)          # ingress -> handler -> egress

    return place_order


def make_on_order_created(seen: list[str]) -> Handler:
    async def on_order_created(request: OrderCreated) -> Result:
        seen.append(request.id)               # the subscriber side
        return Result.ok()

    return on_order_created
```

These handlers are wired onto an `HttpRouter` (for the routes) and a `Registry` (all topics,
including the pub/sub subscriber) inside a single composition root — a
[`BenzeneStartUp`](reference/core.md) subclass, `OrdersStartUp`, that both deployment and tests boot
from (the [composition-root path](getting-started.md#two-ways-to-wire-a-service); its full body is in
[Getting started](getting-started.md#the-composition-root-when-you-want-the-shared-seams)). The `orders:created` subscriber is registered as a topic, so **the same handler answers the
event whether it arrives over SQS or SNS.** Nothing in `orders_domain` mentions Lambda.

## 3. Build the AWS host

Only one file is AWS-specific. It boots the shared `OrdersStartUp`, overrides the single outbound
edge with a real `SnsMessageSender`, and specializes the app to Lambda with `AwsLambdaApp`:

```python
# aws_orders/host.py
import os

from benzene.aws import AwsLambdaApp, SnsMessageSender
from benzene.core import Container, MessageSender, build_application
from orders_domain import OrdersStartUp


def build_aws_orders_app() -> AwsLambdaApp:
    topic_arn = os.environ.get("BENZENE_SNS_TOPIC_ARN")
    if not topic_arn:
        raise RuntimeError(
            "Set BENZENE_SNS_TOPIC_ARN to run the AWS host (tests use create_test_host instead)."
        )

    def use_sns(services: Container) -> None:
        services.add_instance(MessageSender, SnsMessageSender(topic_arn))

    definition, _ = build_application(OrdersStartUp, overrides=[use_sns])
    return AwsLambdaApp.from_definition(definition)
```

Two things to notice:

- **`AwsLambdaApp.from_definition(definition)`** builds the host from the composition root's
  `AppDefinition` in one line: it wires the `http_router` (so API Gateway events route by
  path → topic), the built application (so SQS/SNS records route by topic), and the `standard_paths`
  [Cloud Service Profile](cloud-service-profile.md) well-known surfaces (`/benzene/invoke`,
  `/benzene/health`, `/benzene/spec`). You can still construct `AwsLambdaApp` directly with
  `http_router=`/`registry=` if you're wiring a registry by hand — see the
  [`benzene.aws` reference](reference/aws.md#awslambdaapp).
- **`SnsMessageSender(topic_arn)`** is the egress. It implements `benzene.core.MessageSender`,
  publishes to the SNS topic ARN, and carries the Benzene topic in the `topic` message attribute
  (headers become attributes too, so correlation propagates). It creates its `boto3` SNS client
  lazily on first send — that is the only place `boto3` is used, and why it is an optional extra.
  There is a matching `SqsMessageSender(queue_url)` if you'd rather publish onto a queue; see
  [Outbound clients](reference/aws.md#outbound-clients).

The composition root is shared with every other host; **only the outbound `MessageSender` differs**
between deployment (real SNS) and tests (a fake). That single seam is what makes the tests in step 5
possible.

## 4. Wire the Lambda entry point

The handler AWS invokes is produced by `to_lambda_handler` — it wraps the app in the
`handler(event, context)` callable Lambda expects:

```python
# aws_orders/main.py
from benzene.aws import to_lambda_handler

from .host import build_aws_orders_app

handler = to_lambda_handler(build_aws_orders_app())
```

Point your Lambda's handler string at this attribute — `main.handler` (or `aws_orders.main.handler`
if you package the module inside a package). That single callable dispatches by **event shape**
([transport-bindings](https://benzene.app/docs/specification/transport-bindings)) across all nine
sources the host binds:

- an **API Gateway** event → route → topic → handler; the Benzene status maps to an HTTP status code
  and the result comes back as an API Gateway proxy response;
- an **SQS** event → one pipeline invocation and one scope **per record**; the topic is read from the
  `topic` message attribute; any record whose handler doesn't succeed comes back in
  `batchItemFailures` so only *that* record is redelivered;
- an **SNS** event → one invocation per record (topic from the `topic` attribute); SNS is
  fire-and-forget, so there is no response — a failing handler **raises** so Lambda retries;
- an **S3** object-created notification → one scope per record; no partial-batch channel, so a
  failure raises;
- an **EventBridge** event → a *single* event (no `Records` array), one scope; the topic is the
  event's `detail-type`; a failure raises so the rule retries;
- a **DynamoDB Streams** or **Kinesis Data Streams** event → one scope per record; both carry a
  partial-batch channel, so failures come back in `batchItemFailures` keyed by sequence number;
- a **Kafka / MSK** event → records grouped by topic-partition, one scope per record; the MSK source
  has no partial-batch channel, so a failure raises for redelivery;
- a **direct invoke** — a bare `{"topic", "headers", "body"}` Payload from another Lambda's
  `lambda.invoke(...)` — → one scope; the response envelope comes straight back as the invoke's
  response Payload.

The channel-less sources (S3, EventBridge, DynamoDB, Kinesis) carry no message metadata on the wire,
so their topic comes from an injectable convention on the host — `s3_topic=...`,
`eventbridge_topic=...` (a fallback: EventBridge prefers the event's own `detail-type`),
`kinesis_topic=...`, and `dynamodb_topic=None`, which means "derive it per record from `eventName`".
See [reference/aws.md](reference/aws.md) for the full table.

Classification happens in `benzene.aws.event_source(event)`; an event that is none of the nine
raises `ValueError`.

## 5. Test every source in memory (dogfooded)

Before deploying, drive the real bindings in-memory with `create_test_host(...).build_aws()`. It
boots your actual `OrdersStartUp` — the same construction the deployed handler performs — and returns
an `AwsLambdaTestHost` you push native events into. Fake **only the external edge** (the outbound
client); everything else is the real pipeline, routing, and handlers.

```python
# aws_orders/tests/test_aws_orders.py
import json

from benzene.aws.testing import SqsEventBuilder
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)     # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_aws()
    return host, service, sender, seen
```

`FakeMessageSender` records what was published instead of calling AWS, so a test can assert that
ingress reached egress. `.build_aws()` is the only AWS-specific line — swap it for `.build_gcp()` or
`.build_azure()` and the same test runs against another cloud.

**API Gateway ingress → handler → SNS egress:**

```python
def test_api_gateway_place_order_creates_and_publishes():
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert sender.last_topic == ORDER_CREATED_TOPIC      # the handler published on the way out
    assert sender.last_message.id == order["id"]
    assert order["id"] in service.orders
```

`send_http` returns an `ApiGatewayResponse` (`.status_code`, `.headers`, `.body`) — the Benzene
status `created` mapped to HTTP `201`.

**SQS and SNS ingress** reach the *same* subscriber:

```python
def test_sqs_order_created_is_handled():
    host, _, _, seen = make_host()
    result = host.send_sqs(ORDER_CREATED_TOPIC, {"id": "ord-sqs", "sku": "ABC"})
    assert result.batch_item_failures == []              # SQS partial-batch protocol
    assert seen == ["ord-sqs"]


def test_sns_order_created_is_handled():
    host, _, _, seen = make_host()
    host.send_sns(ORDER_CREATED_TOPIC, {"id": "ord-sns", "sku": "ABC"})   # fire-and-forget, no return
    assert seen == ["ord-sns"]
```

`send_sqs` / `send_sqs_event` return an `SqsBatchResponse` — assert on `.batch_item_failures` (or
`.item_identifiers`), the object mirror of the SQS partial-batch protocol. `send_sns` returns
nothing, matching SNS's fire-and-forget delivery.

**SQS partial-batch failure** — only the bad record is reported, the good one still processes:

```python
def test_sqs_partial_batch_failure_reports_only_failed_record():
    host, _, _, seen = make_host()
    event = (
        SqsEventBuilder()
        .with_message(ORDER_CREATED_TOPIC, {"id": "ok-1", "sku": "A"}, message_id="m1")
        .with_message("orders:unknown", {}, message_id="m2")   # no handler -> not-found -> fails
        .build()
    )
    result = host.send_sqs_event(event)
    assert result.batch_item_failures == [{"itemIdentifier": "m2"}]
    assert seen == ["ok-1"]                                     # the good record still processed
```

`SqsEventBuilder` (and `SnsEventBuilder` / `ApiGatewayRequestBuilder`) build the exact native event
shapes AWS delivers, so these tests exercise the real decoders, not a mock of them. Run them with no
cloud:

```bash
pytest examples/aws_orders
```

See the [testing reference](reference/testing.md) and [`benzene.aws` testing](reference/aws.md#testing)
for the full surface.

## 6. Deploy (sketch)

Package the module together with its `benzene-*` dependencies (add `benzene-aws[boto3]` to your
`requirements.txt` so the SNS client's `boto3` is present in the deployment bundle), then:

1. Create the Lambda function and set its **handler** to `main.handler` (or your packaged path).
2. Set the environment variable **`BENZENE_SNS_TOPIC_ARN`** to the ARN of the SNS topic the
   `place_order` handler publishes to.
3. Attach the triggers this example uses — every one flows into the same function, and you can add
   any of the other sources from [§7](#7-supported-event-sources) the same way:
   - an **API Gateway** proxy integration (REST v1 or HTTP API v2 both work — the binding detects
     either shape),
   - an **SQS** event-source mapping (enable *ReportBatchItemFailures* so `batchItemFailures` is
     honored),
   - an **SNS** subscription.

There is no framework-specific deployment tooling — this is an ordinary Python Lambda, so package it
however you already do (a zip, container image, SAM, CDK, Terraform, or the console). The
[Hosting on AWS Lambda](cookbooks/hosting-on-aws.md) cookbook has the compact end-to-end recap.

## 7. Supported event sources

`benzene.aws` binds nine Lambda event sources, all through the one function:

| Source | Topic comes from | Response | On handler failure |
| --- | --- | --- | --- |
| **API Gateway** | the route (path → topic, via `benzene.http`) | API Gateway proxy response; Benzene status → HTTP code | error status → HTTP error code |
| **SQS** | the `topic` message attribute | `{"batchItemFailures": [...]}` | that record's id reported for redelivery |
| **SNS** | the `topic` message attribute | none (fire-and-forget) | raises → Lambda retries the invocation |
| **S3** (object-created) | host convention (`s3_topic`, default `s3:object-created`) | none | raises → the notification is retried |
| **EventBridge** | the event's `detail-type` (else `eventbridge_topic`, default `eventbridge:event`) | none | raises → the rule retries |
| **DynamoDB Streams** | the record's `eventName` (`dynamodb:insert`), or `dynamodb_topic` | `{"batchItemFailures": [...]}` | that record's sequence number reported |
| **Kinesis Data Streams** | host convention (`kinesis_topic`, default `kinesis:record`) | `{"batchItemFailures": [...]}` | that record's sequence number reported |
| **Kafka / MSK** | the `topic` record header (else the record's own Kafka topic name) | none | raises → redelivery |
| **Direct invoke** | the Payload's own `topic` field | the Benzene response envelope, verbatim | error status returned in the envelope |

The topic attribute for SQS/SNS/Kafka is written automatically by any Benzene outbound client
(`SnsMessageSender` / `SqsMessageSender` / `KafkaMessageSender`), so a Benzene-to-Benzene flow needs
no extra configuration. For those sources the message *body* is the serialized payload and message
*attributes* become Benzene headers (`benzene.core.read_message_metadata`). The channel-less sources
(S3, EventBridge, DynamoDB, Kinesis) carry no metadata on the wire, so their topic comes from the
host convention shown above — configured as constructor keyword arguments on `AwsLambdaApp`.

A direct invoke is the one **synchronous** source besides API Gateway: `LambdaMessageSender` decodes
the target's response envelope straight back into a `Result`. Alongside the inbound bindings, the
package ships **EventBridge, Kinesis, and Lambda outbound clients** (on top of the SNS/SQS senders)
and a **self-hosted SQS consumer** (`SqsConsumerApp` / `run_consumer_loop`) for a long-running worker
or a Kubernetes Deployment that polls a queue itself rather than being invoked by a Lambda
event-source mapping.

> **Compared with the .NET port:** the one .NET affordance still missing is a `UsePresetTopic` option
> for the attribute-carrying transports — accepting messages from a **raw, non-Benzene SQS producer**
> that never writes a `topic` attribute; here SQS, SNS, and Kafka still require it.

## 8. IAM / permissions

The example needs exactly these permissions, driven by the code you saw above:

- **SNS publish (egress).** `SnsMessageSender.send_message` calls `sns:Publish` on
  `BENZENE_SNS_TOPIC_ARN`, so the function's execution role needs `sns:Publish` on that topic ARN.
  (If you use `SqsMessageSender` instead, it needs `sqs:SendMessage` on the queue.)
- **SQS consume (ingress).** An SQS event-source mapping polls on the function's behalf, so the
  execution role needs `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:GetQueueAttributes` on the
  source queue.
- **SNS subscribe (ingress).** SNS invokes the function through a resource-based Lambda permission
  (`lambda:InvokeFunction` for the SNS principal) — **no execution-role IAM** is needed to *receive*
  SNS notifications.

API Gateway similarly invokes the function via a resource-based permission and needs no
execution-role IAM to receive requests.

## 9. Observability

- **Headers / correlation.** SQS/SNS message attributes are lifted into Benzene headers on the way
  in, and the outbound clients forward Benzene headers back out as attributes, so a correlation or
  trace header set upstream flows end to end without host-specific code.
- **Lambda context.** `to_lambda_handler` passes the native `context` into `AwsLambdaApp.handle`; it
  is available to the host but is not injected into handlers today — keep handlers transport-neutral
  and read request metadata from headers instead.
- **Reading one of those headers in a handler.** A handler takes only the request, so lift the header
  onto it in a middleware — see
  [Reading a header in a handler](reference/core.md#reading-a-header-in-a-handler) for the five-line
  recipe. It works identically on every source here, since each lifts its native metadata (SQS/SNS
  message attributes, API Gateway headers, Kafka record headers) into `context.headers` on the way in.

> **Compared with the .NET port:** Python's `benzene.aws` host still does not ship the .NET host's
> invocation feature (`UseBenzeneInvocation`), a W3C-trace-context middleware, or log-enrichment
> middleware; correlation is available at the header level as described above. Tracing, however, is no
> longer .NET-only: the port already traces every invocation through the mesh (`benzene.mesh`'s
> `trace_interception`), and [`benzene-otel`](reference/otel.md) exports those existing mesh spans through
> the OpenTelemetry SDK (topic → span name, semantic status → OTel span status). That is span *export*,
> not automatic `Activity`-style instrumentation of the AWS SDK — you opt in by wiring
> `OtelTraceExporter` into the mesh trace middleware.

## 10. Troubleshooting

- **`ImportError: build_aws() requires the 'benzene-aws' package to be installed`** — the test
  harness imports the AWS host lazily. `pip install benzene-aws`.
- **`RuntimeError: Set BENZENE_SNS_TOPIC_ARN ...` on startup** — the host needs the topic ARN to
  build a real `SnsMessageSender`. Set it for deployment; tests don't run the host — they register a
  `FakeMessageSender` via `create_test_host(OrdersStartUp).with_services(...)` instead.
- **`ModuleNotFoundError: No module named 'boto3'` at first publish** — `boto3` is an optional extra.
  Install `benzene-aws[boto3]` and include it in your deployment bundle.
- **`ValueError: Unrecognised Lambda event` at runtime** — the payload wasn't API Gateway, SQS, or
  SNS shaped. Check the trigger wiring; a test event pasted in the console must match one of the real
  event shapes (use the `*EventBuilder`s as a reference).
- **SQS/SNS message never routes to a handler** — the topic is read from the `topic` message
  *attribute*, not the body. Confirm the producer sets it (Benzene clients do automatically) and that
  a handler is registered for that topic. An unknown topic yields `not-found`, which for SQS surfaces
  as a `batchItemFailures` entry and for SNS raises so Lambda retries.
- **A `not-found` for a route you defined over API Gateway** — the HTTP method must match too;
  a `GET` route won't answer a `POST` (same rule as [Getting started](getting-started.md)).
- **Handler raised an exception** — Benzene turns an uncaught error into a `service-unavailable`
  result rather than crashing; over API Gateway that's HTTP `503`, over SQS a reported failure, over
  SNS a raised error that triggers Lambda retry.

## See also

- [`benzene.aws` reference](reference/aws.md) — the full API: `AwsLambdaApp`, `to_lambda_handler`,
  the outbound clients, and the test helpers.
- [Hosting on AWS Lambda](cookbooks/hosting-on-aws.md) — the compact cookbook version of this flow.
- [Getting started](getting-started.md) — the handler / `Result` / routing fundamentals this guide
  builds on.
- [`benzene.testing` reference](reference/testing.md) and [`benzene.http` reference](reference/http.md).
- Specification: [transport-bindings](https://benzene.app/docs/specification/transport-bindings),
  [wire-contracts](https://benzene.app/docs/specification/wire-contracts), and the
  [Cloud Service Profile](https://benzene.app/docs/specification/cloud-service-profile).
