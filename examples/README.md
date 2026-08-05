# Benzene Python examples

Runnable sample apps that prove the framework's promise — *write your handlers once, host them
anywhere* — and that are held to the [Port Quality Standards](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/port-quality-standards.md):
each cloud example exercises **multiple transports**, ships its **own tests that dogfood the port's
own test helpers**, and runs as a **required CI gate** (in-memory, no cloud credentials).

## The shared domain: `orders_domain`

The transport-agnostic business logic — order handlers and models — reused by every host. A host
example is just a `host.py` that mounts this domain onto one cloud's transports. When adding a demo
capability, put the handler in `orders_domain` and wire it from the hosts rather than duplicating.

## Per-cloud hosts

| Example | Host | Transports | Package |
|---|---|---|---|
| [`gcp_orders/`](gcp_orders) | Google Cloud Functions | HTTP + Pub/Sub + Pub/Sub egress | `benzene-gcp` |
| [`aws_orders/`](aws_orders) | AWS Lambda | API Gateway (HTTP) + SQS + SNS + SNS egress | `benzene-aws` |
| [`azure_orders/`](azure_orders) | Azure Functions | HTTP + Service Bus + Event Hub + Service Bus egress | `benzene-azure` |

## Non-cloud host

| Example | Host | Transports | Package |
|---|---|---|---|
| [`grpc_orders/`](grpc_orders) | gRPC server | gRPC unary (method = topic) + faked egress | `benzene-grpc[transport]` |

The gRPC example mounts the *same* `orders_domain` on a real in-process `grpc.Server`; its test dials
that server with the actual `GrpcMessageSender` client, so it is a genuine gRPC round trip rather than
an in-memory harness. Because the binding serves every topic as one generic method, the domain's
`POST /orders` / `GET /orders/{id}` routes are reached as the `orders:place` / `orders:get` topics.

## Running the tests

All example tests run as part of the normal suite (they're on `testpaths`):

```bash
pytest examples            # just the examples
pytest                     # library + examples
```

They are in-memory and credential-free — the same tests the CI gate runs. Each example's `README.md`
covers running and deploying it against the real cloud.
