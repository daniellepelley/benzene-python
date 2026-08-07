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

The gRPC example mounts the *same* `orders_domain` on the gRPC binding and tests it through the shared
harness like every cloud (`create_test_host(...).build_grpc()` + `send_grpc`), plus one real-socket
test that proves the `GrpcMessageSender` client over a live channel. Because the binding serves every
topic as one generic method, the domain's `POST /orders` / `GET /orders/{id}` routes are reached as the
`orders:place` / `orders:get` topics.

## The mesh demo

| Example | What it shows | Package |
|---|---|---|
| [`mesh_fleet/`](mesh_fleet) | A three-service mesh (`orders` / `payments` / `shipping`) that self-describes, heartbeats + traces into a shared `MeshCollector`, and whose `MeshArtifactEmitter` output renders in the **canonical Mesh UI** (proven with headless Chromium) | `benzene-mesh` |

`orders` calls `payments` (forwarding its mesh span) so the collector derives the `payments ← orders`
edge; the emitter projects the fleet's spec + health + live collector into the six mesh-UI artifacts.
`python -m mesh_fleet.prove` renders them in a real browser and screenshots the result. See its
[`README`](mesh_fleet/README.md).

## Running the tests

All example tests run as part of the normal suite (they're on `testpaths`):

```bash
pytest examples            # just the examples
pytest                     # library + examples
```

They are in-memory and credential-free — the same tests the CI gate runs. Each example's `README.md`
covers running and deploying it against the real cloud.
