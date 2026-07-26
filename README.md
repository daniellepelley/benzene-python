# benzene-python

A **Python port of [Benzene](https://github.com/daniellepelley/Benzene)** — the middleware-based,
hexagonal (ports-and-adapters) message-handler framework whose promise is *write your message
handlers once, host them anywhere*.

This port is **spec-first**: it implements the language-neutral Benzene specification
([`core-concepts`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/core-concepts.md),
[`wire-contracts`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/wire-contracts.md),
[`transport-bindings`](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/transport-bindings.md))
**idiomatically in Python** — it does not translate the C# API. Cross-language interop is the point:
a Python Benzene service and a .NET/Go/TypeScript one speak the same wire contract and show up in the
same mesh.

> **Status: early foundation (Benzene Core).** The core model + the `BenzeneMessage` envelope entry
> point are implemented and pass the language-neutral **conformance fixtures** (status vocabulary,
> HTTP status mapping, and the end-to-end envelope cases against the canonical handlers). Transport
> bindings (HTTP server, Pub/Sub, …), the mesh module, and payload versioning are not built yet.

## The core idea in 60 seconds

A handler is just `async def handle(request) -> Result` — it never sees the transport:

```python
from dataclasses import dataclass
from benzene import BenzeneMessageApplication, Registry, Result, message

@dataclass
class Greet:
    name: str = ""

@message("say:hello", request_type=Greet)
async def hello(request: Greet) -> Result:
    return Result.ok({"greeting": f"Hello {request.name}"})

app = BenzeneMessageApplication(Registry().add(hello))

# Drive it with a Benzene message envelope (the transport-neutral entry point):
response = await app.handle_async(
    {"topic": "say:hello", "headers": {}, "body": '{"name":"world"}'}
)
# -> {"statusCode": "ok", "headers": {"content-type": "application/json"},
#     "body": '{"greeting": "Hello world"}'}
```

Explicit registration is first-class (`Registry().register("say:hello", hello)`); the `@message`
decorator is the idiomatic sugar over it.

## What's implemented

| Spec concept | Module |
|---|---|
| Status vocabulary + success classification (wire §3) | `benzene/status.py` |
| `Result` (core §5) | `benzene/result.py` |
| Topic + handler registry, version selection, duplicate = startup error (core §2, §9) | `benzene/registry.py`, `benzene/handler.py` |
| Middleware pipeline — onion order, short-circuit, one-invocation (core §4) | `benzene/pipeline.py` |
| Context + request/response mapping (core §6) | `benzene/context.py`, `benzene/_mapping.py` |
| Minimal DI container + per-invocation scope, overridable defaults (core §8) | `benzene/container.py` |
| BenzeneMessage envelope + router terminal middleware (wire §1) | `benzene/envelope.py`, `benzene/router.py` |
| Benzene ↔ HTTP status mapping (wire §4.1) | `benzene/http_status.py` |

## Conformance

The language-neutral fixtures from the spec live in [`conformance/`](conformance/) and are run two
ways:

```bash
# dependency-free (no pytest needed)
python -m tests.conformance_runner

# or under pytest (one test per envelope case)
pip install -e ".[dev]"
pytest
```

Passing these plus the live cross-language interop checks (send/receive the envelope against a
running .NET Benzene service) is what "conformant" means — see the spec's
[porting guide §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/porting-guide.md).

## Roadmap

1. **(done)** Wire contracts + core model + `BenzeneMessage` envelope, conformance-green.
2. An HTTP inbound binding end-to-end (ASGI), including the status-code mapping.
3. One cloud host (Google Cloud Functions / AWS Lambda) — the "host anywhere" proof on Python.
4. The mesh module (ServiceDescriptor + `/benzene/spec`) so Python services appear in the mesh UI.
5. Payload versioning, more transports (Pub/Sub, SQS/SNS), and gRPC.
