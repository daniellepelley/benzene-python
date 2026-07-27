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

> **Status: Core + inbound HTTP binding.** Shipped as three layered packages
> (`benzene-results`, `benzene-core`, `benzene-http`) that pass the language-neutral **conformance
> fixtures** (status vocabulary, HTTP status mapping, and the end-to-end envelope cases). A cloud
> host, the mesh module, and payload versioning are not built yet.

## Layered packages — install only what you use

Benzene is delivered as a **stack of small PyPI packages**, mirroring the .NET package layering.
This is deliberate: each package is one **adoption level**, so you take on only as much of Benzene
as you need and your deployment never ships code (or transitive dependencies) it doesn't use. Every
package contributes a subpackage to the shared `benzene` [namespace](https://peps.python.org/pep-0420/),
so `pip install benzene-http` gives you `benzene.http` alongside the `benzene.core` and
`benzene.results` it pulls in.

| Install | Import | You get | Depends on | .NET analog |
|---|---|---|---|---|
| `benzene-results` | `benzene.results` | `Result` + status vocabulary — the return type of a handler | — (zero deps) | `Benzene.Results` |
| `benzene-core` | `benzene.core` | pipeline, registry, `@message`, DI, the `BenzeneMessage` envelope | `benzene-results` | `Benzene.Core*` + `Benzene.Dependencies` |
| `benzene-http` | `benzene.http` | inbound HTTP (ASGI) binding + status mapping | `benzene-core` | `Benzene.Http` |
| `benzene-gcp` | `benzene.gcp` | Google Cloud Functions host (HTTP + Pub/Sub + egress) | `benzene-core`, `benzene-http` | `Benzene.GoogleCloud.Functions.*` |
| `benzene-aws` | `benzene.aws` | AWS Lambda host (API Gateway + SQS + SNS + egress) | `benzene-core`, `benzene-http` | `Benzene.Aws.Lambda.*` |
| `benzene-testing` | `benzene.testing` | in-memory test host + fakes (dev/test) | `benzene-core` | `Benzene.Testing` |

Adoption levels, bottom to top:

1. **Just results** — `pip install benzene-results`. Type a domain service's outcomes with
   `Result.ok(...)` / `Result.not_found(...)`; no pipeline, no transport, no dependencies.
2. **Run handlers** — `pip install benzene-core`. Register handlers and drive them through the
   transport-neutral `BenzeneMessage` envelope. Everything except a concrete transport.
3. **Host over HTTP** — `pip install benzene-http`. The same handlers behind a real ASGI server.

Future transports (Pub/Sub, SQS/SNS, a cloud host) and cross-cutting concerns will each be their
own package on top of `benzene-core`, so the stack grows outward without ever forcing an all-or-
nothing install. See [`docs/packages.md`](docs/packages.md) for the full rationale.

## The core idea in 60 seconds

A handler is just `async def handle(request) -> Result` — it never sees the transport:

```python
from benzene.core import BenzeneMessageApplication, Registry, message
from benzene.results import Result

@message("say:hello")
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneMessageApplication(Registry().add(hello))

# Drive it with a Benzene message envelope (the transport-neutral entry point):
response = await app.handle_async(
    {"topic": "say:hello", "headers": {}, "body": '{"name":"world"}'}
)
# -> {"statusCode": "ok", "headers": {"content-type": "application/json"},
#     "body": '{"greeting": "Hello world"}'}
```

Add `benzene-http` and the same handler hosts behind an ASGI server, topic resolved from the route
and the Benzene status mapped to an HTTP code:

```python
from benzene.http import BenzeneHttpApp, HttpRouter, http_endpoint

@http_endpoint("GET", "/greet/{name}")
@message("say:hello")
async def hello(request: dict) -> Result:
    return Result.ok({"greeting": f"Hello {request['name']}"})

app = BenzeneHttpApp(HttpRouter().add(hello))   # run: uvicorn module:app
```

## Repository layout

A monorepo of independently-publishable distributions:

```
packages/
  benzene-results/   benzene/results/   (Result, Status)
  benzene-core/      benzene/core/      (pipeline, registry, DI, envelope)
  benzene-http/      benzene/http/      (ASGI binding, status mapping)
conformance/         language-neutral spec fixtures (shared)
tests/               cross-package tests + the dependency-free conformance runner
docs/                guides, reference, and the package rationale
```

## Developing

The packages use [PEP 420 namespace packages](https://peps.python.org/pep-0420/), so for local dev
nothing needs building — the layers resolve straight off `sys.path`:

```bash
# run the tests (pytest picks up the packages/ paths from pyproject.toml)
pytest

# run the conformance runner without pytest
PYTHONPATH=packages/benzene-results:packages/benzene-core:packages/benzene-http \
  python -m tests.conformance_runner
```

Or install the layers editable (what CI does — this also verifies the packaging):

```bash
pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http
pip install pytest && pytest -q
```

## Conformance

The language-neutral fixtures from the spec live in [`conformance/`](conformance/) and run two ways
— the dependency-free `python -m tests.conformance_runner`, and one pytest case per envelope fixture.
Passing these plus the live cross-language interop checks (send/receive the envelope against a
running .NET Benzene service) is what "conformant" means — see the spec's
[porting guide §3](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/porting-guide.md).

## Roadmap

1. **(done)** Wire contracts + core model + `BenzeneMessage` envelope, conformance-green.
2. **(done)** An HTTP inbound binding end-to-end (ASGI), including the status-code mapping.
3. **(done)** Layered, install-what-you-use packages (`benzene-results` / `-core` / `-http`).
4. **(in progress)** Cloud hosts — the "host anywhere" proof on Python, each a multi-transport,
   dogfood-tested example per the [Port Quality Standards](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/port-quality-standards.md):
   **GCP (done)** — `benzene-gcp`, HTTP + Pub/Sub + egress; **AWS (done)** — `benzene-aws`, API
   Gateway + SQS + SNS + egress; **Azure** next.
5. The mesh module (ServiceDescriptor + `/benzene/spec`) so Python services appear in the mesh UI.
6. Payload versioning and gRPC.

## Documentation

Start at [`docs/index.md`](docs/index.md).
