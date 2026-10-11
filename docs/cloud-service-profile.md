# Cloud Service Profile conformance

The [Cloud Service Profile](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/cloud-service-profile.md)
is Benzene's named conformance target: the operational requirements (**R1–R8**) a deployable service
layers onto the core model. This port implements all eight. The table maps each requirement to the
Python API that satisfies it and the test that proves it.

| # | Requirement | Implementation | Proof |
|---|---|---|---|
| **R1** | Hosted middleware pipeline behind a transport | `MiddlewarePipeline` + `message_router`, hosted by `BenzeneHttpApp`, the gRPC binding, and the three cloud hosts | every transport test suite |
| **R2** | Topics served through the handler registry | `Registry` is the single source of truth; every host builds its app from it | `tests/test_core.py` |
| **R3** | Health checks + HTTP `/benzene/health` | `HealthChecks` + `health_interception` (reserved `benzene:healthcheck`, answering `ok` with the verdict in `isHealthy`); the HTTP surface returns the `{isHealthy, healthChecks}` report, `200`/`503` by `isHealthy` | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R4** | Wire-envelope invocability + HTTP `/benzene/invoke` | `BenzeneMessageApplication.handle` is the envelope entry point; `StandardPaths` exposes `POST /benzene/invoke` (200 for every processed envelope, 400 only for a malformed one; `invoke_topics` restricts it) | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R5** | Registry-derived spec at `/benzene/spec` | The cross-language **Contract Document** (contract-document.md), derived once at the composition root from `AppDefinition(contract=ContractInfo(...), outbound=...)`; the reserved `benzene:spec` topic answers with it (`spec_interception`, installed by `application_from`) and `GET /benzene/spec` runs that topic through the pipeline (`not-implemented`/501 for a type or format it does not render) | `tests/test_contract_document.py`, `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py`, `contract-document-cases.json` |
| **R6** | Mesh service-side feeds | `ServiceDescriptor` (reserved `benzene:mesh`), `MeshFeedSender` (register / heartbeat / traces), `trace_middleware` (one `TraceEvent` per invocation) | `tests/test_mesh.py`, `mesh-*` conformance fixtures |
| **R7** | Default `/benzene/` standard paths, configurable | `StandardPaths(prefix="/benzene", ...)` — one config object, relocatable prefix; threaded into every HTTP-capable host | `tests/test_wellknown.py::test_prefix_is_configurable_and_relocates_every_surface` |
| **R8** | Join + propagate W3C trace context | `trace_middleware` joins an inbound `traceparent`; `with_trace_propagation` forwards the current span to outbound calls | `tests/test_mesh.py` (outbound trace propagation), `mesh-trace-cases` |

## Claiming the profile

A service claims the profile by hosting its handlers behind a transport (R1/R2 — automatic) and wiring
the operational surfaces. Over HTTP that is one `StandardPaths` plus the mesh feeds:

```python
from benzene.core import AppDefinition, ContractInfo, HealthChecks
from benzene.http import BenzeneHttpApp, StandardPaths

health = HealthChecks().add("db", check_db)

app = BenzeneHttpApp.from_definition(
    AppDefinition(
        router=router,
        standard_paths=StandardPaths(health=health),   # R3 + R4 + R5's route + R7
        contract=ContractInfo("orders"),               # R5's document
    )
)
```

The same `standard_paths=` argument works on `GcpFunctionsApp`, `AwsLambdaApp`, and `AzureFunctionsApp`,
so a service claims the HTTP surfaces identically whether it runs under ASGI or a cloud function. Add
`trace_middleware` and the `MeshFeedSender` for R6/R8 — see [joining the mesh](cookbooks/joining-the-mesh.md).

A service booted from a `BenzeneStartUp` declares the surfaces once, in the composition root, by
returning them on its `AppDefinition` (alongside `router` and `middleware`) — then every host and the
test harness pick them up from that one declaration:

```python
class OrdersStartUp(BenzeneStartUp):
    def configure(self, services, config) -> AppDefinition:
        router, outbound = ...   # the handlers, and the events and sends this service declares
        return AppDefinition(
            router=router,
            outbound=outbound,
            standard_paths=StandardPaths(health=health),
            contract=ContractInfo("orders", service_version="1.4.2", version_scheme="semver"),
        )
```

## The documents: contract and descriptor

R5's Contract Document and R6's mesh `ServiceDescriptor` are both **derived from the registrations**,
but serve different audiences:

- **`ContractDocument`** (`/benzene/spec`, and the reserved `benzene:spec` topic) — R5's document and
  the cross-language one (contract-document.md): `{schemaDialect, service, serviceVersion?,
  versionScheme?, description?, messageEndpoint?, transports?, contractHash, registrationHash,
  requests[], events[], sends[], components}`, the single input every language's client generator
  parses. Its schemas are OpenAPI 3.0 Schema Objects with named components. Depends only on
  `benzene-core`.
- **`ServiceDescriptor`** (`benzene:mesh`) — the richer mesh view: adds service identity, placement, and
  a content `descriptorHash` for drift detection. Lives in `benzene-mesh`.

A service can expose either or both; they never disagree about the topics, because they read the one
registry and the one outbound registry.
