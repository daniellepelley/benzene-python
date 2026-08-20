# Cloud Service Profile conformance

The [Cloud Service Profile](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/cloud-service-profile.md)
is Benzene's named conformance target: the operational requirements (**R1–R8**) a deployable service
layers onto the core model. This port implements all eight. The table maps each requirement to the
Python API that satisfies it and the test that proves it.

| # | Requirement | Implementation | Proof |
|---|---|---|---|
| **R1** | Hosted middleware pipeline behind a transport | `MiddlewarePipeline` + `message_router`, hosted by `BenzeneHttpApp`, the gRPC binding, and the three cloud hosts | every transport test suite |
| **R2** | Topics served through the handler registry | `Registry` is the single source of truth; every host builds its app from it | `tests/test_core.py` |
| **R3** | Health checks + HTTP `/benzene/health` | `HealthChecks` + `health_interception` (reserved `benzene:healthcheck`); the HTTP surface returns the full `{isHealthy, healthChecks}` aggregate, `200`/`503` | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R4** | Wire-envelope invocability + HTTP `/benzene/invoke` | `BenzeneMessageApplication.handle` is the envelope entry point; `StandardPaths` exposes `POST /benzene/invoke` | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R5** | Registry-derived spec at `/benzene/spec` | `ServiceSpec.derive(registry, service=...)` + `spec_interception` (reserved `benzene:spec`); `StandardPaths` exposes `GET /benzene/spec` | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R6** | Mesh service-side feeds | `ServiceDescriptor` (reserved `benzene:mesh`), `MeshFeedSender` (register / heartbeat / traces), `trace_interception` (one `TraceEvent` per invocation) | `tests/test_mesh.py`, `mesh-*` conformance fixtures |
| **R7** | Default `/benzene/` standard paths, configurable | `StandardPaths(prefix="/benzene", ...)` — one config object, relocatable prefix; threaded into every HTTP-capable host | `tests/test_wellknown.py::test_prefix_is_configurable_and_relocates_every_surface` |
| **R8** | Join + propagate W3C trace context | `trace_interception` joins an inbound `traceparent`; `with_trace_propagation` forwards the current span to outbound calls | `tests/test_mesh.py` (outbound trace propagation), `mesh-trace-cases` |

## Claiming the profile

A service claims the profile by hosting its handlers behind a transport (R1/R2 — automatic) and wiring
the operational surfaces. Over HTTP that is one `StandardPaths` plus the mesh feeds:

```python
from benzene.core import HealthChecks, ServiceSpec
from benzene.http import BenzeneHttpApp, StandardPaths

health = HealthChecks().add("db", check_db)
spec = ServiceSpec.derive(registry, service="orders")

app = BenzeneHttpApp(
    router,
    application=application,
    standard_paths=StandardPaths(health=health, spec=spec),   # R3 + R4 + R5 + R7
)
```

The same `standard_paths=` argument works on `GcpFunctionsApp`, `AwsLambdaApp`, and `AzureFunctionsApp`,
so a service claims the HTTP surfaces identically whether it runs under ASGI or a cloud function. Add
`trace_interception` and the `MeshFeedSender` for R6/R8 — see [joining the mesh](cookbooks/joining-the-mesh.md).

A service booted from a `BenzeneStartUp` declares the surfaces once, in the composition root, by
returning them on its `AppDefinition` (alongside `router` and `middleware`) — then every host and the
test harness pick them up from that one declaration:

```python
class OrdersStartUp(BenzeneStartUp):
    def configure(self, services, config) -> AppDefinition:
        router, registry = ...   # built here, so the spec derives from the registry in hand
        return AppDefinition(
            router=router,
            standard_paths=StandardPaths(health=health, spec=ServiceSpec.derive(registry, service="orders")),
        )
```

## The two documents: spec vs. descriptor

R5's spec document and R6's mesh `ServiceDescriptor` are both **derived from the registry** and share
one schema derivation (`benzene.core.json_schema`), but serve different audiences:

- **`ServiceSpec`** (`/benzene/spec`) — the minimal, transport-facing "what I serve": `{service, topics}`
  with request/response schemas. Depends only on `benzene-core`.
- **`ServiceDescriptor`** (`benzene:mesh`) — the richer mesh view: adds service identity, placement, and
  a content `descriptorHash` for drift detection. Lives in `benzene-mesh`.

A service can expose either or both; they never disagree about the topics, because both read the one
registry.
