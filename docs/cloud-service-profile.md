# Cloud Service Profile conformance

The [Cloud Service Profile](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/cloud-service-profile.md)
is Benzene's named conformance target: the operational requirements (**R1–R5, R7**) a deployable
service layers onto the core model — the surfaces any tool can pull from the service itself, with no
collector. This port implements all six. The table maps each requirement to the Python API that
satisfies it and the test that proves it. Joining a mesh is the separate **meshed-service** claim
(mesh.md §7.1, [below](#the-meshed-service-claim-m1-m2)); its M1 and M2 were the profile's R6 and R8,
ids that are retired and not reused.

| # | Requirement | Implementation | Proof |
|---|---|---|---|
| **R1** | Hosted middleware pipeline behind a transport | `MiddlewarePipeline` + `message_router`, hosted by `BenzeneHttpApp`, the gRPC binding, and the three cloud hosts | every transport test suite |
| **R2** | Topics served through the handler registry | `Registry` is the single source of truth; every host builds its app from it | `tests/test_core.py` |
| **R3** | Health checks: `benzene:healthcheck` intercepted, plus HTTP `/benzene/health` where there is an HTTP surface | `HealthChecks` + `health_interception` (reserved `benzene:healthcheck`, answering `ok` with the verdict in `isHealthy`) — installed by the startup, or by `application_from` from `StandardPaths(health=...)`, so one declaration answers both; the HTTP surface returns the `{isHealthy, healthChecks}` report, `200`/`503` by `isHealthy`. A message-only service meets R3 with the topic alone | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py`, `tests/test_cloud_service_profile.py` |
| **R4** | Wire-envelope invocability + HTTP `/benzene/invoke` | `BenzeneMessageApplication.handle` is the envelope entry point; `StandardPaths` exposes `POST /benzene/invoke` (200 for every processed envelope, 400 only for a malformed one; `invoke_topics` restricts it) | `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py` |
| **R5** | Registry-derived spec: the `benzene:spec` topic wherever the transport carries a reply, and `/benzene/spec` over HTTP | The cross-language **Contract Document** (contract-document.md), derived once at the composition root from `AppDefinition(contract=ContractInfo(...), outbound=...)`; the reserved `benzene:spec` topic is intercepted in the pipeline and answered with it wherever the transport carries a reply (`spec_interception`, installed by `application_from`) and `GET /benzene/spec` runs that topic through the pipeline (`not-implemented`/501 for a type or format it does not render) | `tests/test_contract_document.py`, `tests/test_wellknown.py`, `tests/test_cloud_wellknown.py`, `contract-document-cases.json` |
| **R7** | Default `/benzene/` standard paths, configurable | `StandardPaths(prefix="/benzene", ...)` — one config object, relocatable prefix; threaded into every HTTP-capable host | `tests/test_wellknown.py::test_prefix_is_configurable_and_relocates_every_surface` |

## The meshed-service claim (M1, M2)

A **meshed** service (mesh.md §7.1) is a Cloud Service plus:

| # | Requirement | Implementation | Proof |
|---|---|---|---|
| **M1** | Mesh service-side feeds (formerly R6) | registration (`MeshFeedSender.register(registration_document(definition))` — R5's document, mesh.md §2), heartbeats (`Heartbeat.for_document`, carrying `registrationHash` + `serviceVersion`), and traces (`trace_middleware`, one `TraceEvent` per invocation) | `tests/test_mesh.py`, `tests/test_cloud_service_profile.py`, `contract-document-cases.json`'s producer groups, `mesh-trace-cases.json` |
| **M2** | Join + propagate W3C trace context on every outbound message (formerly R8) | `trace_middleware` joins an inbound `traceparent` (keeping its `tracestate`); `with_trace_propagation` forwards `traceparent` + `tracestate` on whatever `MessageSender` it wraps — sends and published events share that one port, so wrapping the event publisher covers events | `tests/test_mesh.py` (outbound trace propagation, `tracestate`, a published event fanned out to two subscribers), `mesh-trace-cases` |

The self-check reports M1/M2 on `report.mesh` (`mesh_missing`, `is_meshed`), never in `missing` or the
document's `profile.missing`: a service that does not join a mesh still claims the profile in full. The
meshed claim requires the profile — `is_meshed` is true only when `is_conformant` is too.

## Claiming the profile

A service claims the profile by hosting its handlers behind a transport (R1/R2 — automatic) and wiring
the operational surfaces. Over HTTP that is one `StandardPaths` and a `ContractInfo`:

```python
from benzene.core import AppDefinition, ContractInfo, HealthChecks
from benzene.http import BenzeneHttpApp, StandardPaths

health = HealthChecks().add("db", check_db)

app = BenzeneHttpApp.from_definition(
    AppDefinition(
        router=router,
        standard_paths=StandardPaths(health=health),   # R3 (topic + route) + R4 + R5's route + R7
        contract=ContractInfo("orders"),               # R5's document
    )
)
```

A message-only service (a queue consumer, no HTTP surface) meets R3 by intercepting
`benzene:healthcheck` — `AppDefinition(middleware=[health_interception(health)], ...)` — and needs no
`/benzene/health`; `StandardPaths(health=...)` on the definition installs the same interception, so
an HTTP service declares its health once for both faces.

The same `standard_paths=` argument works on `GcpFunctionsApp`, `AwsLambdaApp`, and `AzureFunctionsApp`,
so a service claims the HTTP surfaces identically whether it runs under ASGI or a cloud function. Add
`trace_middleware` and the `MeshFeedSender` (registering the same document) to be meshed too (M1/M2)
— see [joining the mesh](cookbooks/joining-the-mesh.md).

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

## One document: served and registered

R5's Contract Document is **derived from the registrations** once, at the composition root, and the
mesh registration (M1) reuses it rather than describing the service a second time:

- **Served** — `/benzene/spec` and the reserved `benzene:spec` topic answer with it
  (contract-document.md): `{schemaDialect, service, serviceVersion?, versionScheme?, description?,
  runtime?, placement?, profile?, degraded?, messageEndpoint?, transports?, contractHash,
  registrationHash, requests[], events[], sends[], components}`, the single input every language's
  client generator parses. Its schemas are OpenAPI 3.0 Schema Objects with named components. Depends
  only on `benzene-core`.
- **Registered** — the `benzene:mesh:register` body is that same document
  (`benzene.mesh.registration_document(definition)`, mesh.md §2). The deployment-side fields
  (`runtime`, `placement`, `profile`, `degraded`) are declared on `ContractInfo` and sit outside both
  hashes, so the two can never disagree; heartbeats carry its `registrationHash`.

The self-check's verdict rides on the document's `profile`
(`benzene.mesh.declare_cloud_service_profile`): `{"name": "cloud-service", "missing": [...]}`, where
`missing` lists only R1–R5/R7 ids and is omitted when none. The probe audits R5's topic half by
invoking `benzene:spec` over `/benzene/invoke`, and its HTTP half at `/benzene/spec`.
