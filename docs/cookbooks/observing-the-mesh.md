# Observing the mesh (the collector + the Mesh UI)

You have one or more Benzene services already reporting into a mesh — or at least exposing
`/benzene/spec` and `/benzene/health`. This cookbook stands up the **receiving** side: a
[`MeshCollector`](../reference/mesh.md#the-collector--meshcollector) that ingests the fleet's feeds,
projects them into the cross-language mesh-ui artifacts, and serves the canonical **Benzene Mesh UI**
so you can see your estate in one dashboard.

## What the mesh gives you

A collector is an ordinary Benzene service. It builds a live catalog of your fleet from two feeds:

- **Push** — services POST their `register` / `heartbeat` / `traces` / `issues` feeds to the collector
  (via [`MeshFeedSender`](../reference/mesh.md#collector-feeds)). This is where the **call graph** comes
  from: a traced invocation whose parent span belongs to a *different* service makes that service a
  consumer of the topic — consumer-edge topology is derived from pushed traces, never declared.
- **Pull** — the collector reaches out to a configured fleet on a timer
  ([`MeshPoller`](../reference/mesh.md#the-poller--meshpoller-pull-aggregator)) and reads each service's
  well-known `/benzene/spec` + `/benzene/health`. Pull covers identity, topics, and health; a service
  needs no egress wiring to appear, just to be pollable.

The two feeds compose in one collector. From the catalog it derives the fleet view and publishes the
**mesh-ui artifacts** — a fixed set of static JSON documents that the canonical `mesh-ui.html` (the same
page every Benzene port vendors) fetches by relative path and renders. No UI configuration is needed.

## Prerequisites

- Python 3.10+
- `pip install benzene-mesh` (installs `benzene-core`; add `benzene-http` + `uvicorn` to serve the
  dashboard over HTTP)
- A fleet to observe: services exposing `/benzene/spec` + `/benzene/health` (see
  [Joining the mesh](joining-the-mesh.md)), and/or services pushing traces into the collector.

## 1. Feed the mesh

### Pull: poll a fleet's well-known surfaces

`MeshPoller` folds each source's spec + health into the collector. `HttpServiceSource` GET-polls
`{prefix}/spec` and `{prefix}/health` (default prefix `/benzene`); a source that is down is a failed
`PollResult`, never a broken sweep.

```python
# poll.py
import asyncio
from benzene.mesh import MeshCollector, MeshPoller, HttpServiceSource

collector = MeshCollector()
sources = [
    HttpServiceSource("orders", "https://orders.svc"),
    HttpServiceSource("inventory", "https://inventory.svc"),
]
poller = MeshPoller(collector, sources)

async def main():
    results = await poller.poll_once()           # one sweep; call on a timer for a live fleet
    for r in results:
        print(r.service, "ok" if r.ok else f"failed: {r.error}")
    fleet = collector.query_fleet({})            # the catalog now reflects both services
    print([s["service"] for s in fleet["services"]])

asyncio.run(main())
```

### Push: receive the call graph from traces

Pull can't see who calls whom — **that comes from the trace feed** the services push. The services use
`MeshFeedSender` to send `benzene:mesh:traces` (and optionally `register` / `heartbeat` / `issues`); the
collector ingests them and derives the consumer edges from the trace parentage. The collector is itself
a Benzene service — `collector_registry` wires the ingest topics onto a registry — so you can drive it
in-process:

```python
from benzene.core import BenzeneMessageApplication
from benzene.mesh import MeshCollector, collector_registry

collector = MeshCollector()
app = BenzeneMessageApplication(collector_registry(collector))

# What a service's MeshFeedSender.publish_traces(...) sends over the wire:
await app.handle({
    "topic": "benzene:mesh:traces",
    "headers": {},
    "body": '{"events": [{"traceId": "t1", "spanId": "s2", "parentSpanId": "s1",'
            ' "service": "inventory", "topic": "inventory:reserve", "status": "ok"}]}',
})
# The collector now knows orders (owner of span s1) is a consumer of inventory:reserve.
```

In a deployment the same collector polls the fleet *and* receives pushed traces — see
[step 4](#4-serve-the-dashboard) for the host that exposes both over HTTP.

## 2. Build the artifacts

`build_artifacts` projects the catalog into the mesh-ui read-model contract as plain dicts;
`write_artifacts` lays them out on disk (atomically) for the UI to fetch. Both take `sources=` (any
objects with `name` / `spec_url` / `health_url`, e.g. `HttpServiceSource`) to supply the manifest's
per-service links, and `generated_at=` (inject it for a deterministic result — it is stamped as each
artifact's `generatedAtUtc` / `fetchedAtUtc`).

```python
from datetime import datetime, timezone
from benzene.mesh import build_artifacts, write_artifacts

now = datetime.now(timezone.utc).isoformat()

artifacts = build_artifacts(collector, sources=sources, generated_at=now)   # dicts, in memory
write_artifacts("/data/mesh-ui", collector, sources=sources, generated_at=now)   # on disk
```

`write_artifacts` writes `manifest.json`, `topology.json`, `topics.json`, `usage.json`,
`asyncapi.json`, `annotations.json` at the root and one `services/{name}.json` per service. What each
artifact carries:

| Artifact | What it is |
|---|---|
| `manifest.json` | The estate: one entry per service with `name`, `status` (`healthy` / `unhealthy` / `unreachable`), `contractDrift`, and its `specUrl` / `healthUrl` links (from `sources`). |
| `topics.json` | The functional map: each topic's `producers` / `consumers`, `version`, request/response/message schemas, `reserved` flag for `benzene:*`, `schemaMismatch` (two providers declaring different contracts), and `changes` (a provider re-registered a topic with a new schema). Plus `removedTopics` — topics once declared, now provided by no one. |
| `topology.json` | The call graph: client→server `edges` derived from pushed traces, each with `errorRate`. |
| `usage.json` | Exercise counts per `(topic, service, status)` derived from traces. |
| `services/{name}.json` | Per service: `specJson`, `specHash` / `previousSpecHash`, `contractDrift`, and `health` (`isHealthy` + per-check `healthChecks`). |
| `asyncapi.json` | An AsyncAPI 3.0 export of the domain (non-reserved) topics — the UI's download / Studio deep-link. |
| `annotations.json` | An honest empty read-model (writing notes is a backend-gated live-plane feature, not on the static floor). |

**The projection never invents fields — it degrades to `null` where the pull + trace catalog can't
derive a value.** With no metrics feed, `topology.json`'s `requestsPerMinute` and `p50/p95/p99LatencyMs`
stay `null` (the UI renders those edges as reduced); `usage.json`'s `windowStartUtc` / `windowEndUtc`,
per-entry `version`, `transport`, and `avgDurationMs` stay `null`; a topic with no metrics carries a
`null` `status`. The structural counts and the fleet/topology map are still fully populated.

## 3. Refresh on a timer

The artifacts are a snapshot. Poll and re-publish on an interval so the dashboard tracks the fleet:

```python
import asyncio
from datetime import datetime, timezone
from benzene.mesh import MeshCollector, MeshPoller, HttpServiceSource, write_artifacts

collector = MeshCollector()
sources = [HttpServiceSource("orders", "https://orders.svc")]
poller = MeshPoller(collector, sources)

async def refresh_loop(interval_seconds: float = 30.0):
    while True:
        await poller.poll_once()                                  # pull the fleet
        now = datetime.now(timezone.utc).isoformat()
        write_artifacts("/data/mesh-ui", collector, sources=sources, generated_at=now)
        await asyncio.sleep(interval_seconds)
```

To keep the fleet view across a restart, give the collector a durable store:
`MeshCollector(store=JsonFileCollectorStore("/data/mesh-state.json"))` — see
[Persistence](../reference/mesh.md#persistence--collectorstore).

## 4. Serve the dashboard

You don't have to wire the loop above yourself: [`deploy/mesh`](../../deploy/mesh/README.md) ships the
**Mesh Host** — a container that runs the collector + poller, exposes the ingest/query API over HTTP,
re-publishes the artifacts after each sweep, and serves the vendored `mesh-ui.html`. One
`terraform apply` stands it up on Fargate behind an ALB, with the dashboard at `/mesh-ui/`.

### Run it locally (no AWS)

The host reads its fleet from `MESH_SERVICES` (inline JSON) or `MESH_CONFIG` (a file), and serves the
UI whenever `MESH_ARTIFACTS_DIR` is set:

```bash
# From the repo root: install the layers the host needs, plus uvicorn.
pip install -e packages/benzene-results -e packages/benzene-core \
            -e packages/benzene-http -e packages/benzene-mesh && pip install uvicorn

MESH_SERVICES='{"pollIntervalSeconds":15,"services":[{"name":"orders","baseUrl":"http://localhost:9000"}]}' \
MESH_ARTIFACTS_DIR=/tmp/mesh-ui \
PYTHONPATH=deploy/mesh python -m collector.main
```

Then read the mesh over HTTP and open the dashboard:

```bash
curl localhost:8080/benzene/health          # {"isHealthy": true, ...}
curl localhost:8080/mesh/fleet              # the polled fleet (after one sweep)
open  http://localhost:8080/mesh-ui/        # the Benzene Mesh UI
```

The host's HTTP surface: the **push** feeds are POSTs (`/mesh/register`, `/mesh/heartbeat`,
`/mesh/traces`, `/mesh/issues`) — point each service's `MeshFeedSender` (over an HTTP `MessageSender`)
at `POST <host>/mesh/traces` to feed the call graph — and the **query** read models are GETs
(`/mesh/fleet`, `/mesh/service/{service}`, `/mesh/topic/{topic}`, `/mesh/trace/{traceId}`). Setting
`MESH_ARTIFACTS_DIR` is what enables `/mesh-ui/`; leave it unset to run the API alone.

## Troubleshooting

- **`/mesh-ui/` 404s or the page loads but shows no data.** The UI mount only exists when
  `MESH_ARTIFACTS_DIR` is set, and it serves only generated `*.json` artifacts from that directory.
  Make sure the host has completed at least one poll sweep (or that you called `write_artifacts`) so the
  artifacts exist before the UI fetches them.
- **The fleet is empty after a sweep.** A source that is down is a failed `PollResult`, not an error —
  inspect `poll_once()`'s results for the `error`. Check each `baseUrl` actually serves
  `/benzene/spec` + `/benzene/health` (the `prefix` is configurable per source).
- **Topology edges are missing.** Pull can't derive the call graph — the edges come from **pushed
  traces**. Confirm your services enable `trace_middleware`, propagate `traceparent` on outbound calls
  (`with_trace_propagation`), and POST their trace batches to the collector.
- **Latency / rate numbers are blank.** Expected: those need a metrics feed the pull + trace catalog
  doesn't have, so they degrade to `null` by design rather than being fabricated.
- **The fleet resets on restart.** The collector is in-memory by default. Pass a
  `JsonFileCollectorStore` (the deploy host sets `MESH_STORE_PATH` for you) to rehydrate on boot.

## See also

- [`benzene.mesh` reference](../reference/mesh.md) — every type, signature, and artifact field.
- [Joining the mesh](joining-the-mesh.md) — the **sending** side: make a service report in.
- [Deploying the mesh collector to AWS (Fargate)](../../deploy/mesh/README.md) — the containerised Mesh
  Host and its Terraform.
- [mesh specification](https://github.com/daniellepelley/Benzene/blob/main/docs/specification/mesh.md)
  — the language-neutral contract these wire shapes and artifacts implement.
```
