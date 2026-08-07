# `deploy/mesh` — the distributed Benzene Mesh Host

The **multi-process** shape of [`examples/mesh_fleet`](../../examples/mesh_fleet). Where the example
builds an in-process fleet and hands the emitter its inputs directly, this stack runs the **Mesh Host**
and the three domain services as **separate ASGI apps on real localhost sockets**, and the fleet data
reaches the collector over **genuine HTTP feed pushes between processes** — then the *host itself* serves
the mesh UI.

This is the Python answer to .NET's [`deploy/Mesh/Benzene.Mesh.Host`](https://github.com/daniellepelley/benzene-dotnet/tree/main/deploy/Mesh/Benzene.Mesh.Host):
one service that is **collector + aggregator + UI**, driven by a background poll loop for the
local/compose case (and a `mesh:aggregate` trigger seam for a hosted/scheduled case).

## What the Mesh Host is

`benzene.mesh.host.MeshHost` (in `benzene-mesh[host]`) is an ASGI app composing three roles:

1. **the collector, networked** — `collector_service_app` wraps a `MeshCollector` behind the profile's
   `/benzene/invoke`, so any service reports in by POSTing a wire envelope: the ingest feeds
   (`benzene:mesh:register` / `heartbeat` / `traces` / `issues`) and the `benzene:mesh:query:*` read
   models alike (`benzene.http.InvokeMessageSender` is the outbound counterpart);
2. **the aggregator** — `benzene.mesh.aggregator.MeshAggregator.run_once(registry, out_dir=…)` HTTP-GETs
   each registered service's `/benzene/spec` + `/benzene/health` (tolerating unreachable → an `error`
   snapshot), queries the co-hosted collector, and runs `MeshArtifactEmitter.emit(out_dir)`;
3. **the UI server** — every non-`/benzene/*` GET serves a file from `out_dir` (`/` → the vendored
   `mesh-ui.html`), so the page's relative `fetch("manifest.json")` resolves against the host, same-origin.

`run_poll_loop` drives `run_once` on a timer (a failed pass is logged, never crashes the host);
`aggregate_handler` exposes the same one pass as a `mesh:aggregate` handler for a scheduler.

## How a service pushes feeds to the host

Each service (`services.py`) is a `BenzeneHttpApp` with trace middleware and its `/benzene/*` surfaces,
plus a `MeshFeedSender` over an `InvokeMessageSender` pointed at the **host's** `/benzene/invoke`:

```python
feeds = MeshFeedSender(InvokeMessageSender(lambda _topic: f"{host_url}/benzene/invoke"))
await feeds.register(descriptor)                       # → HTTP POST envelope to the host
await feeds.publish_heartbeat(Heartbeat(...))          # → HTTP POST envelope to the host
await feeds.publish_traces(exporter.drain())           # → HTTP POST envelope to the host
```

Domain calls between services use the same `InvokeMessageSender` (wrapped in
`TracePropagatingMessageSender`, so the caller's mesh span rides in the envelope headers) — that
cross-process trace parentage is what lets the collector derive the `orders → payments` edge.

## The three demonstrated states (over real HTTP)

| Service | Manifest state | How |
|---|---|---|
| `orders` | **healthy** | `/benzene/health` reports all checks healthy |
| `payments` | **unhealthy + drift** | `/benzene/health` → 503 (gateway check fails); seeded previous spec hash differs |
| `shipping` | **unreachable** | deliberately exposes no `/benzene/spec` or `/benzene/health` (they 404) — yet still registers, heartbeats, receives calls, and traces into the collector |

## Run the proof

Brings the four-app stack up on ephemeral ports, drives `orders` traffic over HTTP, lets every service
push its feeds to the host, runs one aggregation pass, then drives **headless Chromium** against the
**host's** UI URL, asserts the live DOM (three services + health/drift/unreachable + the
collector-derived `orders → payments` edge), and screenshots to the repo root:

```bash
PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers \
PYTHONPATH=packages/benzene-results:packages/benzene-core:packages/benzene-http:packages/benzene-mesh:examples:deploy \
  python -m mesh.prove
# -> ./mesh-host-proof.png
```

The browser-free integration test (same stack, socket-only, no Chromium) runs in the normal suite:

```bash
pytest deploy/mesh
```

## Layout

| File | What it is |
|---|---|
| `asgi_server.py` | A stdlib-only asyncio HTTP/1.1 server — just enough to run the ASGI apps over real sockets (demo-grade; a real deployment uses uvicorn/hypercorn) |
| `services.py` | The three domain services as separate ASGI apps, wired to push feeds + call peers over HTTP (domain reused from `mesh_fleet.domain`) |
| `stack.py` | Starts all four apps, wires them, drives traffic, runs one aggregation pass → `run_stack()` |
| `prove.py` | Brings the stack up on a background loop, drives Chromium against the host UI, asserts + screenshots |
| `test_mesh_host_stack.py` | Integration test: the distributed fleet reaches the host and renders the three states (no browser) |

## Hosting for real

The demo runs the ASGI apps under `asgi_server.py` because this environment ships no ASGI server. In a
real deployment each app is an ordinary ASGI application — run the host with
`uvicorn deploy.mesh.host_module:app` (or Hypercorn), bind-mount the artifact `out_dir` for persistence,
and either keep the background poll loop or trigger `mesh:aggregate` from a scheduler.

## Deferred / out of scope for this piece

- **HTTP mappings in the distributed catalog.** The aggregator fetches each peer's *transport-neutral*
  `/benzene/spec`, which carries topics + schemas but **not** the service's HTTP route table, so
  `topics.json` `consumers[].httpMappings` are empty and an unversioned HTTP topic reads as `gap` in the
  UI. The in-process `mesh_fleet` demo fills mappings from the router directly. Surfacing routes over the
  wire would be a spec change (extend `ServiceSpec`) — not made here.
- **A single collector process.** The collector's catalog is in-memory in the one host process (the
  aggregator queries the same object the HTTP feeds mutate). Running multiple host replicas would need a
  shared collector store — out of scope; this demo is single-collector by design.
- **Auth between service↔collector, and X-Ray/CloudWatch enrichment** — explicitly out of scope for this
  piece.
