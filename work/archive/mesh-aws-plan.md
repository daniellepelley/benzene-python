> ARCHIVED 2026-08-20: actioned; `deploy/mesh/` (Terraform + Fargate collector + Lambda fleet, runbook in `deploy/mesh/README.md`) shipped, and — beyond this plan's own status text — Phase 4's CI automation shipped too (`.github/workflows/deploy-mesh.yml` + `destroy-mesh.yml`). The one live remainder (move those workflows from static keys to OIDC role assumption) is tracked in `work/remaining-items.md`.

# Plan: a Benzene-Python mesh, deployed and working in AWS

The goal: a multi-service Benzene-Python mesh deployed to AWS that matches the scope of the .NET
[`benzene-dotnet/deploy/Mesh`](https://github.com/daniellepelley/benzene-dotnet/tree/main/deploy/Mesh)
(a central Mesh Host that discovers a fleet, builds a topology, and shows it in a UI). This was the
port's first run on real infrastructure.

> **Status: built and verified live.** Phases 0–3 are done, and Phase 4 has been run by hand. The
> only item still open is Phase 4's **CI automation** — a gated, OIDC-authenticated deploy workflow.
> `deploy/mesh` stands the whole thing up with one `terraform apply`; see
> [`deploy/mesh/README.md`](../deploy/mesh/README.md).

## Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Aggregator model | **Thin poller** — a pull aggregator polling each service's `/benzene/spec` + `/benzene/health`, feeding the existing `MeshCollector`. |
| 2 | Collector runtime | **Fargate** (a long-running container with a volume), mirroring the .NET Mesh Host. |
| 3 | UI | **Reuse the existing `mesh-ui`** (lives in the main `Benzene` repo; language-neutral). The Python collector emits artifacts in the shape `mesh-ui` renders. |
| 4 | Infrastructure-as-code | **Terraform**. |
| 5 | AWS deployment | **Set it up** — author everything so a single `terraform apply` (with credentials) stands the stack up. |

## The .NET yardstick

`Benzene.Mesh.Host`: a container that **polls** a fleet on a timer (`MeshAggregator` /
`MeshPollBackgroundService`) via `HttpMeshServiceSource` / `LambdaMeshServiceSource` (plus self-report
at `/mesh/report`), serves a **UI at `/mesh-ui`**, and persists artifacts to a volume
(`manifest.json`, per-service JSON, `topology.json`). Config via `mesh.json`.

## What the port already has (leverage)

- **Pollable services**: every host exposes `/benzene/spec` + `/benzene/health` via `StandardPaths`.
- **The collector**: `MeshCollector` ingests register/heartbeat/traces/issues and answers
  `query:fleet/service/topic/trace` — with consumer-edge topology, health, hash-drift, missing-feeds.
- **The poller** (done — `benzene.mesh.MeshPoller`): pulls a fleet's spec+health into the collector.
- **AWS host**: `benzene-aws` (API Gateway + SQS + SNS + egress) — services can target Lambda.
- **Trace propagation**: `trace_middleware` + `with_trace_propagation` for the call graph.

## The gap → phased build

**Phase 0 — prove the port runs on real AWS (foundational).** *(done.)* `benzene-aws` services run
on Lambda behind API Gateway from Terraform, with live `/benzene/spec` + `/benzene/health` + handlers
answering. It flushed out exactly what it was there to flush out: packaging the PEP 420 layers into a
Lambda zip, boto3 against real AWS, IAM, cold start. The whole mesh — fleet on Lambda plus collector
on Fargate — now stands up from one `terraform apply` ([`deploy/mesh`](../deploy/mesh)) and was
verified live end to end (estate, functional map, topology, usage) before being torn down.

**Phase 1 — a real fleet.** 2–3 related services (`orders` → `inventory` → `notifications`) deployed to
Lambda, each with `StandardPaths` + `trace_middleware` + traceparent propagation, and pushing traces to
the collector (so the graph has real consumer edges). Built on the shared example domain. *(done —
`deploy/mesh/fleet`: `orders` → `inventory` → `notifications`, deployed by the same `terraform apply`
and pointed at the collector automatically.)*

**Phase 2 — the Fargate collector.** *(done.)* The collector is packaged as a container: the
`MeshPoller` on a timer (pull spec+health) **plus** HTTP ingest routes for pushed traces/register/
heartbeat/issues, and an HTTP surface exposing `query:*`. Persistence is a `CollectorStore` seam —
`NullCollectorStore` (in-memory default, for tests) and `JsonFileCollectorStore` (atomic JSON snapshot
on a mounted volume); the collector restores on boot and saves after each ingest. Terraform stands up
the Fargate service, task role, and an EFS volume mounted at `/data` (gated on `persist_state`), so a
replaced task rehydrates the fleet it already knew.

**Phase 3 — topology + UI.** *(done.)* `benzene.mesh.artifacts` projects the collector catalog into the
cross-language read-model artifacts the canonical `mesh-ui.html` renders — `manifest.json`,
`topology.json`, `topics.json`, `services/{name}.json` — matching the contract in the main repo's
`docs/guides/mesh-ui.md` (pinned by `website/demos/mesh/`). The Fargate host republishes them after each
poll sweep and serves them, plus the vendored UI, under `/mesh-ui/`. The collector retains the
descriptor's per-topic **schemas + version**, a **previousSpecHash** across contract changes, and the
heartbeat's **healthChecks**, so the functional map (schemas, schema-mismatch), per-service (spec +
per-check health + drift history), and **usage.json** (from traces) all populate. Only true
observability metrics (latency/rate, usage window/transports) and annotations degrade — they need
feeds this collector doesn't have.

**Phase 4 — verify live.** *(partly done — the only phase still open.)* The apply → drive traffic →
assert full fleet + edges + health → destroy loop has been run by hand and the runbook is in
[`deploy/mesh/README.md`](../deploy/mesh/README.md). What is left is the **CI automation**: a gated,
manually-dispatched deploy workflow authenticating to AWS over OIDC, so the loop runs on demand rather
than from a maintainer's shell.

## What is left

Every risk this plan opened with has been retired: the mesh-ui artifact contract is
`docs/guides/mesh-ui.md` in the main repo (pinned by `website/demos/mesh/`) and
`benzene.mesh.artifacts` emits it; consumer edges come from traces the fleet pushes; collector
persistence is the `CollectorStore` seam with an EFS-backed JSON snapshot on Fargate; and the
first-ever real AWS run happened, which is what closed Phase 0.

The one open item is **Phase 4's CI automation** — a gated, manually-dispatched deploy workflow
authenticating over OIDC — and it still needs an AWS account, its region/naming conventions, and
credentials this repository does not hold.
