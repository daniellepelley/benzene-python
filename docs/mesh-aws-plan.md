# Plan: a Benzene-Python mesh, deployed and working in AWS

The goal: a multi-service Benzene-Python mesh deployed to AWS that matches the scope of the .NET
[`benzene-dotnet/deploy/Mesh`](https://github.com/daniellepelley/benzene-dotnet/tree/main/deploy/Mesh)
(a central Mesh Host that discovers a fleet, builds a topology, and shows it in a UI). This is the
port's first run on real infrastructure.

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

**Phase 0 — prove the port runs on real AWS (foundational; never done).**
Deploy *one* `benzene-aws` service to Lambda behind API Gateway with Terraform; hit its live
`/benzene/spec` + `/benzene/health` + a handler. Flushes out packaging the PEP 420 layers into a Lambda
zip, boto3 clients against real AWS, IAM, cold start. *Nothing downstream is trustworthy until this is
green.* — needs AWS credentials.

**Phase 1 — a real fleet.** 2–3 related services (`orders` → `inventory` → `notifications`) deployed to
Lambda, each with `StandardPaths` + `trace_middleware` + traceparent propagation, and pushing traces to
the collector (so the graph has real consumer edges). Built on the shared example domain. *(Fleet
example code is AWS-independent and can land first; deploy needs Phase 0.)*

**Phase 2 — the Fargate collector.** Package the collector as a container: the `MeshPoller` on a timer
(pull spec+health) **plus** an ingest endpoint for pushed traces (SNS→SQS→collector), a durable store
(a `CollectorStore` seam over the mounted volume — see task), and an HTTP surface exposing `query:*`.
Terraform for the Fargate service, task role, and the volume/EFS.

**Phase 3 — topology + UI.** Emit `manifest.json` / per-service JSON / `topology.json` in the shape the
main-repo `mesh-ui` renders (**open item**: inspect `mesh-ui`'s expected schema and add an artifact
writer / adapter; the collector's query models are close but not confirmed byte-compatible). Serve the
UI (static assets from the collector container or S3+CloudFront).

**Phase 4 — verify live.** An integration/smoke test: `terraform apply`, drive traffic across the
fleet, assert the mesh shows the full fleet + edges + health, then `terraform destroy`. A runbook, and
optionally a CI deploy workflow (OIDC to AWS, gated/manual).

## What I can build vs. what needs you

- **I can author (no AWS needed):** the poller (done), the fleet example + tests, the `CollectorStore`
  seam, the Fargate container entrypoint, all Terraform, the artifact writer, a deploy runbook, and a
  CI deploy workflow skeleton.
- **Needs you:** an AWS account + credentials (I can't `terraform apply` from here); confirmation of
  the `mesh-ui` artifact schema (and whether `mesh-ui` should stay in the main repo or be vendored);
  and the AWS region/naming conventions.

## Open items / risks

- **mesh-ui artifact compatibility** — the single biggest unknown. Reusing the UI cleanly means the
  Python collector emits exactly what `mesh-ui` reads. Needs a look at the main-repo `mesh-ui`.
- **Consumer edges need traces** — pull gives identity/topics/health; the call graph needs pushed
  traces. The fleet pushes traces to the collector (hybrid), which the collector already supports.
- **Collector persistence** — Fargate + volume mirrors .NET; an `CollectorStore` seam keeps the
  in-memory default for tests and swaps to the volume in the container.
- **First-ever real AWS run** — Phase 0 is the real risk retirement; do it before the mesh phases.
