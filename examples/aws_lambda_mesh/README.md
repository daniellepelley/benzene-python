# AWS Lambda Mesh — self-discovery, end to end

The Python counterpart of .NET's [`examples/AwsMesh`](https://github.com/daniellepelley/benzene-dotnet/tree/main/examples/AwsMesh)
and TypeScript's [`examples/aws-lambda-mesh`](https://github.com/daniellepelley/benzene-typescript/tree/main/examples/aws-lambda-mesh):
**six** Benzene Cloud Service Lambdas that call each other over **SQS, SNS and EventBridge**, plus a
**mesh** (a seventh Lambda) that discovers them by tag via the real AWS `ListFunctions`/`ListTags` APIs,
interrogates each over a synchronous **direct Lambda Invoke**, and publishes the aggregated catalog to
S3 — the same estate shape as the .NET/TypeScript ports, so the Mesh UI renders an identical topology
across all three languages.

This is the AWS-deployable mesh example; [`examples/k8s_mesh`](../k8s_mesh) proves the same
discover-interrogate-aggregate story on Kubernetes (label-based discovery, plain HTTP interrogation).
Both dogfood the same transport-agnostic `benzene.mesh` core (`MeshPoller`, `MeshCollector`,
`build_artifacts`) — only discovery and interrogation differ per substrate.

## The estate

```
  orders --payments:capture (SQS)--> payments --shipping:book (SQS)--> shipping
    |                                    |                                 |
    +--order:placed (SNS, fan-out)-----> +--payment:captured (EventBridge)-+--shipment:dispatched (EventBridge)-->
    v            v                       v            v                             v          v          v
 inventory  notifications           notifications  analytics                   inventory  notifications  analytics
```

Each of the six services is **one Lambda**, tagged `benzene=true` for discovery, and:

- answers a **direct Lambda invoke** on the two reserved topics the mesh interrogates —
  `benzene:mesh` (the derived `ServiceDescriptor`, via `benzene.mesh.mesh_interception`) and
  `benzene:healthcheck` (the health aggregate, via `benzene.core.health_interception`) — the AWS
  analogue of TS's `useSpec`/`useHealthCheck` reserved topics and .NET's `spec`/`healthcheck` invoke
  handling. No HTTP round trip needed for interrogation;
- is also fronted by its own HTTP API (API Gateway v2), so `/benzene/invoke`, `/benzene/health`, and
  `/benzene/spec` (`benzene.http.StandardPaths`) answer over HTTP too — orders additionally exposes
  `POST /orders`, the front door of the chain;
- sends its produced topics through the transport Terraform wires for it (SQS/SNS/EventBridge) via a
  small per-topic outbound router (`service/host.py`'s `TopicRoutingMessageSender`) — a single POST to
  `/orders` therefore genuinely cascades through the whole estate on a real deploy;
- runs `benzene.mesh.trace_interception` outermost (`service/startup.py`) and wraps that router in
  `benzene.mesh.with_trace_propagation`, so a downstream hop joins the caller's trace; after each
  invocation it drains the batch and **pushes it straight into the mesh's trace inbox in S3**
  (`benzene.mesh.MeshFeedSender` + `benzene.mesh.S3TraceInbox`, via the `MESH_ARTIFACT_BUCKET` env var
  Terraform sets) — the collector derives **consumer edges** from those traces, exactly as
  `examples/k8s_mesh`'s services do over HTTP, just pushed once per invocation rather than on a
  background loop (a Lambda only runs *during* an invocation; mirrors `deploy/mesh/fleet/service.py`).

The **mesh Lambda** (`mesh/main.py`, **not** tagged for discovery — it never interrogates itself) fires
on the EventBridge schedule (default every 5 minutes) or an on-demand invoke, running discover →
interrogate → drain → publish:

- **discover** — `benzene.mesh_fleet.AwsLambdaDiscovery` (real `list_functions` + `list_tags`,
  paginated, filtered by the `benzene` tag);
- **interrogate** — one `benzene.mesh.CallableServiceSource` per discovered function
  (`mesh/discovery_service.py`'s `lambda_service_source`), backed by `benzene.aws.LambdaMessageSender`
  invoking `benzene:mesh` and `benzene:healthcheck` on each — fed into a real
  `benzene.mesh.MeshPoller`/`MeshCollector`, the same transport-agnostic core every Benzene mesh uses;
- **drain** — every trace batch pushed into the trace inbox (above) since the last pass
  (`benzene.mesh.S3TraceInbox.drain`), merged into the same collector this pass just polled;
- **publish** — the discovered registry (`registry.json`) and the full catalog (`manifest.json`,
  `topology.json`, `topics.json`, `usage.json`, `asyncapi.json`, `annotations.json`,
  `services/{name}.json`) to S3 via `benzene.mesh.S3ArtifactStore` / `write_artifacts_to_s3`.

This pass is the collector's **sole writer**, and that's deliberate, not incidental: the durable
collector snapshot lives in S3 (`benzene.mesh.S3CollectorStore`, `MESH_STATE_KEY`), and one order
fanning out over SNS/EventBridge invokes *several* service Lambdas at once, each independently pushing
a trace batch. An earlier version of this example had each push do a load-mutate-save straight against
that shared snapshot — which **races** under exactly that fan-out condition (not a rare edge case; the
normal shape of a single order): whichever push saves last wins, silently discarding whatever the
other wrote in between, so a topic could show a provider but never all of its consumers. Splitting the
push (many independent, uniquely-keyed S3 objects — nothing to contend over) from the merge (one
aggregation pass at a time, the only writer of the shared snapshot) removes the race entirely instead
of adding locking/retries around it. See "Framework additions" below and
`mesh/discovery_service.py`'s module docstring for the full story.

## Framework additions

Small, additive changes to the framework packages made this example possible — all confirmed-real gaps,
all covered by their own unit tests:

- **`benzene.mesh_fleet.AwsLambdaDiscovery`** (`packages/benzene-mesh-fleet/.../discovery_adapters.py`)
  — a `Discovery` adapter alongside `AwsCloudMapDiscovery`/`AzureDiscovery`/`KubernetesDiscovery`:
  paginated `list_functions` + per-function `list_tags`, filtered by a configurable tag key (default
  `"benzene"`). Unlike the network-addressed adapters, a discovered `ServiceEndpoint`'s `address` is the
  Lambda **function name** (what `LambdaMessageSender` needs), not a URL — a Lambda has no HTTP endpoint
  of its own.
- **`benzene.mesh.S3ArtifactStore` / `write_artifacts_to_s3`** (`packages/benzene-mesh/.../s3_artifacts.py`)
  — the S3 counterpart of the existing local-filesystem `write_artifacts`: same `(collector, sources,
  generated_at)` shape, publishing the identical document set as S3 objects under a bucket + prefix
  instead of files. `S3ArtifactStore.write(key, document)` is generic, so the mesh Lambda also uses it to
  publish `registry.json` (the discovered config) alongside the catalog — mirroring TS's
  `store.publishAsync('registry.json', ...)`.
- **`benzene.mesh.S3CollectorStore`** (`packages/benzene-mesh/.../store.py`) — the S3 counterpart of the
  existing `JsonFileCollectorStore` (EFS-backed, for the Fargate/K8s collectors): one JSON object holds
  the collector's whole snapshot, loaded on construction and saved after every ingest, same
  `CollectorStore` protocol. It exists because a Lambda-based mesh has no persistent disk of its own
  *and* no long-lived process to keep the catalog in memory between the separate, stateless invocations
  that build it up — the seam every other Benzene mesh gets for free from a long-running host. **Only
  the mesh Lambda's own aggregation pass ever touches it**, so it stays single-writer.
- **`benzene.mesh.S3TraceInbox`** (`packages/benzene-mesh/.../inbox.py`) — a race-free push seam for
  concurrent, stateless writers: each push (a service Lambda's `MeshFeedSender.publish_traces`) is one
  independent `PutObject` to a **uniquely keyed** object, so pushes fanned out from the same message
  never contend. Implements `MessageSender` (drop-in for `MeshFeedSender`, same as `LambdaMessageSender`/
  `HttpMessageSender` elsewhere in this port); a single reader's `drain()` lists, reads, and deletes
  every object in one pass — meant for the aggregation pass alone, never a concurrent drain.

All three are purely additive: the local path (`JsonFileCollectorStore`, plain `write_artifacts`) is
untouched.

Both are optional-`boto3` (the `[aws]` extras on `benzene-mesh-fleet` and `benzene-mesh`), lazily
imported, and constructor-injectable — exactly the convention every other AWS binding in this port
follows (`benzene.aws.clients`).

## Two catalog identities, on purpose

`registry.json` and the rest of the catalog name services differently, and that's intentional, not a
bug: **`registry.json`** reflects *discovery-time* identity — the Lambda **function names**
Terraform assigns (`benzene-python-awsmesh-orders`, …). The **catalog** (`manifest.json`,
`services/{name}.json`, `topics.json`, …) reflects *interrogation-time* identity — each service's own
declared `service` field from its `benzene:mesh` response (`orders`, `payments`, …), exactly as
`MeshPoller` already resolves it for every other discovery mechanism in this port
(`spec.get("service") or source.name`). A function could be renamed in Terraform without the mesh
identity changing, or vice versa — the two are deliberately decoupled.

## Deploying to a real AWS account

```bash
examples/aws_lambda_mesh/deploy/build_service.sh   # -> deploy/build/service.zip (one shared zip, all six domains)
examples/aws_lambda_mesh/deploy/build_mesh.sh      # -> deploy/build/mesh.zip
cd examples/aws_lambda_mesh/deploy
terraform init -backend-config=... # see .github/workflows/deploy-aws-lambda-mesh.yml for the exact S3 backend config
terraform apply
```

Terraform provisions: the six tagged service Lambdas + one HTTP API each (API Gateway v2, `$default`
AWS_PROXY), the mesh Lambda (untagged, no HTTP API — driven by its own EventBridge schedule, reading
what the six services pushed to S3), the two SQS queues + event-source mappings, the SNS topic +
subscriptions, the custom EventBridge bus + rules + targets, IAM (a shared service execution+messaging
role — SQS/SNS/EventBridge sends, plus `s3:PutObject` scoped to just the trace-inbox prefix
(`_state/traces/*`) for pushing trace batches; a mesh role scoped to
`lambda:ListFunctions`/`lambda:ListTags`/`lambda:InvokeFunction` on the six service ARNs +
`s3:GetObject`/`PutObject`/`ListBucket`/`DeleteObject` on the artifacts bucket, covering the published
catalog, the durable collector snapshot, and draining the trace inbox), and the S3 artifacts bucket.

**Static viewer, not a live HTTP surface on the mesh Lambda.** Following TS's precedent exactly: the
bucket is configured as an **S3 static website** (`aws_s3_bucket_website_configuration` +
`aws_s3_bucket_public_access_block` + a public-read bucket policy scoped to `mesh/*`), and Terraform
uploads the **canonical, already-vendored `mesh-ui.html`** (`web/index.html` here — the identical file
`examples/k8s_mesh/mesh/ui/mesh-ui.html` already vendors in this repo) as `mesh/index.html`, right next
to the catalog the mesh Lambda writes. `http://<bucket-website-endpoint>/mesh/` therefore serves the
full Mesh UI reading the real catalog with same-origin relative fetches — no server, no BenzeneHttpApp
wiring on the mesh Lambda at all. The alternative (serving the UI live off the mesh Lambda's own HTTP
surface) would need a whole extra API Gateway + `BenzeneHttpApp` + static-asset route for a Lambda that
otherwise never needs one; the static site gives the identical UI for a fraction of the moving parts.

See `deploy/main.tf` for the full resource list (modelled closely on TS's `deploy/main.tf`) and
`.github/workflows/deploy-aws-lambda-mesh.yml` for the apply/plan/destroy dispatch. To tear the stack
down without switching that dropdown, run
[`Destroy AWS Lambda Mesh Example`](../../.github/workflows/destroy-aws-lambda-mesh.yml) instead —
**Actions → Destroy AWS Lambda Mesh Example → Run workflow** with the defaults is enough, no typed
confirmation phrase needed. It shares the deploy workflow's S3 state, so it tears down anything that
workflow created; if nothing was ever deployed in the target account/region, it's a no-op.

## Known first-deploy iteration points

- **Base runtime** — `deploy/variables.tf`'s `lambda_runtime` defaults to `python3.12`; bump it if your
  account's Lambda console/tooling expects a different minor version.
- **Cold starts** — this is a demo-proportionate estate (256 MB, 30s timeout); tune `deploy/main.tf`'s
  `memory_size`/`timeout` for a real workload.

## Tests

```bash
pytest examples/aws_lambda_mesh/tests -q
```

`tests/test_services.py` boots each of the six domains from `ServiceStartUp` through
`create_test_host(...).build_aws()` (the same in-memory AWS test harness every AWS example in this repo
uses), faking only the outbound `MessageSender` — proving ingress -> handler -> egress over the *native*
event shapes each service actually receives (API Gateway, SQS, SNS, EventBridge), and that the two
reserved topics (`benzene:mesh` / `benzene:healthcheck`) really do answer a direct invoke.
`tests/test_mesh.py` drives the real `run_mesh_aggregation` against a fake `Discovery` and a fake
`boto3` Lambda client that routes `invoke()` straight to each service's real, in-memory `AwsLambdaApp` —
proving discover -> interrogate -> collector -> S3 catalog end to end, with no cloud account. It also
covers the fix that motivated `S3TraceInbox`: one test pushes a trace batch between two aggregation
passes and asserts it survives into the next published catalog's consumers; another pushes **two**
independently (simulating an SNS/EventBridge fan-out invoking two service Lambdas at once) and asserts
*both* survive into the same pass — the exact scenario a shared-state push used to lose one of.

## Projects

| Path | What it is |
|---|---|
| `service/` | the six-domain composition root — `domain.py` (handlers + health checks), `startup.py` (`ServiceStartUp`, picks the domain by `SERVICE_NAME`), `host.py` (env-driven AWS wiring + `TopicRoutingMessageSender`), `main.py` (the Lambda entry point) |
| `mesh/` | the discovery + interrogation + S3-publishing aggregator — `discovery_service.py` (`run_mesh_aggregation`, `lambda_service_source`), `main.py` (the Lambda entry point) |
| `web/index.html` | the vendored canonical Mesh UI (same file as `examples/k8s_mesh/mesh/ui/mesh-ui.html`), uploaded by Terraform as the S3 static-website viewer |
| `deploy/` | Terraform (modelled on TS's `deploy/main.tf`) + `build_service.sh`/`build_mesh.sh` (the two Lambda zips) |
| `tests/` | in-memory, dogfooded tests (`create_test_host` for the six services; a fake `Discovery` + fake `boto3` Lambda client for the mesh) |
| `../../.github/workflows/deploy-aws-lambda-mesh.yml` | apply / plan / destroy, dispatched |
| `../../.github/workflows/destroy-aws-lambda-mesh.yml` | one-click teardown — no dropdown, no confirmation phrase |
