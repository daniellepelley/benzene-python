# Deploying the Benzene mesh collector to AWS (Fargate)

The **Mesh Host** — the Benzene-Python analogue of .NET's `Benzene.Mesh.Host` — runs the
[`MeshCollector`](../../packages/benzene-mesh) and the [`MeshPoller`](../../docs/reference/mesh.md),
polling a fleet's `/benzene/spec` + `/benzene/health` on a timer and serving the mesh over HTTP. This
directory ships it as a container (`collector/`) and the Terraform to run it on Fargate behind an ALB
(`terraform/`).

> **Status.** Complete and validated locally (the host, the fleet service, the artifact projection, and
> the static UI serving are unit-tested — `collector/tests`, `fleet/tests`, `tests/test_mesh_artifacts`).
> One `terraform apply` stands up the whole mesh: the fleet on Lambda + the collector on Fargate, wired
> together, **and the [Mesh UI](#the-mesh-ui) served from the collector** at `/mesh-ui/`. See
> the archived plan, [`work/archive/mesh-aws-plan.md`](../../work/archive/mesh-aws-plan.md).

## What gets created

- **The collector**: an ECR repository, an ECS Fargate cluster + service (one collector task), a public
  Application Load Balancer (port 80 → container 8080), security groups, IAM roles, a CloudWatch log
  group — in the account's **default VPC**.
- **The fleet** (`deploy_fleet=true`, the default): three Lambdas (`orders` → `inventory` →
  `notifications`, all from one zip, env-selected) each behind an HTTP API Gateway, plus their IAM role.
  The collector is pointed at their URLs automatically; each service registers what it produces (so the
  collector's provider edges exist with zero traffic) and calls the next with trace propagation,
  pushing traces back to feed those edges' invocation/error stats.

Roughly the cost of one small Fargate task + an ALB while it runs (the Lambdas + HTTP APIs are
pay-per-request). Set `-var="deploy_fleet=false"` to deploy just the collector.

## Prerequisites

- An AWS account and credentials (`aws sts get-caller-identity` works).
- `terraform` ≥ 1.5, `docker`, and the `aws` CLI.
- A region (default `eu-west-1`).

## Try it locally first (no AWS)

```bash
# From the repo root — run the host against an inline fleet (any pollable Benzene service URLs):
pip install -e packages/benzene-results -e packages/benzene-core -e packages/benzene-http \
            -e packages/benzene-mesh && pip install uvicorn
MESH_SERVICES='{"pollIntervalSeconds":15,"services":[{"name":"demo","baseUrl":"http://localhost:9000"}]}' \
  PYTHONPATH=deploy/mesh python -m collector.main
# then: curl localhost:8080/benzene/health ; curl localhost:8080/mesh/fleet
```

## Deploy with a GitHub task (no laptop)

The [`Deploy Mesh (AWS)`](../../.github/workflows/deploy-mesh.yml) workflow does everything below from
a runner. It reads AWS credentials from the repo's `test` environment (the access key id is an
environment *variable*, the secret an environment *secret*), populated by the main Benzene repo's
**Sync Test Environment** workflow. Terraform state goes to an auto-created, versioned S3 bucket so a
later run can tear the stack down.

- **Actions → Deploy Mesh (AWS) → Run workflow**, pick `action` = `apply` (or `plan` / `destroy`).
- The run's summary prints the mesh URL and fleet URLs when it finishes.
- `destroy` removes everything it created (state is durable in S3).

To tear the stack down without switching that dropdown, use
[`Destroy Mesh (AWS)`](../../.github/workflows/destroy-mesh.yml) instead — **Actions → Destroy Mesh
(AWS) → Run workflow** with the defaults is enough; it needs no typed confirmation phrase, unlike some
teams' destroy pipelines. It shares the same S3 state as Deploy Mesh (AWS), so it can tear down
anything that workflow created. Match its `deploy_fleet` input to however the stack was last deployed
(default `true`). If nothing was ever deployed in the target account/region, it's a no-op.

The rest of this file is the same flow by hand.

## Deploy (by hand)

Terraform creates the ECR repo, but the ECS service needs an image to exist first — so create the repo,
push the image, then apply the rest.

```bash
# 0. Build the fleet Lambda zip (from the repo root).
deploy/mesh/fleet/build.sh          # -> deploy/mesh/build/fleet.zip

cd deploy/mesh/terraform
terraform init

# 1. Create just the ECR registry (the ECS service needs an image to exist first).
terraform apply -target=aws_ecr_repository.collector -var="collector_image=placeholder"
REPO=$(terraform output -raw ecr_repository_url)
REGION=${AWS_REGION:-eu-west-1}

# 2. Build the collector image (context = repo root, needs packages/ + deploy/) and push it.
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${REPO%/*}"
docker build -f ../collector/Dockerfile -t "$REPO:v1" ../../..
docker push "$REPO:v1"

# 3. Apply the whole mesh — fleet Lambdas + collector, wired together.
terraform apply -var="collector_image=$REPO:v1"
```

That's it: the fleet deploys, the collector is pointed at the fleet's own API Gateway URLs, and the
fleet's `COLLECTOR_URL` points at the ALB. To deploy the collector against an *external* fleet instead,
pass `-var="deploy_fleet=false" -var='mesh_services_json=...'` (see
[`collector/mesh.json.example`](collector/mesh.json.example) for the shape).

## Verify

```bash
MESH=$(terraform output -raw mesh_url)
curl "$MESH/benzene/health"     # {"isHealthy": true, ...}   (the ALB health check hits this)
curl "$MESH/mesh/fleet"         # the polled fleet: orders, inventory, notifications (after one poll)

# Drive an order through the fleet to generate cross-service traces, then read the edges back:
ORDERS=$(terraform output -json fleet_urls | python -c 'import json,sys;print(json.load(sys.stdin)["orders"])')
curl -X POST "$ORDERS/orders" -d '{"sku":"ABC"}'
curl "$MESH/mesh/topic/inventory:reserve"   # providers: [orders], consumers: [inventory]
curl "$MESH/mesh/topic/notify:send"         # providers: [inventory], consumers: [notifications]
```

Open the dashboard in a browser: `terraform output -raw mesh_ui_url` (i.e. `$MESH/mesh-ui/`).

Logs: `aws logs tail "$(terraform output -raw log_group)" --follow`.

## The Mesh UI

The collector serves the canonical, cross-language **Benzene Mesh UI** at **`/mesh-ui/`** — the same
`mesh-ui.html` every port vendors (from the main Benzene repo; see
[`docs/guides/mesh-ui.md`](https://github.com/daniellepelley/Benzene/blob/main/docs/guides/mesh-ui.md)),
kept verbatim in [`collector/ui/`](collector/ui/) with its provenance banner.

After each poll sweep the host projects its catalog into the read-model artifacts the UI renders —
`manifest.json`, `topology.json`, `topics.json`, `usage.json`, and `services/{name}.json` — via
[`benzene.mesh.write_artifacts`](../../docs/reference/mesh.md#the-mesh-ui-artifacts--build_artifacts--write_artifacts),
writing them to `MESH_ARTIFACTS_DIR` (the volume) and serving them alongside the page. The UI fetches
them by relative path, so no UI configuration is needed. From the descriptor/spec feed it derives the
estate (health, contract-drift + spec-hash history), the functional map (topics + consumers/producers,
**payload schemas, version, schema-mismatch**), and per-service **spec + per-check health**; from the
traces, the topology and **usage** counts. Only what genuinely needs feeds this collector doesn't have
— latency/rate metrics, a usage time window, transports, and annotations — degrades gracefully.

Set `MESH_ARTIFACTS_DIR` (Terraform sets it automatically) to enable it; unset disables the UI.

## Feeding the mesh

- **Pull (identity/topics/health):** every `baseUrl` in the fleet config is polled every
  `pollIntervalSeconds`. Any pollable Benzene service works — a Lambda behind API Gateway, another
  Fargate service, anything exposing `/benzene/spec` + `/benzene/health`.
- **Push (the declared graph, then real stats):** a service's `POST <MESH>/mesh/register` carries its
  full `ServiceDescriptor` — `topics` *and* `produces` — so its provider edges exist the moment it
  registers, with zero traffic (mesh.md §2.3/§4). `POST <MESH>/mesh/traces` then feeds invocation/error
  stats for those declared edges; it never adds or removes an edge itself. Point each service's
  `MeshFeedSender` (or an HTTP `MessageSender`) at these routes — `deploy/mesh/fleet/service.py` does
  both at startup and per-invocation respectively.

## Persistence

The collector keeps its fleet view in memory. So it survives a replaced Fargate task, the stack gives
it a durable **EFS volume** mounted at `/data` and sets `MESH_STORE_PATH=/data/mesh-state.json`; the
collector snapshots there after every ingest and rehydrates on the next boot (see
[`benzene.mesh.JsonFileCollectorStore`](../../docs/reference/mesh.md#persistence--collectorstore)).
Access is by EFS access point over NFS from the task's security group only, transit-encrypted.

It is on by default; set `-var="persist_state=false"` to skip the EFS entirely and run purely
in-memory (the catalog refills from the fleet within one poll interval). Running the container by hand,
export `MESH_STORE_PATH` to point at a writable path (unset keeps it in-memory).

## Tear down

```bash
terraform destroy
```

The state EFS is created and destroyed with the stack, so `destroy` takes the persisted snapshot with
it — a fresh `apply` starts from an empty catalog and refills from the fleet.

## Next

- **A metrics feed** — the topology's rate/latency (`requestsPerMinute`, p50/p95/p99) and `usage.json`'s
  time window / transports / durations need a real metrics source (CloudWatch, Tempo). The structural
  usage counts derived from traces already drive the usage view; a metrics feed would fill in the
  observability numbers the UI leaves reduced today.
- **Surface declared-vs-observed (mesh.md §4.2) in the artifacts** — the collector now emits both
  signals (`GET /mesh/topic/{topic}` carries `providerActivity`/`consumerActivity`, and an
  observed-but-undeclared edge files a `contract-drift` issue, merged into `GET /mesh/fleet`'s
  `issues`), but the mesh-ui artifact projection (`topics.json`, `topology.json`) doesn't render either
  one yet — that's part of the `mesh-ui.html` visual-update follow-up.
- **Live-plane enhancements** — `annotations.json` writes and other backend-gated features (mesh-ui.md
  §4) are out of scope for the static floor served here.
