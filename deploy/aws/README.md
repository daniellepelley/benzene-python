# `deploy/aws` — the Benzene mesh on real AWS

The **AWS lift of [`deploy/mesh`](../mesh/)**. Where `deploy/mesh` runs the distributed mesh on localhost
sockets, this stands the same fleet up on AWS: the three domain services as **Lambda functions** behind
**HTTP API Gateway**, and the **Mesh Host** (collector + aggregator + UI) as a long-lived **App Runner**
container that self-discovers the fleet from Lambda tags, enriches topology + usage from **X-Ray** +
**CloudWatch**, and serves the live mesh UI.

```
                    ┌──────────────────────── AWS account ─────────────────────────┐
 you ──POST /orders─┼─▶ HTTP API ─▶ orders  Lambda ─┐  (X-Ray active tracing on all)│
                    │   HTTP API ─▶ payments Lambda ─┼─▶ /benzene/invoke peer calls  │
                    │   HTTP API ─▶ shipping Lambda ─┘     + mesh feeds ──────┐      │
                    │                    ▲ tags: benzene, benzene:mesh-url    │      │
                    │                    │ list_functions/list_tags           ▼      │
 browser ──GET ─────┼─▶ App Runner ─── Mesh Host: discovery + poll /spec+/health,    │
                    │   (host_url)      receive feeds (x-benzene-mesh-key), aggregate,│
                    │                   read X-Ray service graph + CloudWatch metrics,│
                    │                   emit + serve the mesh UI                      │
                    └───────────────────────────────────────────────────────────────┘
```

Everything here is **Terraform + two container images + a runbook**. There is no hidden state: the
services are the `mesh_fleet` domain on `benzene-aws`, the host is the ASGI `MeshHost` on
`benzene-mesh[host,aws]`.

## Layout

```
deploy/aws/
├── README.md                  ← this runbook
├── drive_traffic.py           ← zero-dep script: POST the orders API so the edges form
├── lambdas/                   ← the service Lambda package (one image, three handlers)
│   ├── Dockerfile             ← shared image; image_config.command selects the handler
│   ├── service.py             ← env-driven builder for orders/payments/shipping
│   ├── orders_handler.py      ← handler = make_handler("orders")   (+ payments_, shipping_)
│   └── payments_handler.py … shipping_handler.py
├── host/                      ← the Mesh Host container
│   ├── Dockerfile             ← installs benzene-mesh[host,aws] + uvicorn
│   └── host_app.py            ← ASGI app: discovery + enrichment + poll loop + UI
└── terraform/                 ← the IaC
    ├── versions.tf providers.  variables.tf  locals.tf  outputs.tf
    ├── ecr.tf  iam.tf  lambda.tf  apigateway.tf  apprunner.tf  ssm.tf
    └── terraform.tfvars.example
```

## Prerequisites

| Need | Why | Notes |
|---|---|---|
| An **AWS account** + credentials | everything below runs against it | `aws configure` / `AWS_PROFILE`; the identity needs IAM/Lambda/API Gateway/App Runner/ECR/SSM/X-Ray/CloudWatch/logs permissions |
| **Terraform** ≥ 1.6 | the IaC | one static binary |
| **Docker** | build + push the two images | any recent Docker |
| **AWS CLI v2** | ECR login + reading outputs | |

**Every step below that touches AWS requires your account** — nothing here was (or can be) run against a
live account from the repo. See *What was and wasn't validated* at the end.

### Assumptions & cost

- **Region** defaults to `eu-west-1` (override `aws_region`). Note **App Runner is not in every region** —
  pick one where it and Lambda container images are available.
- **Cost**: three Lambdas (pay-per-invoke, ~free at rest), three HTTP APIs (pay-per-request), two ECR
  repos (pennies), and **one App Runner instance running 24/7** — App Runner bills for the provisioned
  container, so this is the one line item that accrues while the stack is up. Tear down with
  `terraform destroy` when you're done.
- **X-Ray + CloudWatch** ingestion/metrics have their own (small) per-trace / per-metric costs.

## Steps

Run everything from the **repo root** (the Docker build context needs `packages/` and `examples/`).

### 1. Configure

```bash
cd deploy/aws/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: aws_region, name_prefix, image_tag (e.g. "v1")

# OPTIONAL shared-secret auth on the feed endpoints — keep it out of the tfvars file:
export TF_VAR_mesh_key="$(openssl rand -hex 24)"   # omit to run the collector open (the default)
```

### 2. `terraform init`

```bash
terraform init
```

### 3. Create the ECR repos first (targeted apply)

A Lambda / App Runner service can only reference an image tag that already exists, so create the repos,
push, then apply the rest.

```bash
terraform apply -target=aws_ecr_repository.lambda -target=aws_ecr_repository.host
LAMBDA_REPO=$(terraform output -raw ecr_lambda_repository_url)
HOST_REPO=$(terraform output -raw ecr_host_repository_url)
REGION=$(terraform output -raw ecr_lambda_repository_url | cut -d. -f4)
REGISTRY=${LAMBDA_REPO%/*}
```

### 4. Build + push the two images

```bash
# From the repo root (two levels up from terraform/):
cd ../../..

# ECR login
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"

# The shared service-Lambda image (orders/payments/shipping all run this one image):
docker build -f deploy/aws/lambdas/Dockerfile -t "$LAMBDA_REPO:v1" .
docker push "$LAMBDA_REPO:v1"

# The Mesh Host image:
docker build -f deploy/aws/host/Dockerfile -t "$HOST_REPO:v1" .
docker push "$HOST_REPO:v1"
```

> Use the **same tag** you set for `image_tag` in `terraform.tfvars` (`v1` here). Lambda container images
> must be **linux/amd64** — on an Apple-silicon machine build with `docker build --platform linux/amd64 …`.

### 5. `terraform apply` (the rest)

```bash
cd deploy/aws/terraform
terraform apply
```

This creates the IAM roles, three HTTP APIs, three Lambdas (X-Ray active tracing, mesh-discovery tags),
the App Runner Mesh Host, and (when `mesh_key` is set) the SSM SecureString. App Runner takes a few
minutes to reach `Running`.

### 6. Read the outputs

```bash
terraform output
# orders_api_url    = "https://xxxx.execute-api.<region>.amazonaws.com"
# service_api_urls  = { orders = "…", payments = "…", shipping = "…" }
# host_url          = "https://yyyy.<region>.awsapprunner.com"
```

### 7. Drive traffic so the edges form

```bash
python ../drive_traffic.py "$(terraform output -raw orders_api_url)" --creates 40 --lists 8
```

Each `POST /orders` fans out orders → payments → shipping (spans forwarded), so the collector derives the
consumer edges and X-Ray records the service-graph edges.

### 8. Open the mesh UI

```bash
terraform output -raw host_url   # open this in a browser
```

Within one poll interval (default 60s) the host re-discovers the fleet, polls each service's
`/benzene/spec` + `/benzene/health`, ingests the feeds, reads the X-Ray service graph + CloudWatch
metrics, and the UI renders the three services, the topology plane (with real latency once X-Ray has
aggregated the window), and the usage feed.

> **First-deploy timing**: the host boots before the Lambdas finish creating (they carry its URL), so the
> very first pass finds an empty fleet. Because the host **re-discovers every pass**, it fills in within a
> poll interval — no restart needed. Give X-Ray a few minutes to aggregate a service graph.

### 9. Teardown

```bash
terraform destroy
```

`force_delete` is set on the ECR repos so they delete even with images in them (demo-grade).

## The shared-secret auth (the feed endpoints)

Setting `mesh_key` (step 1) turns on the **simple shared-secret** option:

- each service's `MeshFeedSender` attaches `x-benzene-mesh-key: <key>` to every `register` / `heartbeat`
  / `traces` / `issues` feed (the header rides inside the wire envelope);
- the host's collector rejects any ingest feed without the matching key as `unauthorized`; the
  `benzene:mesh:query:*` read models stay open (the UI polls them same-origin);
- the key is stored as an **SSM SecureString** (`terraform output mesh_key_ssm_parameter`) and injected as
  the `BENZENE_MESH_KEY` env var on both sides, so they always match. Empty key → open (the default,
  today's behaviour).

**This is the simple option only.** Deeper auth — **IAM SigV4** on the invoke URL, **mTLS**, or an **API
Gateway authorizer / Lambda authorizer** in front of the host — is a deliberate follow-up: layer it in
front of the host's `/benzene/invoke` without changing the shared-secret header. The SSM parameter this
stack creates is also what lets you move the key out of the env block (App Runner
`runtime_environment_secrets` / a Lambda SSM read) as a next step.

## Discovery vs. static registry

The host supports **both** (default: discovery):

- **`discovery_mode = "lambda"`** — the host calls `lambda:ListFunctions` + `ListTags` each pass and keeps
  the functions tagged `benzene`, reading each one's API base URL off its `benzene:mesh-url` tag (and the
  optional `benzene:mesh-path` prefix). New services appear automatically; removed ones drop out.
- **`discovery_mode = "static"`** — the host reads a fixed registry from `BENZENE_MESH_REGISTRY`, which
  Terraform always populates with the three service API URLs (known at plan time). Use this to pin the
  fleet, or to drop the host's `lambda:*` permissions.

## Enrichment (X-Ray + CloudWatch)

The host constructs an `XRayTopologySource` (real `client → server` edges + p50/p95/p99 latency from the
X-Ray **service graph**) and a `CloudWatchUsageSource` (per-(topic, transport, status) counts + mean
duration from CloudWatch **metrics**) and drives them each pass — its instance role grants exactly
`xray:GetServiceGraph`/`BatchGetTraces` and `cloudwatch:GetMetricData`/`GetMetricStatistics`/`ListMetrics`.

- **Topology**: Lambda **active tracing** (enabled on all three functions) feeds the X-Ray service graph.
  For rich `client → server` edges (the outbound `/benzene/invoke` legs), instrument the outbound HTTP
  calls with the **AWS X-Ray SDK** or **ADOT** — a documented enhancement; active tracing gives the
  baseline, and the collector-plane trace feeds provide the consumer edges regardless.
- **Usage**: the `benzene.messages.processed` / `benzene.message.duration` metrics must reach CloudWatch
  (e.g. via an EMF exporter / ADOT) in the `Benzene/Mesh` namespace for the usage feed to populate. Set
  `enrich = false` to skip both sources (collector-plane view only).

## What was and wasn't validated (honesty note)

This deployment was authored and **statically validated** in an environment with **no AWS account, no
credentials, and no `aws` CLI** — nothing here was applied against a live account.

- ✅ **Terraform**: `terraform fmt -check -recursive` passes; `terraform init` parses the whole config and
  resolves the provider constraint (it fails only when fetching the `hashicorp/aws` plugin, because this
  environment's egress policy blocks `registry.terraform.io`). `terraform validate` needs that plugin, so
  it could not be run here — the config was reference-audited by hand instead.
- ✅ **Python packaging**: the three Lambda handler modules import and build their services cleanly, and a
  Lambda handler answers `/benzene/spec`, `/benzene/health`, and a domain route end-to-end (with feeds
  disabled when no host is configured). The host `host_app:app` boots through the ASGI lifespan, seeds the
  UI, serves `GET /`, and shuts down cleanly. The shared-secret path is unit-tested (`tests/test_mesh_auth.py`).
- ⚠️ **Docker images**: the Dockerfiles are minimal and correct, but `docker build` **could not** be run
  here — the sandbox has no running Docker daemon reachable for image pulls (the base-image CDN is blocked
  by the same egress policy). Build them in step 4 against your own Docker.

Every step marked as touching AWS (init/apply/destroy, ECR login/push, driving traffic, opening the UI)
**requires your live AWS account**.
