# Deploying the Benzene mesh collector to AWS (Fargate)

The **Mesh Host** — the Benzene-Python analogue of .NET's `Benzene.Mesh.Host` — runs the
[`MeshCollector`](../../packages/benzene-mesh) and the [`MeshPoller`](../../docs/reference/mesh.md),
polling a fleet's `/benzene/spec` + `/benzene/health` on a timer and serving the mesh over HTTP. This
directory ships it as a container (`collector/`) and the Terraform to run it on Fargate behind an ALB
(`terraform/`).

> **Status.** The collector container, the demo fleet Lambdas, and the Terraform are complete and
> validated locally (both the host and the fleet service are unit-tested — `collector/tests`,
> `fleet/tests`). One `terraform apply` stands up the whole mesh: the fleet on Lambda + the collector on
> Fargate, wired together. Only the [`mesh-ui`](../../docs/mesh-aws-plan.md) dashboard is outstanding
> (awaiting its artifact schema). See [`docs/mesh-aws-plan.md`](../../docs/mesh-aws-plan.md).

## What gets created

- **The collector**: an ECR repository, an ECS Fargate cluster + service (one collector task), a public
  Application Load Balancer (port 80 → container 8080), security groups, IAM roles, a CloudWatch log
  group — in the account's **default VPC**.
- **The fleet** (`deploy_fleet=true`, the default): three Lambdas (`orders` → `inventory` →
  `notifications`, all from one zip, env-selected) each behind an HTTP API Gateway, plus their IAM role.
  The collector is pointed at their URLs automatically; each service calls the next with trace
  propagation and pushes traces back, so the collector derives the consumer edges.

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
curl "$MESH/mesh/topic/inventory:reserve"   # providers: [inventory], consumers: [orders]
curl "$MESH/mesh/topic/notify:send"         # providers: [notifications], consumers: [inventory]
```

Logs: `aws logs tail "$(terraform output -raw log_group)" --follow`.

## Feeding the mesh

- **Pull (identity/topics/health):** every `baseUrl` in the fleet config is polled every
  `pollIntervalSeconds`. Any pollable Benzene service works — a Lambda behind API Gateway, another
  Fargate service, anything exposing `/benzene/spec` + `/benzene/health`.
- **Push (the call graph):** services POST their trace batches to `POST <MESH>/mesh/traces` (and
  optionally `/mesh/register`, `/mesh/heartbeat`, `/mesh/issues`). Consumer edges are derived from the
  trace parentage — point each service's `MeshFeedSender` (or an HTTP `MessageSender`) at these routes.

## Tear down

```bash
terraform destroy
```

## Next

- **mesh-ui** — emit `manifest.json` / `topology.json` in the shape the main-repo dashboard renders,
  and serve it (from the container or S3+CloudFront). This is the last piece; it needs the `mesh-ui`
  artifact schema.
- **First live run** — apply this in a real account and confirm end to end (the port has not yet run on
  AWS). Consider an integration test that applies, drives an order, asserts the mesh, and destroys.
