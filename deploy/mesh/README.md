# Deploying the Benzene mesh collector to AWS (Fargate)

The **Mesh Host** — the Benzene-Python analogue of .NET's `Benzene.Mesh.Host` — runs the
[`MeshCollector`](../../packages/benzene-mesh) and the [`MeshPoller`](../../docs/reference/mesh.md),
polling a fleet's `/benzene/spec` + `/benzene/health` on a timer and serving the mesh over HTTP. This
directory ships it as a container (`collector/`) and the Terraform to run it on Fargate behind an ALB
(`terraform/`).

> **Status.** The container and Terraform are complete and validated locally (the host app is
> unit-tested in `collector/tests`). The steps below stand up a *running collector*; pointing it at a
> deployed fleet (services on Lambda) is the next phase, and wiring the [`mesh-ui`](../../docs/mesh-aws-plan.md)
> dashboard awaits its artifact schema. See [`docs/mesh-aws-plan.md`](../../docs/mesh-aws-plan.md).

## What gets created

An ECR repository, an ECS Fargate cluster + service (one collector task), an Application Load Balancer
(public, port 80 → container 8080), the security groups, IAM roles, and a CloudWatch log group — in the
account's **default VPC**. Roughly the cost of one small Fargate task + an ALB while it runs.

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

## Deploy

Terraform creates the ECR repo, but the ECS service needs an image to exist first — so create the repo,
push the image, then apply the rest.

```bash
cd deploy/mesh/terraform
terraform init

# 1. Create just the registry.
terraform apply -target=aws_ecr_repository.collector -var="collector_image=placeholder"
REPO=$(terraform output -raw ecr_repository_url)
REGION=${AWS_REGION:-eu-west-1}

# 2. Build (from the repo root — the Docker context needs packages/ and deploy/) and push.
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${REPO%/*}"
docker build -f ../collector/Dockerfile -t "$REPO:v1" ../../..
docker push "$REPO:v1"

# 3. Apply the full stack, pointing the service at the pushed image and your fleet.
terraform apply \
  -var="collector_image=$REPO:v1" \
  -var='mesh_services_json={"pollIntervalSeconds":30,"services":[{"name":"orders","baseUrl":"https://<orders-url>"}]}'
```

`mesh_services_json` is the inline fleet config (the `MESH_SERVICES` env var; see
[`collector/mesh.json.example`](collector/mesh.json.example) for the shape). Leave it empty to deploy an
idle collector and populate it once a fleet exists.

## Verify

```bash
MESH=$(terraform output -raw mesh_url)
curl "$MESH/benzene/health"     # {"isHealthy": true, ...}   (the ALB health check hits this)
curl "$MESH/benzene/spec"       # the mesh API's own derived spec
curl "$MESH/mesh/fleet"         # the polled fleet: services, topics, issues
curl "$MESH/mesh/topic/orders:place"   # providers + consumer edges for a topic
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

- **Fleet on Lambda** — Terraform to deploy the [`mesh_fleet`](../../examples/mesh_fleet) services as
  Lambdas so the collector polls a real fleet end to end (the collector's `task` IAM role has a spot to
  grant `lambda:InvokeFunction` for a direct-invoke source).
- **mesh-ui** — emit `manifest.json` / `topology.json` in the shape the main-repo dashboard renders,
  and serve it (from the container or S3+CloudFront).
