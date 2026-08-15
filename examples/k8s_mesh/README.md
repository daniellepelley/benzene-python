# Kubernetes Mesh Self-Discovery — end-to-end example

The Python counterpart of .NET's `examples/K8sMesh`: three Benzene Cloud Services running as pods, plus
a **mesh service** that discovers them **by label** via the Kubernetes API, interrogates each over plain
in-cluster HTTP, and serves the Mesh UI. The three services also **call each other** — orders → payments
→ shipping — over lightweight Benzene messages on HTTP, so the mesh has real service-to-service traffic
to observe, not just static specs. It runs two ways from the same manifests: credential-free on a
throwaway [`kind`](https://kind.sigs.k8s.io) cluster in CI, or on a real **AWS EKS** cluster with the
Mesh UI on the public internet (see "Deploy to AWS (EKS)" below).

This is the multi-service **mesh estate**; [`examples/k8s_orders`](../k8s_orders) remains the
single-service, three-transport (HTTP/SQS/Kafka) example — a different point in the design space, not
superseded by this one.

## Architecture

```
        Kubernetes namespace: benzene-mesh
  ┌──────────┐   ┌───────────┐   ┌────────────┐
  │ orders   │──▶│ payments  │──▶│ shipping   │   3 Deployments (one image, MESH_SERVICE selects domain)
  │ Service  │   │ Service   │   │ Service    │   each Service labelled  benzene: "true"
  └────┬─────┘   └─────┬─────┘   └─────┬──────┘   ──▶ POST /benzene/invoke  (a { topic, headers, body }
       │  ▲            │  ▲            │  ▲             envelope, addressed by in-cluster DNS — the chain)
       │  │  3. each service PUSHES register + heartbeat + traces to the mesh's collector
       │  │     (http://mesh/benzene/invoke) — the live feed
       │   1. list Services (label benzene=true) via the Kubernetes API
       │   2. GET http://<svc>.<ns>.svc.cluster.local/benzene/spec|health  (interrogate — the pull feed)
       ▼
   ┌────────┐   writes manifest.json / services/*.json / topics.json / registry.json
   │  mesh  │   to /artifacts (pod volume) and serves the Mesh UI at /mesh-ui — NodePort 30080
   └────────┘
```

## Service-to-service calls — lightweight Benzene messages over HTTP

Beyond discovery, each service **chains to the next** over its neighbour's generic envelope endpoint:

- **Ingress** — every service exposes `POST /benzene/invoke`, this port's implementation of the Cloud
  Service Profile's wire-envelope surface (`benzene.http.StandardPaths`, requirement R4). A
  `{ topic, headers, body }` envelope POSTed there is routed to the service's handlers **by the
  envelope's topic**, exactly as a queue or a Lambda invoke would — one endpoint serves every topic, no
  per-route REST contract. This is this port's existing analogue of .NET's `POST /benzene-message`; see
  "Naming difference from .NET" below. (Each service also exposes a friendlier per-topic REST route —
  `POST /orders`, `POST /payments`, `POST /shipments` — for curling by hand, mirroring .NET's
  `[HttpEndpoint]` alongside its own generic endpoint.)
- **Egress** — `orders`' `order:create` handler asks `payments` to `payment:take`, and `payments`'
  `payment:take` handler asks `shipping` to `shipment:book`, each via **`EnvelopeHttpMessageSender`**
  (`envelope_client.py`) — a small, example-local outbound client that POSTs the actual wire envelope to
  one fixed URL and decodes the response envelope, exactly what .NET's `HttpBenzeneMessageClient` does.
  The downstream URL is the neighbour's in-cluster DNS name, injected as `DOWNSTREAM_MSG_URL` (e.g.
  `http://payments/benzene/invoke`); the terminal `shipping` service has none (`NullMessageSender`
  stands in, mirroring .NET's `NullBenzeneMessageClient`).

Send an order into the front of the chain and watch it propagate (from a
`kubectl -n benzene-mesh port-forward svc/orders 8081:80`):

```bash
curl -XPOST localhost:8081/orders -H 'content-type: application/json' \
     -d '{"customerId":"cust-1","sku":"espresso","quantity":2}'
# => {"orderId":"order-...","status":"created"}   ... orders/payments/shipping logs show the chain

# Or hit any service's envelope endpoint directly, addressing a topic it owns:
curl -XPOST localhost:8081/benzene/invoke -H 'content-type: application/json' \
     -d '{"topic":"order:create","headers":{},"body":"{\"customerId\":\"c1\",\"sku\":\"latte\",\"quantity\":1}"}'
```

### Naming difference from .NET: `/benzene/invoke`, not `/benzene-message`

.NET's K8sMesh example hosts its generic multiplexed endpoint at `POST /benzene-message`
(`Benzene.Http.BenzeneMessage`). This Python port's existing Cloud Service Profile implementation
already ships the *same concept* — a single endpoint that decodes a `{topic, headers, body}` envelope
and routes by topic — as `POST /benzene/invoke` (`benzene.http.StandardPaths`, requirement R4). Rather
than inventing a second, parallel envelope endpoint just to match .NET's path literally, this example
reuses the port's own R4 surface: it is architecturally identical ("one endpoint serves every topic,
routed by the envelope's topic"), just at the path this port already standardizes on. `envelope_client.py`
documents the same reasoning for the outbound side.

- Discovery is `benzene.mesh_fleet.discovery_adapters.KubernetesDiscovery`: it lists Services carrying
  the `benzene` label and turns each into a `ServiceEndpoint` at its in-cluster DNS — the mesh's
  discovery service (`mesh/discovery_service.py`) then wraps each in an `HttpServiceSource` and feeds it
  to `benzene.mesh.MeshPoller`, exactly the pull-discovery seam the mesh module already ships (this
  example does not implement a new discovery mechanism).
- The mesh's ServiceAccount has RBAC to **list Services** only (`k8s/mesh.yaml`). The mesh's own
  Service is **not** `benzene`-labelled, so it never discovers itself.
- The catalog lives on the mesh pod's own `emptyDir` volume (single writer + reader) — no blob store.
- **The live Fleet plane**: the mesh pod also hosts a `benzene.mesh.MeshCollector` at `/benzene/invoke`
  (the same generic surface, shared with the discovery/refresh topic), and each service reports to it
  (`MeshReporter`, driven by the `MESH_COLLECTOR_ENVELOPE_URL` the manifests set) — registrations,
  health heartbeats, and per-call traces, on a 15s interval. It reduces gracefully: an unreachable
  collector never fails a service, it just leaves that service's live feed empty.

## Projects

| Path | What it is |
|---|---|
| `service/` | one domain-service image; `MESH_SERVICE` picks the domain (orders/payments/shipping) — `domain.py` (handlers), `startup.py` (composition root), `host.py` (env-driven wiring), `reporting.py` (the mesh push loop), `main.py` (entrypoint) |
| `mesh/` | the discovery + aggregation + UI service — `discovery_service.py` (Kubernetes discovery + poll + artifact-write pass), `host.py` (the collector + `/mesh/refresh` + Mesh UI wiring), `static.py` (serves `/mesh-ui/*`), `main.py` (entrypoint: HTTP server + a 30s discovery loop) |
| `envelope_client.py` | `EnvelopeHttpMessageSender` — the shared outbound client both the service chain and the collector reporting use |
| `k8s/` | manifests: namespace, the 3 Deployments+Services, and the mesh (SA + RBAC + Deployment + NodePort Service), with a kustomize base for target-specific overlays |
| `deploy/` | Terraform for the AWS leg: EKS cluster + node group + the two ECR repositories |
| `deploy/eks/` | kustomize overlay over `k8s/`: ECR images (set by the workflow) + a LoadBalancer mesh Service |
| `../../.github/workflows/deploy-k8s-mesh-kind.yml` | build images → kind → deploy → assert 3 discovered |
| `../../.github/workflows/deploy-k8s-mesh-eks.yml` | terraform apply → push images to ECR → deploy → assert 3 discovered → print the public URLs |
| `../../.github/workflows/destroy-k8s-mesh-eks.yml` | one-click teardown of the EKS stack — no dropdown, no confirmation phrase |
| `tests/` | in-memory, dogfooded tests (`create_test_host` for the services; a fake `Discovery` + `CallableServiceSource` for the mesh) |

## Run it in CI (no credentials)

**Actions → Deploy K8s Mesh Example (kind) → Run workflow.** It builds both images, creates a `kind`
cluster, loads the images, applies the manifests, waits for rollout, then `POST`s `/mesh/refresh` and
asserts `{"discovered":3}`, then exercises the orders → payments → shipping chain over HTTP — a real
end-to-end proof of both the Kubernetes discovery path and the service-to-service chain.

## Run it locally

You need Docker and a `kind` cluster (`kind create cluster --name benzene`):

```bash
docker build -f examples/k8s_mesh/Dockerfile      -t benzene-k8smesh-service:local .
docker build -f examples/k8s_mesh/Dockerfile.mesh -t benzene-k8smesh-mesh:local .
kind load docker-image benzene-k8smesh-service:local --name benzene
kind load docker-image benzene-k8smesh-mesh:local     --name benzene
kubectl apply -k examples/k8s_mesh/k8s/   # -k: the directory is a kustomize base (deploy/eks overlays it)

kubectl -n benzene-mesh port-forward svc/mesh 8080:80
# then, in another shell:
curl -XPOST localhost:8080/mesh/refresh   # {"discovered":3}
open http://localhost:8080/mesh-ui        # the discovered catalog + Topics table, merged with the
                                          # live Fleet plane (observed) — services as they register,
                                          # heartbeat, and push traces to the mesh's collector
```

Each service's own Spec is reachable the same way (`port-forward svc/orders 8081:80` →
`http://localhost:8081/benzene/spec`).

## Deploy to AWS (EKS)

**Actions → Deploy K8s Mesh Example (EKS) → Run workflow**, `action: apply`. The AWS leg of this
example, using the same "test" GitHub Environment credentials as this repo's `Deploy Mesh (AWS)`
workflow (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, which additionally need EKS, EC2, and ECR
permissions) and the same per-account S3 state bucket, under its own key (`python-k8s-mesh/`). The
workflow:

1. `terraform apply` on `deploy/` — an EKS cluster (`benzene-python-k8smesh`) with one small managed
   node group on the account's default VPC, plus two ECR repositories. First-time cluster creation
   takes ~10–15 minutes.
2. builds the two images and pushes them to ECR, tagged with the commit SHA.
3. applies the **unchanged** `k8s/` manifests through the `deploy/eks` kustomize overlay, which swaps
   in the ECR images and turns the mesh's NodePort Service into an internet-facing **LoadBalancer** —
   and does the same for each `benzene`-labelled Service, so orders/payments/shipping are directly
   callable from the internet as well.
4. waits for the ELBs, `POST`s `/mesh/refresh`, asserts `{"discovered":3}`, and prints
   `http://<elb-hostname>/mesh-ui` plus each service's `http://<elb-hostname>/benzene/spec` URL in the
   run summary — open the Mesh UI to watch the mesh discover the pods, or hit a service's
   `/benzene/spec`, `/benzene/health`, or `POST /benzene/invoke` directly.

Same dogfooding, different substrate: discovery is still `KubernetesDiscovery` listing
`benzene`-labelled Services via the cluster API — EKS needs no code or manifest changes, only images
it can pull and a route in.

**Costs & teardown:** an EKS control plane bills ~$0.10/hour plus two `t3.small` nodes and four
classic ELBs (mesh + the three services, one per LoadBalancer Service). Re-run the workflow with
`action: destroy` to tear it all down (it deletes the namespace first so Kubernetes releases the
ELBs, then `terraform destroy`) — or, for a one-click teardown that needs no dropdown switch, run
[`Destroy K8s Mesh Example (EKS)`](../../.github/workflows/destroy-k8s-mesh-eks.yml)
(**Actions → Destroy K8s Mesh Example (EKS) → Run workflow** with the defaults; no typed
confirmation phrase needed). It shares the same S3 state as the deploy workflow, so it tears down
anything that workflow created; if nothing was ever deployed in the target account, it's a no-op.
Note the services are exposed **unauthenticated** — fine for this throwaway demo, not a pattern to
copy for real workloads.

To deploy from a laptop instead of CI, run the same four steps by hand: `terraform apply` in
`deploy/`, push the images to the ECR repositories it outputs, `aws eks update-kubeconfig`, then
`kustomize edit set image` + `kubectl apply -k` in `deploy/eks` (the workflow is the reference
script for the exact commands).

## Tests

```bash
pytest examples/k8s_mesh/tests -q
```

`tests/test_service.py` boots each of the three domains from `ServiceStartUp` through
`create_test_host(...).build_http()` (the same in-memory HTTP harness every example in this repo
uses), faking only the outbound `MessageSender` — proving ingress → handler → egress, and that
`/benzene/invoke` really does serve every topic. `tests/test_mesh.py` drives the real
`MeshDiscoveryService` against a real `MeshCollector`, faking only the two external edges (a fake
`Discovery` standing in for the Kubernetes API, and `CallableServiceSource` standing in for the HTTP
poll) — proving discover → poll → collector → artifacts end to end without a cluster.

## Known first-deploy iteration points

- **Base image** — the Dockerfiles use `python:3.12-slim`; swap the tag if your registry mirrors a
  different Python version.
- **RBAC scope** — discovery is scoped to the `benzene-mesh` namespace (`MESH_NAMESPACE`); widen the
  Role to a ClusterRole if you point it across namespaces.
- **`kubernetes` extra** — the mesh image needs `benzene-mesh-fleet[kubernetes]` (the real
  `kubernetes` Python client) installed; the service image does not.
