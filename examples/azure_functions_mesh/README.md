# Azure Functions Mesh — self-discovery, end to end (purely Azure Functions)

The Python counterpart of .NET's
[`examples/AzureFunctionsMesh`](https://github.com/daniellepelley/benzene-dotnet/tree/main/examples/AzureFunctionsMesh):
**six** Benzene Cloud Service Azure Functions that call each other over **Service Bus, Event Hub, and
Event Grid**, plus a **mesh** (a seventh Function) that discovers them via Azure Resource Manager
(managed identity, tag-based), interrogates each over real **HTTPS**, and publishes the aggregated
catalog to **Blob Storage** — the same estate shape as the .NET port, so the Mesh UI renders an
identical topology.

Unlike this repo's push-based [`examples/aws_lambda_mesh`](../aws_lambda_mesh) sibling (a Lambda has no
HTTP endpoint of its own, so that mesh interrogates by direct invoke instead), Azure Functions *are*
HTTP-addressable — so this mesh's interrogation is genuine **pull-based discovery over HTTP**, exactly
like [`examples/k8s_mesh`](../k8s_mesh) proves on Kubernetes. Both dogfood the same transport-agnostic
`benzene.mesh` core (`MeshPoller`, `MeshCollector`, `build_artifacts`) — only discovery and the storage
target differ per substrate.

## The estate

```
  orders --payment:take (Service Bus)--> payments --shipment:book (Service Bus)--> shipping
    |                                        |                                        |
    +--order:placed (Event Hub, fan-out)---->+  payment:captured (Event Grid)         +  shipment:dispatched (Event Grid)
    v            v                           v            v                          v          v          v
 inventory  notifications                notifications  analytics                inventory  notifications  analytics
```

Each of the six services is **one Function App**, tagged `benzene = "true"` for discovery, and:

- exposes the Cloud Service Profile over an HTTP trigger — `/benzene/spec`, `/benzene/health`,
  `/benzene/invoke` (`benzene.http.StandardPaths`) — at the **site root**, because `host.json` sets
  `"extensions": {"http": {"routePrefix": ""}}` (Function HTTP triggers default to an `/api` prefix,
  which would not match the mesh's discovery-built URLs otherwise); orders additionally answers
  `POST /orders`, the front door of the chain — both match the same catch-all
  `route="{*route}"` trigger (`service/function_app.py`);
- sends its produced topics over the transport Terraform wires for it (Service Bus / Event Hub / Event
  Grid) via a small per-topic outbound router (`service/host.py`'s `TopicRoutingMessageSender`) — a
  single POST to `/orders` therefore genuinely cascades through the whole estate on a real deploy.

The **mesh Function** (`mesh/function_app.py`, **not** tagged for discovery — it never interrogates
itself) runs on a **Timer trigger** (default every 5 minutes; Azure Functions Python has no long-lived
background-service host the way a container would, so a schedule is the periodic-aggregation
mechanism here, matching .NET's `UseTimerTrigger` on the same Consumption plan):

1. **discover** — `benzene.mesh_fleet.AzureDiscovery` (the real ARM SDK: `DefaultAzureCredential` +
   `ResourceManagementClient.resources.list()`, filtered by tag);
2. **interrogate** — one `benzene.mesh.HttpServiceSource` per discovered Function App
   (`mesh/discovery_service.py`'s `azure_service_source`), GETting `/benzene/spec` +
   `/benzene/health` over real HTTPS — fed into a real `benzene.mesh.MeshPoller`/`MeshCollector`, the
   same transport-agnostic core every Benzene mesh uses. `AzureDiscovery` hands back a **bare
   hostname** (e.g. `myapp.azurewebsites.net`) — there is no separate scheme field to discover, since
   ARM's hostname attributes never carry one — so `azure_service_source` is the one place `https://`
   gets prepended before polling (same as every other example's discovery-to-HTTP wiring in this repo,
   not a gap this task needed to close);
3. **publish** — the discovered registry (`registry.json`) and the full catalog (`manifest.json`,
   `topology.json`, `topics.json`, `usage.json`, `asyncapi.json`, `annotations.json`,
   `services/{name}.json`) to Blob Storage via the new `benzene.mesh.BlobArtifactStore` /
   `write_artifacts_to_blob` (see "Framework additions" below).

## Framework additions

Two small, additive changes to the framework packages made this example possible — both confirmed-real
gaps per the task's research audit, both covered by their own unit tests, and both the *only*
framework-package files this example's work touched:

- **`benzene.azure.EventHubMessageSender`** (`packages/benzene-azure/benzene/azure/clients.py`) — the
  missing egress counterpart of Event Hub *ingress* (`decode_event_hub_event` already existed): builds
  a real `azure.eventhub.EventData`, carries the Benzene topic + headers on its `properties` (the same
  `properties`/`application_properties` channel the decoder reads), and sends it via a lazily-built
  `azure.eventhub.EventHubProducerClient` — or an injected `producer` for tests, exactly like
  `ServiceBusMessageSender`'s existing shape. Unit tests: `tests/test_azure_eventhub_egress.py`.
- **`benzene.mesh.BlobArtifactStore` / `write_artifacts_to_blob`**
  (`packages/benzene-mesh/benzene/mesh/blob_artifacts.py`) — the Blob counterpart of the existing local
  `write_artifacts` and AWS's `S3ArtifactStore`/`write_artifacts_to_s3`: the identical `write(key,
  document)` seam and `(collector, sources, generated_at)` signature, publishing the same document set
  as Blob Storage objects (`ContainerClient.upload_blob(name, data, overwrite=True,
  content_settings=ContentSettings(content_type="application/json"))`) instead of files or S3 objects.
  Purely additive — `artifacts.py`, `store.py`, and `s3_artifacts.py` are untouched. Unit tests:
  `tests/test_mesh_blob_artifacts.py`.

Both are optional-Azure-SDK (the `[eventhub]` extra on `benzene-azure`, the `[azure]` extra on
`benzene-mesh`), lazily imported, and constructor-injectable — exactly the convention every other Azure
binding in this port follows (`benzene.azure.clients`, `benzene.mesh_fleet.discovery_adapters`).

**`benzene.mesh_fleet.AzureDiscovery`** itself needed *no* framework change — it already existed
(alongside `AwsCloudMapDiscovery`/`AwsLambdaDiscovery`/`KubernetesDiscovery`) but carried zero unit
tests. This task added real, fake-client coverage for it in `tests/test_mesh_fleet.py`: tag-to-name
resolution, the `default_host_name`/`fqdn` address fallback, dict-shaped vs. object-shaped ARM resource
duck-typing, a custom `service_tag`, and the empty-registry contract.

## Two tags, on purpose

Terraform tags every service Function App with **two** keys, and that's intentional:

- **`discovery_tag_key`** (default `"benzene"`, value `"true"`) — matches this port's cross-example
  discovery-tag convention (`deploy/mesh/terraform`, `examples/aws_lambda_mesh`'s `discovery_tag_key`).
  Nothing in `AzureDiscovery` actually *filters* on this tag (see the caveat below) — it exists so a
  human (or a future, stricter discovery adapter) can tell "in the mesh" resources apart from anything
  else in the resource group at a glance.
- **`service_tag_key`** (default `"benzene:service"`) — the tag `AzureDiscovery` **actually reads**
  (its `service_tag` constructor argument) to resolve a discovered resource's display name; falling
  back to the resource's own ARM name when absent. Terraform sets it to each service's plain domain
  name (`orders`, `payments`, …) so `registry.json` and the interrogation-time catalog agree, without
  the mesh's own resource name (`benzene-py-fnmesh-orders`) leaking into topic/topology JSON.

## A known, unresolved gap in `AzureDiscovery` (read before a real deploy)

`AzureDiscovery.discover()` calls `client.resources.list()` — Azure Resource Manager's **generic**
resource-listing API — and reads each resource's top-level `default_host_name`/`fqdn` attribute. In
practice, ARM's generic `resources.list()` does **not** populate provider-specific properties like a
`Microsoft.Web/sites` resource's `defaultHostName` unless the call additionally requests
`$expand=properties` *and* the resource provider actually returns that field through the generic
resource envelope — which `Microsoft.Web/sites` does not reliably do. On a real subscription, this
adapter is therefore likely to discover **zero** usable endpoints as currently implemented, unlike
`AwsLambdaDiscovery`/`KubernetesDiscovery`, whose underlying list APIs are resource-type-specific and
do return an address directly.

This is a pre-existing gap in `AzureDiscovery` itself (not introduced by this example), and per this
task's explicit scope — reuse `AzureDiscovery` exactly as it exists, add tests for its *current*,
documented behavior — it was **not** modified here. `tests/test_mesh.py`/`tests/test_mesh_fleet.py`
therefore prove the code paths this adapter actually has (tag resolution, address fallback, the
Discovery contract) against a fake ARM client, not that `resources.list()` genuinely returns Function
App hostnames on a live subscription. A first real deploy should budget time to verify this — likely
fix: switch the adapter to `WebSiteManagementClient.web_apps.list_by_resource_group(...)` (the
`Microsoft.Web`-specific SDK, which *does* return `default_host_name`), or add `$expand=properties` plus
provider-specific property parsing to the existing generic-resource path. Either is a `benzene-mesh-
fleet` change outside this example's authorized scope; flagging it here rather than making it
unilaterally.

## Deploying to a real Azure subscription

```bash
examples/azure_functions_mesh/deploy/build_service.sh   # -> deploy/build/service.zip (one shared zip, all six domains)
examples/azure_functions_mesh/deploy/build_mesh.sh      # -> deploy/build/mesh.zip
cd examples/azure_functions_mesh/deploy
terraform init -backend-config=... # see .github/workflows/deploy-azure-functions-mesh.yml for the exact azurerm backend config
terraform apply
# publish each app (az functionapp deployment source config-zip ...), warm it, then:
terraform apply -var wire_eventgrid_subscriptions=true
```

Terraform provisions: a storage account (Functions runtime **and** the mesh catalog's `$web` static
website), a Linux Consumption plan, the seven Function Apps (six tagged services + the mesh), the
**Service Bus** namespace + queues, the **Event Hub** namespace + hub + consumer groups, the **Event
Grid** topic + subscriptions, and the mesh identity's role assignments (**Reader** on the resource
group for ARM discovery, **Storage Blob Data Contributor** on the storage account for the catalog).
Each service gets exactly the messaging connection strings it uses; the mesh is scoped via
`MESH_SUBSCRIPTION_ID` (read from the caller's own `azurerm_client_config`, not a variable — a
resource-group-scoped Reader can't be widened by mis-setting one).

Python Function Apps on Linux Consumption don't need .NET's example's "publish self-contained"
workaround (that routed around .NET 10 not shipping on the Y1 plan yet): a plain zip deploy with
`SCM_DO_BUILD_DURING_DEPLOYMENT=true` lets Oryx `pip install` from `requirements.txt` remotely during
deploy, so `deploy/build_service.sh`/`deploy/build_mesh.sh` just stage source — no dependency vendoring.

**Static viewer, not a live HTTP surface on the mesh Function.** Following `examples/aws_lambda_mesh`'s
precedent: the storage account's `$web` container is enabled as a **static website**
(`azurerm_storage_account.static_website{}`, which auto-configures anonymous blob read — no separate
bucket-policy dance the way S3 needs), and Terraform uploads the **canonical, already-vendored
`mesh-ui.html`** (`web/index.html` here — the identical file `examples/k8s_mesh/mesh/ui/mesh-ui.html`
and `examples/aws_lambda_mesh/web/index.html` already vendor) as `$web/mesh/index.html`, right next to
the catalog the mesh Function writes (`MESH_BLOB_CONTAINER=$web`, `MESH_ARTIFACT_PREFIX=mesh`). The
storage account's `primary_web_endpoint` + `mesh/` therefore serves the full Mesh UI reading the real
catalog with same-origin relative fetches — no extra HTTP trigger, no `BenzeneHttpApp` wiring on the
mesh Function at all.

See `deploy/main.tf` for the full resource list (modelled on .NET's `deploy/main.tf`, minus Application
Insights/usage tracking — out of scope for this round) and
`.github/workflows/deploy-azure-functions-mesh.yml` for the apply/plan/destroy dispatch.

## Teardown

Run **Deploy Azure Functions Mesh Example** with **action: destroy** (same workflow, one dispatch input
— this repo's `deploy-mesh.yml`/`deploy-k8s-mesh-eks.yml`/`deploy-aws-lambda-mesh.yml` convention, not a
separate destroy workflow file). Pass the **same** `location`/`storage_account` you deployed with — the
remote azurerm state is keyed on `storage_account`, so a mismatched value points Terraform at an empty
state and destroys nothing. `terraform destroy` runs with `wire_eventgrid_subscriptions` left at its
default `false`, which keeps the live function-key data source out of the destroy plan (it would
otherwise fail reading an app already mid-teardown) while still destroying the Event Grid subscriptions
already recorded in state. Tick **`delete_resource_group`** for a full cleanup — that additionally
removes `benzene-py-fnmesh-rg` and, with it, the Terraform state account; leave it unticked to keep the
resource group (and state) around for a follow-up apply.

Locally: `cd examples/azure_functions_mesh/deploy && terraform destroy -var "storage_account=<name>"`
(against the same remote state).

## Known first-deploy iteration points

Live Azure behaviour is only verifiable on a real deploy (as with every mesh example in this port); the
likely first-run tweaks, beyond the `AzureDiscovery` gap above:

- **Cold start vs. the 10s fetch timeout** — an idle Consumption-plan app may cold-start past
  `HttpServiceSource`'s default 10s fetch timeout and flash *unreachable* until warm. The deploy
  workflow warms every service before wiring Event Grid, but the mesh's own Timer-triggered
  interrogation pass has no equivalent warm-up; a Premium/dedicated plan avoids this for a real workload.
- **`routePrefix`** — if `/benzene/spec` 404s, confirm `service/host.json`'s `"routePrefix": ""` took
  effect on the deployed app (Azure's `syncfunctiontriggers` must have run against the *published*
  `host.json`, not a stale one).
- **Event Grid subscriptions need the target functions warm** — same Consumption-plan validation
  quirk .NET's sibling documents: the Functions Event Grid extension webhook validates against the
  *live* running function, so a cold consumer app can fail subscription creation with a validation
  timeout. The workflow warms every app before the second `terraform apply`; if a subscription still
  fails, the consumer app was likely cold — warm it (`curl` any route) and re-run that apply.
- **`WEBSITE_RUN_FROM_PACKAGE` drift** — the zip-deploy step (or Oryx's remote build) sets this app
  setting once the build finishes; it is deliberately not declared in `app_settings` and both Function
  App resources carry `lifecycle { ignore_changes = [...] }` for it, so the second (`wire_eventgrid_
  subscriptions=true`) apply doesn't strip the deployed package and undeploy the functions mid-run.
- **Oryx remote build latency** — `SCM_DO_BUILD_DURING_DEPLOYMENT=true` means each `zip deploy` waits
  on a real `pip install` server-side; the workflow's warm-up loop tolerates this, but a manual `az
  functionapp deployment source config-zip` right after a fresh apply may need a minute before the app
  actually answers.

## Tests

```bash
pytest examples/azure_functions_mesh/tests -q
```

`tests/test_services.py` boots each of the six domains from `ServiceStartUp` through
`create_test_host(...).build_azure()` (the same in-memory Azure test harness every Azure example in
this repo uses), faking only the outbound `MessageSender` — proving ingress -> handler -> egress over
the *native* Azure Functions trigger shapes each service actually receives (HTTP, Service Bus, Event
Hub, Event Grid), and that the real HTTP Cloud Service Profile (`/benzene/health`, `/benzene/spec`,
`/benzene/invoke`) answers too — the surface the mesh actually interrogates.
`tests/test_mesh.py` drives the real `run_mesh_aggregation` against a fake `Discovery` and an injected
HTTP `fetch` that routes straight to each service's real, in-memory `AzureFunctionsApp` — proving
discover -> interrogate -> collector -> Blob catalog end to end, with no cloud account, and that
`azure_service_source` correctly prepends `https://` to `AzureDiscovery`'s bare-hostname addresses.

## Projects

| Path | What it is |
|---|---|
| `service/` | the six-domain composition root — `domain.py` (handlers + health checks), `startup.py` (`ServiceStartUp`, picks the domain by `SERVICE_NAME`), `host.py` (env-driven Azure wiring + `TopicRoutingMessageSender`), `function_app.py` (the Azure Functions v2 entry point — one shared file, `SERVICE_NAME` picks which extra triggers it declares) |
| `mesh/` | the discovery + interrogation + Blob-publishing aggregator — `discovery_service.py` (`run_mesh_aggregation`, `azure_service_source`), `host.py` (env-driven production wiring), `function_app.py` (the Timer-triggered entry point) |
| `web/index.html` | the vendored canonical Mesh UI (same file as `examples/k8s_mesh/mesh/ui/mesh-ui.html` / `examples/aws_lambda_mesh/web/index.html`), uploaded by Terraform to the storage account's `$web` static website |
| `deploy/` | Terraform (modelled on .NET's `deploy/main.tf`, minus Application Insights) + `build_service.sh`/`build_mesh.sh` (the two Function App zips) |
| `tests/` | in-memory, dogfooded tests (`create_test_host` for the six services; a fake `Discovery` + an injected HTTP fetch routed to real in-memory `AzureFunctionsApp`s for the mesh) |
| `../../.github/workflows/deploy-azure-functions-mesh.yml` | apply / plan / destroy, dispatched |
