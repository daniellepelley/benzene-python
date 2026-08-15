# Terraform for examples/azure_functions_mesh — modelled on .NET's examples/AzureFunctionsMesh/deploy
# (same topology, same resource shapes, minus Application Insights/usage tracking — out of scope for
# this port's round) and on this repo's own examples/aws_lambda_mesh/deploy Terraform style. State:
# azurerm remote backend (empty here; -backend-config supplies resource_group/storage_account/container
# at `terraform init`, same convention as .NET's AzureFunctionsMesh workflow).
#
# Divergence from .NET's build: Python Function Apps on Linux Consumption don't need the "self-contained
# runtime" workaround .NET's example uses (that was to route around .NET 10 not shipping on Y1 yet) — a
# plain zip deploy with SCM_DO_BUILD_DURING_DEPLOYMENT=true lets Oryx `pip install` from requirements.txt
# during deploy, so deploy/build_service.sh / deploy/build_mesh.sh just stage source, not vendor deps.

terraform {
  required_version = ">= 1.5.0"
  backend "azurerm" {}
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    # For the identity-propagation delay before the mesh identity's role assignments (see time_sleep).
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
}

provider "azurerm" {
  features {}
  # The deploy workflow registers the resource providers this stack needs (Storage, Web,
  # ManagedIdentity) before Terraform runs, so the provider need not mass-register on every apply.
  resource_provider_registrations = "none"
}

data "azurerm_client_config" "current" {}

# The resource group is bootstrapped imperatively by the workflow (`az group create`, idempotent) — it
# holds the remote-state storage account — so Terraform reads it rather than owning it.
data "azurerm_resource_group" "this" {
  name = var.resource_group
}

locals {
  # The six Cloud Service Function Apps (tagged for discovery). orders/payments/shipping form the
  # command chain and publish events; inventory/notifications/analytics are pure event consumers.
  services = ["orders", "payments", "shipping", "inventory", "notifications", "analytics"]

  # Per-service app settings — the messaging connection strings + entity names each service's host.py
  # actually reads (see service/host.py's _SERVICE_BUS_TARGETS/_EVENT_HUB_TARGETS/_EVENT_GRID_TOPICS,
  # which this map must stay in step with), merged with the common settings on each Function App below.
  service_app_settings = {
    orders = {
      BENZENE_SERVICEBUS_CONNECTION = azurerm_servicebus_namespace.this.default_primary_connection_string
      PAYMENTS_QUEUE                = azurerm_servicebus_queue.payments.name
      BENZENE_EVENTHUB_CONNECTION   = azurerm_eventhub_namespace.this.default_primary_connection_string
      ORDER_PLACED_EVENTHUB         = azurerm_eventhub.order_placed.name
    }
    payments = {
      BENZENE_SERVICEBUS_CONNECTION = azurerm_servicebus_namespace.this.default_primary_connection_string
      SHIPPING_QUEUE                = azurerm_servicebus_queue.shipping.name
      BENZENE_EVENTGRID_ENDPOINT    = azurerm_eventgrid_topic.this.endpoint
      BENZENE_EVENTGRID_KEY         = azurerm_eventgrid_topic.this.primary_access_key
    }
    shipping = {
      BENZENE_SERVICEBUS_CONNECTION = azurerm_servicebus_namespace.this.default_primary_connection_string
      BENZENE_EVENTGRID_ENDPOINT    = azurerm_eventgrid_topic.this.endpoint
      BENZENE_EVENTGRID_KEY         = azurerm_eventgrid_topic.this.primary_access_key
    }
    inventory     = { BENZENE_EVENTHUB_CONNECTION = azurerm_eventhub_namespace.this.default_primary_connection_string }
    notifications = { BENZENE_EVENTHUB_CONNECTION = azurerm_eventhub_namespace.this.default_primary_connection_string }
    analytics     = {}
  }

  # Event Grid routing: which consuming Function's Event-Grid-triggered function each event fans out
  # to (matched by the event's type = the Benzene topic). function = the Azure Function's declared name
  # (service/function_app.py's @app.function_name(f"{service}-eg")).
  eventgrid_routes = {
    "payment_captured-notifications"    = { event_type = "payment:captured", service = "notifications", function = "notifications-eg" }
    "payment_captured-analytics"        = { event_type = "payment:captured", service = "analytics", function = "analytics-eg" }
    "shipment_dispatched-inventory"     = { event_type = "shipment:dispatched", service = "inventory", function = "inventory-eg" }
    "shipment_dispatched-notifications" = { event_type = "shipment:dispatched", service = "notifications", function = "notifications-eg" }
    "shipment_dispatched-analytics"     = { event_type = "shipment:dispatched", service = "analytics", function = "analytics-eg" }
  }
}

# ---------------------------------------------------------------------------------------------------
# Storage: the Functions runtime store AND, via its $web static website, the mesh catalog + the
# vendored canonical mesh-ui.html viewer (same file examples/aws_lambda_mesh/web/index.html and
# examples/k8s_mesh/mesh/ui/mesh-ui.html vendor) — the lightweight, language-neutral stand-in for a
# Function-served UI (mirrors examples/aws_lambda_mesh's S3-static-website choice; a live HTTP surface
# on the mesh Function itself would need a whole extra HTTP trigger + routing for no benefit over a
# static page reading the same JSON with same-origin relative fetches).
# ---------------------------------------------------------------------------------------------------
resource "azurerm_storage_account" "this" {
  name                     = var.storage_account
  resource_group_name      = data.azurerm_resource_group.this.name
  location                 = data.azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  static_website {
    index_document = "index.html"
  }
}

# Uploaded to $web/mesh/index.html; BlobArtifactStore (mesh/host.py: MESH_BLOB_CONTAINER="$web",
# MESH_ARTIFACT_PREFIX="mesh") writes the catalog JSON right alongside it, so the viewer's relative
# fetches (./manifest.json etc.) resolve against the static website endpoint below.
resource "azurerm_storage_blob" "viewer" {
  name                   = "mesh/index.html"
  storage_account_name   = azurerm_storage_account.this.name
  storage_container_name = "$web"
  type                   = "Block"
  source                 = var.viewer_html
  content_type           = "text/html"
  content_md5            = filemd5(var.viewer_html)
}

# --- Consumption plan (Linux) ------------------------------------------------------------------------
resource "azurerm_service_plan" "this" {
  name                = "${var.project}-plan"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  os_type             = "Linux"
  sku_name            = "Y1"
}

# ---------------------------------------------------------------------------------------------------
# The six Cloud Service Function Apps (tagged for discovery). One shared zip
# (deploy/build_service.sh); SERVICE_NAME picks the domain (service/startup.py's ServiceStartUp).
# ---------------------------------------------------------------------------------------------------
resource "azurerm_linux_function_app" "service" {
  for_each            = toset(local.services)
  name                = "${var.project}-${each.value}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  service_plan_id     = azurerm_service_plan.this.id

  storage_account_name       = azurerm_storage_account.this.name
  storage_account_access_key = azurerm_storage_account.this.primary_access_key

  # Discovery finds services by this tag; the mesh Function App deliberately does NOT carry it.
  # service_tag_key additionally carries the display name AzureDiscovery actually resolves the
  # ServiceEndpoint's `name` from (see README "Two tags, on purpose").
  tags = {
    (var.discovery_tag_key) = "true"
    (var.service_tag_key)   = each.value
  }

  site_config {
    # The mesh (and the platform) probe the Cloud Service's own health endpoint.
    health_check_path                 = "/benzene/health"
    health_check_eviction_time_in_min = 5
    application_stack {
      python_version = var.python_version
    }
  }

  # Each service is its own deployable now; it gets exactly the messaging connection strings + entity
  # names it uses (local.service_app_settings). SCM_DO_BUILD_DURING_DEPLOYMENT lets Oryx `pip install`
  # from requirements.txt on zip deploy (see deploy/build_service.sh).
  app_settings = merge(
    {
      SERVICE_NAME                   = each.value
      FUNCTIONS_WORKER_RUNTIME       = "python"
      SCM_DO_BUILD_DURING_DEPLOYMENT = "true"
      ENABLE_ORYX_BUILD              = "true"
    },
    local.service_app_settings[each.value]
  )

  # The zip-deploy step sets WEBSITE_RUN_FROM_PACKAGE once Oryx finishes building. That setting is
  # deliberately NOT declared above, so ignore it here: otherwise the second apply (the one that wires
  # the Event Grid subscriptions after publish) sees it as drift and strips it, un-deploying the
  # function — fatal for the Event Grid subscriptions, which target these functions by their live
  # webhook (see the wire_eventgrid_subscriptions resources below).
  lifecycle {
    ignore_changes = [app_settings["WEBSITE_RUN_FROM_PACKAGE"]]
  }
}

# ---------------------------------------------------------------------------------------------------
# Inter-service messaging — each transport used for what it's good at:
#   • Service Bus queues (point-to-point commands): orders -> payments -> shipping.
#   • Event Hub (fan-out stream): orders publishes order:placed -> inventory + notifications each read
#     their own consumer group.
#   • Event Grid (routed integration events): payments publishes payment:captured, shipping publishes
#     shipment:dispatched -> routed by event type to inventory/notifications/analytics.
# ---------------------------------------------------------------------------------------------------
resource "azurerm_servicebus_namespace" "this" {
  # A Service Bus namespace name may not end with "-sb"/"-mgmt" (reserved), so this is "-bus".
  name                = "${var.project}-bus"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  sku                 = "Standard" # cheapest SKU with queues + the throughput this demo needs
}

resource "azurerm_servicebus_queue" "payments" {
  name         = "payments"
  namespace_id = azurerm_servicebus_namespace.this.id
}

resource "azurerm_servicebus_queue" "shipping" {
  name         = "shipping"
  namespace_id = azurerm_servicebus_namespace.this.id
}

resource "azurerm_eventhub_namespace" "this" {
  name                = "${var.project}-eh"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  sku                 = "Standard" # Basic has no consumer groups beyond $Default, so no fan-out
  capacity            = 1
}

resource "azurerm_eventhub" "order_placed" {
  name              = "order-placed"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = 2
  message_retention = 1
}

# One consumer group per subscriber, so inventory and notifications each read the whole stream (fan-out).
resource "azurerm_eventhub_consumer_group" "inventory" {
  name                = "inventory"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.order_placed.name
  resource_group_name = data.azurerm_resource_group.this.name
}

resource "azurerm_eventhub_consumer_group" "notifications" {
  name                = "notifications"
  namespace_name      = azurerm_eventhub_namespace.this.name
  eventhub_name       = azurerm_eventhub.order_placed.name
  resource_group_name = data.azurerm_resource_group.this.name
}

resource "azurerm_eventgrid_topic" "this" {
  name                = "${var.project}-eg"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  # benzene.azure.EventGridMessageSender publishes the native Event Grid schema by default (matching
  # this topic's default input_schema, "EventGridSchema") — no CloudEvents conversion needed.
}

# Read only when we're wiring subscriptions (the apps must be published + warm by then) — the Event
# Grid extension's system key for each consumer app, used to build its webhook URL below.
data "azurerm_function_app_host_keys" "consumer" {
  for_each            = var.wire_eventgrid_subscriptions ? toset([for r in local.eventgrid_routes : r.service]) : []
  name                = azurerm_linux_function_app.service[each.value].name
  resource_group_name = data.azurerm_resource_group.this.name
}

# Subscribe via the Functions Event Grid extension **webhook** rather than azure_function_endpoint: the
# latter validates the endpoint through an ARM control-plane lookup of the function, which is unreliable
# for a fresh Consumption-plan deploy ("Destination endpoint not found … should pre-exist"). The webhook
# is validated against the *live* running function, which the Functions EG extension auto-answers — so
# the deploy must warm the consumer apps just before this apply (see the GitHub Actions workflow).
resource "azurerm_eventgrid_event_subscription" "route" {
  for_each             = var.wire_eventgrid_subscriptions ? local.eventgrid_routes : {}
  name                 = replace(each.key, "_", "-")
  scope                = azurerm_eventgrid_topic.this.id
  included_event_types = [each.value.event_type]

  webhook_endpoint {
    url = "https://${azurerm_linux_function_app.service[each.value.service].default_hostname}/runtime/webhooks/eventgrid?functionName=${each.value.function}&code=${data.azurerm_function_app_host_keys.consumer[each.value.service].event_grid_extension_config_key}"
  }
}

# ---------------------------------------------------------------------------------------------------
# The mesh Function App (NOT tagged for discovery) — a single Timer trigger with a managed identity
# that can read resources (discovery) and read/write the catalog blobs.
# ---------------------------------------------------------------------------------------------------
resource "azurerm_linux_function_app" "mesh" {
  name                = "${var.project}-mesh"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = data.azurerm_resource_group.this.location
  service_plan_id     = azurerm_service_plan.this.id

  storage_account_name       = azurerm_storage_account.this.name
  storage_account_access_key = azurerm_storage_account.this.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = var.python_version
    }
  }

  app_settings = {
    FUNCTIONS_WORKER_RUNTIME       = "python"
    SCM_DO_BUILD_DURING_DEPLOYMENT = "true"
    ENABLE_ORYX_BUILD              = "true"
    # Scope discovery explicitly to this deployment so a resource-group-scoped Reader can't widen the
    # sweep beyond what the role assignment below actually grants.
    MESH_SUBSCRIPTION_ID  = data.azurerm_client_config.current.subscription_id
    MESH_DISCOVERY_TAG    = var.service_tag_key
    MESH_BLOB_ACCOUNT_URL = azurerm_storage_account.this.primary_blob_endpoint
    MESH_BLOB_CONTAINER   = "$web"
    MESH_ARTIFACT_PREFIX  = "mesh"
  }

  # Same as the service apps: the mesh code is zip-deployed out-of-band, setting
  # WEBSITE_RUN_FROM_PACKAGE. Ignore it so the post-publish apply doesn't strip the package.
  lifecycle {
    ignore_changes = [app_settings["WEBSITE_RUN_FROM_PACKAGE"]]
  }
}

# A system-assigned identity's principal must propagate to Entra ID before a role assignment that
# references it will succeed; on a cold subscription the assignments below otherwise intermittently
# fail the first apply with PrincipalNotFound. A short delay after the Function App exists removes
# that race.
resource "time_sleep" "identity_propagation" {
  depends_on      = [azurerm_linux_function_app.mesh]
  create_duration = "30s"
}

# Discover: the mesh identity can read (list) the resources in the resource group
# (benzene.mesh_fleet.AzureDiscovery's client.resources.list()).
resource "azurerm_role_assignment" "mesh_reader" {
  scope                = data.azurerm_resource_group.this.id
  role_definition_name = "Reader"
  principal_id         = azurerm_linux_function_app.mesh.identity[0].principal_id
  depends_on           = [time_sleep.identity_propagation]
}

# Persist: the mesh identity can read/write the catalog blobs (benzene.mesh.BlobArtifactStore, which
# authenticates via DefaultAzureCredential — the Function App's own managed identity — not an access key).
resource "azurerm_role_assignment" "mesh_blob" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_function_app.mesh.identity[0].principal_id
  depends_on           = [time_sleep.identity_propagation]
}
