variable "location" {
  description = "Azure region to deploy into."
  type        = string
  default     = "westeurope"
}

variable "project" {
  description = "Name prefix for all resources (each Function App is \"<project>-<service>\")."
  type        = string
  default     = "benzene-py-fnmesh"
}

variable "resource_group" {
  description = "Resource group name. Bootstrapped imperatively by the workflow (az group create, idempotent) — it also holds the remote-state storage account — so Terraform reads it as a data source rather than owning it."
  type        = string
  default     = "benzene-py-fnmesh-rg"
}

variable "storage_account" {
  description = "Storage account for the Functions runtime AND the mesh catalog's static website (globally unique, lowercase alphanumeric, <=24 chars). Must differ from any other example's value — the remote state uses <name>tfstate, so a collision fails terraform init with a 404."
  type        = string
  default     = "benzenepyfnmesh"
}

variable "python_version" {
  description = "Python runtime version for every Function App in this stack."
  type        = string
  default     = "3.12"
}

variable "discovery_tag_key" {
  description = "Tag key the six service Function Apps carry (value \"true\"), matching this port's cross-example discovery-tag convention (deploy/mesh/terraform, examples/aws_lambda_mesh). The mesh Function App deliberately does not carry it, so it never discovers itself."
  type        = string
  default     = "benzene"
}

variable "service_tag_key" {
  description = "The Azure resource tag key benzene.mesh_fleet.AzureDiscovery actually reads for a discovered service's display name (its service_tag constructor arg, default \"benzene:service\") — distinct from discovery_tag_key, which merely marks a resource as mesh-eligible. See README 'Two tags, on purpose'."
  type        = string
  default     = "benzene:service"
}

variable "aggregate_schedule" {
  description = "NCRONTAB schedule (Azure Functions Timer trigger syntax: {second} {minute} {hour} {day} {month} {day-of-week}) that fires the mesh Function's discover -> aggregate pass. Must match mesh/function_app.py's hard-coded @app.timer_trigger schedule if changed (Terraform does not control the trigger schedule directly — it's in code, not config, for Azure Functions Python)."
  type        = string
  default     = "0 */5 * * * *"
}

variable "service_zip" {
  description = "Path to the service Function App deployment zip (build it with deploy/build_service.sh)."
  type        = string
  default     = "./build/service.zip"
}

variable "mesh_zip" {
  description = "Path to the mesh Function App deployment zip (build it with deploy/build_mesh.sh)."
  type        = string
  default     = "./build/mesh.zip"
}

variable "wire_eventgrid_subscriptions" {
  description = "Wire the Event Grid subscriptions onto the consumer Function Apps' webhook endpoints. False on the first apply (the target functions must exist and be warm first — see README); set true on the second apply, after publish + warm-up."
  type        = bool
  default     = false
}

variable "viewer_html" {
  description = "Path to the static Mesh UI viewer page uploaded to the storage account's $web static website (the vendored canonical mesh-ui.html)."
  type        = string
  default     = "../web/index.html"
}
