# Inputs to the Benzene mesh AWS stack. Everything has a sensible default except the two image URIs,
# which point at the images you build + push to the ECR repos this stack creates (see the runbook).

variable "aws_region" {
  description = "AWS region to deploy the mesh into."
  type        = string
  default     = "eu-west-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name, so multiple meshes can coexist in one account."
  type        = string
  default     = "benzene-mesh"
}

variable "image_tag" {
  description = "The tag of the Lambda + host images to deploy (built + pushed to the ECR repos here)."
  type        = string
  default     = "latest"
}

variable "mesh_key" {
  description = <<-EOT
    The optional shared secret guarding the collector's ingest feeds. Empty (the default) leaves the
    collector open — the services attach no key and the host requires none. Set it to turn the simple
    shared-secret auth on: it is stored as an SSM SecureString AND injected as the BENZENE_MESH_KEY env
    var on both the services and the host, so their keys always match.
  EOT
  type        = string
  default     = ""
  sensitive   = true
}

variable "discovery_mode" {
  description = "How the host builds its service registry: 'lambda' (tag discovery) or 'static'."
  type        = string
  default     = "lambda"

  validation {
    condition     = contains(["lambda", "static"], var.discovery_mode)
    error_message = "discovery_mode must be 'lambda' or 'static'."
  }
}

variable "enrich" {
  description = "Wire the X-Ray topology + CloudWatch usage enrichment sources into the host (recommended)."
  type        = bool
  default     = true
}

variable "poll_interval_seconds" {
  description = "Seconds between the host's aggregation passes."
  type        = number
  default     = 60
}

variable "lambda_memory_mb" {
  description = "Memory (MB) for each service Lambda."
  type        = number
  default     = 256
}

variable "lambda_timeout_seconds" {
  description = "Timeout (s) for each service Lambda (must cover a fan-out call to its peers)."
  type        = number
  default     = 15
}

variable "host_cpu" {
  description = "App Runner vCPU for the Mesh Host (e.g. '0.25 vCPU', '0.5 vCPU', '1 vCPU')."
  type        = string
  default     = "0.25 vCPU"
}

variable "host_memory" {
  description = "App Runner memory for the Mesh Host (e.g. '0.5 GB', '1 GB', '2 GB')."
  type        = string
  default     = "0.5 GB"
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the Lambda log groups."
  type        = number
  default     = 14
}
