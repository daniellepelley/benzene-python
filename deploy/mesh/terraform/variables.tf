variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "name_prefix" {
  description = "Prefix for all resource names/tags."
  type        = string
  default     = "benzene-mesh"
}

variable "collector_image" {
  description = "Full image URI:tag for the Mesh Host container (push to the ECR repo this stack creates, then set this and re-apply)."
  type        = string
}

variable "mesh_services_json" {
  description = "Inline JSON fleet config for the collector (the MESH_SERVICES env var) — {pollIntervalSeconds, services:[{name,baseUrl,prefix?}]}."
  type        = string
  default     = "{\"pollIntervalSeconds\":30,\"services\":[]}"
}

variable "desired_count" {
  description = "Number of collector tasks."
  type        = number
  default     = 1
}

variable "cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)."
  type        = number
  default     = 256
}

variable "memory" {
  description = "Fargate task memory (MiB)."
  type        = number
  default     = 512
}

variable "container_port" {
  description = "Port the Mesh Host listens on (matches the container's PORT)."
  type        = number
  default     = 8080
}

variable "ingress_cidrs" {
  description = "CIDRs allowed to reach the ALB (lock this down — defaults to open for a demo)."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "tags" {
  description = "Extra tags applied to every resource."
  type        = map(string)
  default     = {}
}
