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
  description = "Inline JSON fleet config for the collector when deploy_fleet=false. Ignored when the fleet is deployed here (the collector is pointed at the fleet's own API Gateway URLs)."
  type        = string
  default     = "{\"pollIntervalSeconds\":30,\"services\":[]}"
}

variable "deploy_fleet" {
  description = "Also deploy the demo fleet (orders -> inventory -> notifications) as Lambdas + HTTP APIs, and point the collector at them."
  type        = bool
  default     = true
}

variable "fleet_zip" {
  description = "Path to the fleet Lambda deployment zip (build it with deploy/mesh/fleet/build.sh)."
  type        = string
  default     = "../build/fleet.zip"
}

variable "lambda_runtime" {
  description = "Python runtime for the fleet Lambdas."
  type        = string
  default     = "python3.12"
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
