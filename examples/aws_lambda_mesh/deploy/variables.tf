variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"
}

variable "project" {
  description = "Name prefix for all resources."
  type        = string
  default     = "benzene-python-awsmesh"
}

variable "discovery_tag_key" {
  description = "Tag key the six service Lambdas carry (value \"true\") and AwsLambdaDiscovery filters on. The mesh Lambda deliberately does not carry it, so it never discovers itself."
  type        = string
  default     = "benzene"
}

variable "lambda_runtime" {
  description = "Python runtime for every Lambda in this stack (matches this repo's deploy/mesh/terraform/fleet.tf convention)."
  type        = string
  default     = "python3.12"
}

variable "lambda_architecture" {
  description = "Lambda instruction set architecture."
  type        = string
  default     = "x86_64"
}

variable "service_zip" {
  description = "Path to the service Lambda deployment zip (build it with deploy/build_service.sh)."
  type        = string
  default     = "./build/service.zip"
}

variable "mesh_zip" {
  description = "Path to the mesh Lambda deployment zip (build it with deploy/build_mesh.sh)."
  type        = string
  default     = "./build/mesh.zip"
}

variable "artifact_bucket_name" {
  description = "S3 bucket name for the discovered registry + generated catalog artifacts. Empty (the default) derives one from the project name and account id."
  type        = string
  default     = ""
}

variable "aggregate_schedule" {
  description = "EventBridge schedule expression that fires the mesh Lambda's discover -> aggregate pass."
  type        = string
  default     = "rate(5 minutes)"
}

variable "viewer_html" {
  description = "Path to the static Mesh UI viewer page uploaded alongside the catalog (the vendored canonical mesh-ui.html)."
  type        = string
  default     = "../web/index.html"
}
