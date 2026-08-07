# What you need after `terraform apply`: where to push images, where to drive traffic, where the UI is.

output "ecr_lambda_repository_url" {
  description = "Push the shared service-Lambda image here (tag it with var.image_tag)."
  value       = aws_ecr_repository.lambda.repository_url
}

output "ecr_host_repository_url" {
  description = "Push the Mesh Host image here (tag it with var.image_tag)."
  value       = aws_ecr_repository.host.repository_url
}

output "service_api_urls" {
  description = "Each service's HTTP API base URL (drive orders here; {url}/benzene/spec is the spec)."
  value       = { for name, api in aws_apigatewayv2_api.this : name => api.api_endpoint }
}

output "orders_api_url" {
  description = "Convenience: the orders API base URL — POST {url}/orders to drive the mesh."
  value       = aws_apigatewayv2_api.this["orders"].api_endpoint
}

output "host_url" {
  description = "The Mesh Host base URL — open it in a browser for the live mesh UI."
  value       = local.host_url
}

output "mesh_key_ssm_parameter" {
  description = "Name of the SSM SecureString holding the shared secret (empty when no key is set)."
  value       = var.mesh_key == "" ? "" : aws_ssm_parameter.mesh_key[0].name
}
