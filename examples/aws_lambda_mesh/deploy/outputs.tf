output "service_urls" {
  description = "Each service Lambda's HTTP API base URL (POST /orders on the orders one kicks off the cascade; every service also answers /benzene/invoke|health|spec)."
  value       = { for k in keys(local.services) : k => aws_apigatewayv2_api.service[k].api_endpoint }
}

output "service_function_names" {
  description = "The six service Lambdas' function names (what AwsLambdaDiscovery discovers, and what the mesh's registry.json lists)."
  value       = local.services
}

output "artifact_bucket" {
  description = "The S3 bucket the mesh Lambda publishes the catalog (and registry.json) to."
  value       = aws_s3_bucket.artifacts.id
}

output "mesh_ui_url" {
  description = "The static Mesh UI viewer, reading the catalog the mesh Lambda publishes under mesh/."
  value       = "${aws_s3_bucket_website_configuration.viewer.website_endpoint}/mesh/"
}

output "mesh_function_name" {
  description = "The mesh Lambda's function name (invoke on demand to run the discover -> aggregate pass immediately, instead of waiting for the schedule)."
  value       = aws_lambda_function.mesh.function_name
}
