output "ecr_repository_url" {
  description = "Push the Mesh Host image here, then set var.collector_image to <this>:<tag>."
  value       = aws_ecr_repository.collector.repository_url
}

output "mesh_url" {
  description = "Base URL of the deployed mesh collector (e.g. GET <url>/mesh/fleet, <url>/benzene/health)."
  value       = "http://${aws_lb.mesh.dns_name}"
}

output "cluster" {
  description = "ECS cluster name (for `aws ecs` commands / log tailing)."
  value       = aws_ecs_cluster.mesh.name
}

output "log_group" {
  description = "CloudWatch log group for the collector task."
  value       = aws_cloudwatch_log_group.collector.name
}
