# The three service Lambdas — one per entry in local.services, all from the same container image, each
# selecting its handler via image_config.command.
#
# Each function:
#   * enables X-Ray active tracing (tracing_config Active) — the topology the host reads from X-Ray;
#   * carries the mesh-discovery tags the host's AwsLambdaDiscoveryProvider filters on:
#       benzene           = "true"                       (the required membership tag)
#       benzene:mesh-url  = <this service's API base URL> (where {url}/benzene/spec is fetched)
#       benzene:mesh-path = "/benzene"                   (the well-known prefix; explicit for clarity)
#   * gets its wiring from the environment: the host URL (feeds), peer URLs (domain calls), the key.

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.services
  name              = "/aws/lambda/${var.name_prefix}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "service" {
  for_each = local.services

  function_name = "${var.name_prefix}-${each.key}"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = local.lambda_image
  memory_size   = var.lambda_memory_mb
  timeout       = var.lambda_timeout_seconds

  image_config {
    command = [each.value.command]
  }

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = merge(
      {
        BENZENE_MESH_HOST_URL = local.host_url
        BENZENE_INSTANCE_ID   = "${each.key}-1"
      },
      local.peer_env,
      local.mesh_key_env,
    )
  }

  tags = {
    "benzene"           = "true"
    "benzene:mesh-url"  = aws_apigatewayv2_api.this[each.key].api_endpoint
    "benzene:mesh-path" = "/benzene"
  }

  # The log group is created explicitly (above) with a retention; make Lambda wait for it.
  depends_on = [aws_cloudwatch_log_group.lambda]
}
