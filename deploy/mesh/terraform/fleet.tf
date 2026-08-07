# The demo fleet: orders -> inventory -> notifications, each an env-driven Lambda (the same zip) behind
# an HTTP API Gateway. Each service exposes /benzene/spec + /benzene/health (so the collector polls it)
# and calls the next hop with trace propagation, pushing traces to the collector (so edges appear).
# Toggle with var.deploy_fleet; the collector is pointed at these services' URLs automatically.

locals {
  # Call chain and topics. `next` names the downstream service (or null for the leaf).
  fleet = var.deploy_fleet ? {
    notifications = { topic = "notify:send", next = null, next_topic = null }
    inventory     = { topic = "inventory:reserve", next = "notifications", next_topic = "notify:send" }
    orders        = { topic = "orders:place", next = "inventory", next_topic = "inventory:reserve" }
  } : {}

  collector_url = "http://${aws_lb.mesh.dns_name}"

  # Point the collector at the fleet's own API Gateway base URLs (poll /benzene/spec + /benzene/health).
  effective_mesh_services = var.deploy_fleet ? jsonencode({
    pollIntervalSeconds = 30
    services            = [for k in keys(local.fleet) : { name = k, baseUrl = aws_apigatewayv2_api.fleet[k].api_endpoint }]
  }) : var.mesh_services_json
}

# --- IAM: one execution role for the fleet Lambdas (logs only; the calls are plain HTTP) ---------
resource "aws_iam_role" "fleet" {
  count              = var.deploy_fleet ? 1 : 0
  name               = "${local.name}-fleet"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "fleet_logs" {
  count      = var.deploy_fleet ? 1 : 0
  role       = aws_iam_role.fleet[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# --- Per-service Lambda + HTTP API -------------------------------------------------------------
resource "aws_lambda_function" "fleet" {
  for_each = local.fleet

  function_name    = "${local.name}-${each.key}"
  role             = aws_iam_role.fleet[0].arn
  runtime          = var.lambda_runtime
  handler          = "fleet.service.handler"
  filename         = var.fleet_zip
  source_code_hash = filebase64sha256(var.fleet_zip)
  timeout          = 15

  environment {
    variables = {
      SERVICE_NAME  = each.key
      SERVICE_TOPIC = each.value.topic
      SERVICE_ROUTE = "/${each.key}"
      COLLECTOR_URL = local.collector_url
      # NEXT_URL is the downstream service's exact route URL ({api}/{name}); empty for the leaf.
      NEXT_URL   = each.value.next == null ? "" : "${aws_apigatewayv2_api.fleet[each.value.next].api_endpoint}/${each.value.next}"
      NEXT_TOPIC = each.value.next_topic == null ? "" : each.value.next_topic
    }
  }
  tags = local.tags
}

resource "aws_apigatewayv2_api" "fleet" {
  for_each      = local.fleet
  name          = "${local.name}-${each.key}"
  protocol_type = "HTTP"
  tags          = local.tags
}

resource "aws_apigatewayv2_integration" "fleet" {
  for_each               = local.fleet
  api_id                 = aws_apigatewayv2_api.fleet[each.key].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.fleet[each.key].invoke_arn
  payload_format_version = "1.0" # v1 event shape (httpMethod/path) — what benzene-aws parses
}

resource "aws_apigatewayv2_route" "fleet" {
  for_each  = local.fleet
  api_id    = aws_apigatewayv2_api.fleet[each.key].id
  route_key = "$default" # all paths reach the Lambda; BenzeneHttpApp routes internally
  target    = "integrations/${aws_apigatewayv2_integration.fleet[each.key].id}"
}

resource "aws_apigatewayv2_stage" "fleet" {
  for_each    = local.fleet
  api_id      = aws_apigatewayv2_api.fleet[each.key].id
  name        = "$default"
  auto_deploy = true
  tags        = local.tags
}

resource "aws_lambda_permission" "fleet" {
  for_each      = local.fleet
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fleet[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.fleet[each.key].execution_arn}/*/*"
}

output "fleet_urls" {
  description = "The deployed fleet services' base URLs (each serves /benzene/spec, /benzene/health, and /<name>)."
  value       = { for k in keys(local.fleet) : k => aws_apigatewayv2_api.fleet[k].api_endpoint }
}
