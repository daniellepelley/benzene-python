# One HTTP API per service (orders / payments / shipping).
#
# Why one API each, not one shared API with path prefixes: the Mesh Host discovers a service by its own
# base URL (the `benzene:mesh-url` tag) and then fetches `{base}/benzene/spec` + `/benzene/health` at the
# well-known prefix. A dedicated API per service gives each a clean base URL whose `$default` route
# forwards every path (`/orders`, `/benzene/spec`, …) to that service's Lambda unchanged — no stage-path
# or prefix-stripping games. Three tiny HTTP APIs cost nothing and keep the well-known paths honest.

resource "aws_apigatewayv2_api" "this" {
  for_each      = local.services
  name          = "${var.name_prefix}-${each.key}"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "this" {
  for_each               = local.services
  api_id                 = aws_apigatewayv2_api.this[each.key].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.service[each.key].invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

# `$default` catches every method + path and forwards it to the integration.
resource "aws_apigatewayv2_route" "default" {
  for_each  = local.services
  api_id    = aws_apigatewayv2_api.this[each.key].id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.this[each.key].id}"
}

# Auto-deploying `$default` stage → the invoke URL is exactly api_endpoint (no stage path segment).
resource "aws_apigatewayv2_stage" "default" {
  for_each    = local.services
  api_id      = aws_apigatewayv2_api.this[each.key].id
  name        = "$default"
  auto_deploy = true
}

# Let each API invoke its Lambda.
resource "aws_lambda_permission" "apigw" {
  for_each      = local.services
  statement_id  = "AllowInvokeFrom-${each.key}-Api"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.this[each.key].execution_arn}/*/*"
}
