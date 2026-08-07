# The three mesh services, and the derived image URIs.
#
# `command` is the Lambda image entrypoint (the shared image; each function overrides image_config.command
# to select its handler). One HTTP API + one Lambda is created per entry, via for_each over this map.

locals {
  services = {
    orders   = { command = "orders_handler.handler" }
    payments = { command = "payments_handler.handler" }
    shipping = { command = "shipping_handler.handler" }
  }

  lambda_image = "${aws_ecr_repository.lambda.repository_url}:${var.image_tag}"
  host_image   = "${aws_ecr_repository.host.repository_url}:${var.image_tag}"

  # The Mesh Host's public base URL (App Runner serves HTTPS on the service_url host).
  host_url = "https://${aws_apprunner_service.host.service_url}"

  # Peer API base URLs, injected into every service's env so orders → payments/shipping and
  # payments → shipping resolve (a service ignores the peer URLs it doesn't call).
  peer_env = {
    BENZENE_PEER_PAYMENTS_URL = aws_apigatewayv2_api.this["payments"].api_endpoint
    BENZENE_PEER_SHIPPING_URL = aws_apigatewayv2_api.this["shipping"].api_endpoint
  }

  # The shared secret env, present only when a key is configured (else the feeds stay open).
  mesh_key_env = var.mesh_key == "" ? {} : { BENZENE_MESH_KEY = var.mesh_key }
}
