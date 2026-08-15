# Terraform for examples/aws_lambda_mesh — modelled closely on TypeScript's
# examples/aws-lambda-mesh/deploy/main.tf (same topology, same resource shapes); the divergences are
# Python-specific: a managed python3.12 runtime (no bundler — deploy/build_service.sh and
# deploy/build_mesh.sh produce the two zips via `pip install --target`), dotted-module handlers
# (aws_lambda_mesh.service.main.handler / aws_lambda_mesh.mesh.main.handler), and the catalog viewer is
# the canonical vendored mesh-ui.html (examples/k8s_mesh already vendors the same file) rather than a
# bespoke page. State: S3 remote backend (empty here; -backend-config supplies bucket/key/region at
# `terraform init`, same convention as examples/k8s_mesh/deploy and deploy/mesh/terraform).

terraform {
  required_version = ">= 1.5.0"
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  bucket_name = var.artifact_bucket_name != "" ? var.artifact_bucket_name : "${var.project}-${data.aws_caller_identity.current.account_id}"

  # The six Cloud Service Lambdas. orders/payments/shipping form the command chain and publish events;
  # inventory/notifications/analytics are pure event consumers.
  services = {
    orders        = "${var.project}-orders"
    payments      = "${var.project}-payments"
    shipping      = "${var.project}-shipping"
    inventory     = "${var.project}-inventory"
    notifications = "${var.project}-notifications"
    analytics     = "${var.project}-analytics"
  }

  # Per-service outbound targets, handed to each producer as env vars — service/host.py's
  # `_SQS_TARGETS` / `_SNS_TARGETS` / `_EVENTBRIDGE_TARGETS` read exactly these names. A stable
  # SERVICE_NAME var is always present so every function's environment block is non-empty and its
  # shape never changes across applies (avoids the AWS provider's "block count changed 0->1" plan bug
  # when a value like a queue URL is only known after apply).
  service_env = {
    orders        = { PAYMENTS_QUEUE_URL = aws_sqs_queue.payments.url, ORDER_PLACED_TOPIC_ARN = aws_sns_topic.order_placed.arn }
    payments      = { SHIPPING_QUEUE_URL = aws_sqs_queue.shipping.url, EVENT_BUS_NAME = aws_cloudwatch_event_bus.bus.name }
    shipping      = { EVENT_BUS_NAME = aws_cloudwatch_event_bus.bus.name }
    inventory     = {}
    notifications = {}
    analytics     = {}
  }

  # SNS fan-out: order:placed is delivered to each of these service Lambdas.
  sns_order_placed_subscribers = toset(["inventory", "notifications"])

  # EventBridge routing: one rule per integration event (matched on detail-type = the Benzene topic),
  # fanned out to the listed consumer Lambdas. Rule keys are slugs (no ':') for valid resource names.
  eventbridge_rules = {
    payment_captured    = { detail_type = "payment:captured", targets = ["notifications", "analytics"] }
    shipment_dispatched = { detail_type = "shipment:dispatched", targets = ["inventory", "notifications", "analytics"] }
  }

  # Flatten {rule -> [targets]} to individual (rule, service) pairs for the per-target resources.
  eventbridge_targets = merge([
    for rule_key, rule in local.eventbridge_rules : {
      for svc in rule.targets : "${rule_key}-${svc}" => { rule_key = rule_key, service = svc }
    }
  ]...)
}

# ---------------------------------------------------------------------------------------------------
# S3 bucket for the discovered registry + generated catalog artifacts (written by the mesh Lambda).
# ---------------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "artifacts" {
  bucket        = local.bucket_name
  force_destroy = true
}

# ---------------------------------------------------------------------------------------------------
# Static catalog viewer: the mesh writes its catalog (manifest/topics/topology.json, ...) under mesh/,
# and the vendored canonical mesh-ui.html (examples/k8s_mesh/mesh/ui/mesh-ui.html — same file) reads
# them with same-origin relative fetches. Serve it as an S3 static website so
# `http://<endpoint>/mesh/` resolves to mesh/index.html and its ./manifest.json etc. resolve to the
# catalog next to it — the lightweight, language-neutral stand-in for a Lambda-served UI (mirrors TS's
# deploy/main.tf choice; a live HTTP surface on the mesh Lambda itself was the other option but adds a
# whole API Gateway + BenzeneHttpApp wiring for no benefit over a static page reading the same JSON).
# NOTE: this makes the catalog under mesh/ publicly readable (demo default) — the catalog is
# non-sensitive service metadata; drop the public-read policy to keep it private.
# ---------------------------------------------------------------------------------------------------
resource "aws_s3_bucket_website_configuration" "viewer" {
  bucket = aws_s3_bucket.artifacts.id
  index_document {
    suffix = "index.html"
  }
}

resource "aws_s3_bucket_public_access_block" "viewer" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

data "aws_iam_policy_document" "viewer_public_read" {
  statement {
    sid       = "PublicReadMeshCatalog"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/mesh/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "viewer" {
  bucket     = aws_s3_bucket.artifacts.id
  policy     = data.aws_iam_policy_document.viewer_public_read.json
  depends_on = [aws_s3_bucket_public_access_block.viewer]
}

resource "aws_s3_object" "viewer" {
  bucket       = aws_s3_bucket.artifacts.id
  key          = "mesh/index.html"
  source       = var.viewer_html
  etag         = filemd5(var.viewer_html)
  content_type = "text/html"
}

# ---------------------------------------------------------------------------------------------------
# IAM: a shared execution+messaging role for the service Lambdas, and a discover+invoke+S3 role for
# the mesh.
# ---------------------------------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  name               = "${var.project}-service-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "service_logs" {
  role       = aws_iam_role.service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role" "mesh" {
  name               = "${var.project}-mesh-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "mesh_logs" {
  role       = aws_iam_role.mesh.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "mesh" {
  # Discover: list all functions and read their tags (AwsLambdaDiscovery).
  statement {
    actions   = ["lambda:ListFunctions", "lambda:ListTags"]
    resources = ["*"]
  }
  # Interrogate: invoke the discovered service functions directly (benzene:mesh / benzene:healthcheck).
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [for s in local.services : "arn:aws:lambda:${var.region}:${data.aws_caller_identity.current.account_id}:function:${s}"]
  }
  # Persist the registry + catalog artifacts.
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }
}

resource "aws_iam_role_policy" "mesh" {
  name   = "${var.project}-mesh-policy"
  role   = aws_iam_role.mesh.id
  policy = data.aws_iam_policy_document.mesh.json
}

# The shared service role's producer permissions: send to both queues (and consume them — the
# event-source mapping polls with the function's role), publish the SNS topic, put events on the bus,
# and push trace batches to the mesh Lambda (a direct invoke — see MESH_FUNCTION_NAME above — so the
# collector can derive consumer edges; mirrors the mesh role's own invoke permission on the six
# services, just in the opposite direction).
data "aws_iam_policy_document" "service_messaging" {
  statement {
    actions   = ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.payments.arn, aws_sqs_queue.shipping.arn]
  }
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.order_placed.arn]
  }
  statement {
    actions   = ["events:PutEvents"]
    resources = [aws_cloudwatch_event_bus.bus.arn]
  }
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.mesh.arn]
  }
}

resource "aws_iam_role_policy" "service_messaging" {
  name   = "${var.project}-service-messaging"
  role   = aws_iam_role.service.id
  policy = data.aws_iam_policy_document.service_messaging.json
}

# ---------------------------------------------------------------------------------------------------
# The six Cloud Service Lambdas (tagged for discovery) + one HTTP API each. One shared zip
# (deploy/build_service.sh); SERVICE_NAME picks the domain (service/startup.py's ServiceStartUp).
# ---------------------------------------------------------------------------------------------------
resource "aws_lambda_function" "service" {
  for_each = local.services

  function_name    = each.value
  role             = aws_iam_role.service.arn
  filename         = var.service_zip
  source_code_hash = filebase64sha256(var.service_zip)
  runtime          = var.lambda_runtime
  handler          = "aws_lambda_mesh.service.main.handler"
  architectures    = [var.lambda_architecture]
  memory_size      = 256
  timeout          = 30

  environment {
    variables = merge(
      { SERVICE_NAME = each.key, MESH_FUNCTION_NAME = aws_lambda_function.mesh.function_name },
      local.service_env[each.key]
    )
  }

  # Discovery finds services by this tag; the mesh Lambda deliberately does NOT carry it.
  tags = { (var.discovery_tag_key) = "true" }
}

# ---------------------------------------------------------------------------------------------------
# Runtime interconnectivity — each transport used for what it's good at:
#   - SQS (point-to-point commands): orders -> payments (payments:capture), payments -> shipping
#     (shipping:book). Each queue triggers its service Lambda (event-source mapping).
#   - SNS (fan-out event): orders publishes order:placed -> inventory AND notifications (subscriptions).
#   - EventBridge (routed integration events on a custom bus): payments publishes payment:captured,
#     shipping publishes shipment:dispatched -> routed by rule to notifications/inventory/analytics.
# ---------------------------------------------------------------------------------------------------

# --- SQS: the point-to-point command hops -----------------------------------------------------------
resource "aws_sqs_queue" "payments" {
  name                       = "${var.project}-payments-queue"
  visibility_timeout_seconds = 60
}

resource "aws_sqs_queue" "shipping" {
  name                       = "${var.project}-shipping-queue"
  visibility_timeout_seconds = 60
}

resource "aws_lambda_event_source_mapping" "payments" {
  event_source_arn = aws_sqs_queue.payments.arn
  function_name    = aws_lambda_function.service["payments"].arn
  batch_size       = 1
}

resource "aws_lambda_event_source_mapping" "shipping" {
  event_source_arn = aws_sqs_queue.shipping.arn
  function_name    = aws_lambda_function.service["shipping"].arn
  batch_size       = 1
}

# --- SNS: the order:placed fan-out ------------------------------------------------------------------
resource "aws_sns_topic" "order_placed" {
  name = "${var.project}-order-placed"
}

resource "aws_sns_topic_subscription" "order_placed" {
  for_each  = local.sns_order_placed_subscribers
  topic_arn = aws_sns_topic.order_placed.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.service[each.key].arn
}

resource "aws_lambda_permission" "sns_invoke" {
  for_each      = local.sns_order_placed_subscribers
  statement_id  = "AllowSnsInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service[each.key].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.order_placed.arn
}

# --- EventBridge: the routed integration events on a dedicated bus -----------------------------------
resource "aws_cloudwatch_event_bus" "bus" {
  name = "${var.project}-bus"
}

resource "aws_cloudwatch_event_rule" "integration" {
  for_each       = local.eventbridge_rules
  name           = "${var.project}-${each.key}"
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  event_pattern  = jsonencode({ "detail-type" = [each.value.detail_type] })
}

resource "aws_cloudwatch_event_target" "integration" {
  for_each       = local.eventbridge_targets
  rule           = aws_cloudwatch_event_rule.integration[each.value.rule_key].name
  event_bus_name = aws_cloudwatch_event_bus.bus.name
  target_id      = each.value.service
  arn            = aws_lambda_function.service[each.value.service].arn
}

resource "aws_lambda_permission" "eventbridge_invoke" {
  for_each      = local.eventbridge_targets
  statement_id  = "AllowEventBridge-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service[each.value.service].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.integration[each.value.rule_key].arn
}

# ---------------------------------------------------------------------------------------------------
# One HTTP API per service: a $default catch-all proxies the full path through. The meaningful route is
# orders' POST /orders (kicks off the cascade); the others expose each service's HTTP domain surface
# (/benzene/invoke, /benzene/health, /benzene/spec — StandardPaths).
# ---------------------------------------------------------------------------------------------------
resource "aws_apigatewayv2_api" "service" {
  for_each      = local.services
  name          = "${each.value}-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "service" {
  for_each               = local.services
  api_id                 = aws_apigatewayv2_api.service[each.key].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.service[each.key].invoke_arn
  payload_format_version = "1.0" # v1 event shape (httpMethod/path) — what benzene.aws.events parses
}

resource "aws_apigatewayv2_route" "service" {
  for_each  = local.services
  api_id    = aws_apigatewayv2_api.service[each.key].id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.service[each.key].id}"
}

resource "aws_apigatewayv2_stage" "service" {
  for_each    = local.services
  api_id      = aws_apigatewayv2_api.service[each.key].id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "service_api" {
  for_each      = local.services
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.service[each.key].execution_arn}/*/*"
}

# ---------------------------------------------------------------------------------------------------
# The mesh Lambda (NOT tagged for discovery) answers two invoke shapes on the one handler:
#   - the EventBridge schedule's constant payload (or an on-demand invoke with no "topic") -> runs the
#     discover -> interrogate -> aggregate pass, returning a plain summary dict.
#   - a direct Lambda invoke carrying a Benzene envelope ({"topic": "benzene:mesh:traces", ...}, pushed
#     by a service Lambda after each invocation via MESH_FUNCTION_NAME) -> ingested straight into the
#     persistent collector (MESH_STATE_KEY), so consumer edges derived from those traces are already in
#     the catalog the next scheduled aggregation pass publishes.
# It has no HTTP API of its own; read its output from the S3 bucket / the static viewer.
# ---------------------------------------------------------------------------------------------------
resource "aws_lambda_function" "mesh" {
  function_name    = "${var.project}-mesh"
  role             = aws_iam_role.mesh.arn
  filename         = var.mesh_zip
  source_code_hash = filebase64sha256(var.mesh_zip)
  runtime          = var.lambda_runtime
  handler          = "aws_lambda_mesh.mesh.main.handler"
  architectures    = [var.lambda_architecture]
  memory_size      = 256
  timeout          = 60

  environment {
    variables = {
      MESH_ARTIFACT_BUCKET   = aws_s3_bucket.artifacts.id
      MESH_ARTIFACT_PREFIX   = "mesh"
      MESH_DISCOVERY_TAG_KEY = var.discovery_tag_key
      # Outside the public mesh/* prefix (aws_s3_bucket_policy.viewer) -- the collector snapshot is
      # internal aggregator state, not part of the published catalog the viewer reads.
      MESH_STATE_KEY = "_state/collector.json"
    }
  }
}

# Scheduled aggregation: fire the mesh Lambda on a schedule with a constant payload (the mesh handler
# ignores the event and always runs the discover -> aggregate pass).
resource "aws_cloudwatch_event_rule" "aggregate" {
  name                = "${var.project}-aggregate"
  schedule_expression = var.aggregate_schedule
}

resource "aws_cloudwatch_event_target" "aggregate" {
  rule      = aws_cloudwatch_event_rule.aggregate.name
  target_id = "mesh"
  arn       = aws_lambda_function.mesh.arn
  input     = jsonencode({ "detail-type" = "mesh:aggregate", "source" = "benzene.mesh", "detail" = {} })
}

resource "aws_lambda_permission" "mesh_events" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mesh.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aggregate.arn
}
