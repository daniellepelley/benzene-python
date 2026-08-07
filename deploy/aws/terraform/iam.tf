# Least-privilege IAM for the mesh.
#
# Three roles:
#   * lambda_exec       — the service Lambdas' execution role: CloudWatch Logs + X-Ray write (for the
#                         active tracing the functions enable).
#   * host_instance     — the Mesh Host's App Runner *instance* role (its runtime AWS calls):
#                         Lambda-tag discovery, X-Ray service graph (topology), CloudWatch metrics (usage).
#   * apprunner_ecr     — the App Runner *access* role: pull the host image from ECR.

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.name_prefix}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# CloudWatch Logs (the managed basic-execution policy).
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# X-Ray write — the minimum an active-tracing function needs to ship segments.
data "aws_iam_policy_document" "lambda_xray_write" {
  statement {
    sid       = "XRayWrite"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"] # X-Ray write actions do not support resource-level scoping.
  }
}

resource "aws_iam_role_policy" "lambda_xray_write" {
  name   = "xray-write"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_xray_write.json
}

# --- the Mesh Host instance role -----------------------------------------------------------------
data "aws_iam_policy_document" "host_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["tasks.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host_instance" {
  name               = "${var.name_prefix}-host-instance"
  assume_role_policy = data.aws_iam_policy_document.host_assume.json
}

# Exactly the three read capabilities the host's three sources need — discovery, topology, usage — plus
# its own logs. All are account-level read APIs that do not support resource-level scoping, so resources
# is "*"; the actions themselves are the least-privilege boundary.
data "aws_iam_policy_document" "host_permissions" {
  statement {
    sid       = "LambdaTagDiscovery"
    actions   = ["lambda:ListFunctions", "lambda:ListTags"]
    resources = ["*"]
  }
  statement {
    sid       = "XRayTopology"
    actions   = ["xray:GetServiceGraph", "xray:BatchGetTraces"]
    resources = ["*"]
  }
  statement {
    sid = "CloudWatchUsage"
    actions = [
      "cloudwatch:GetMetricData",
      "cloudwatch:GetMetricStatistics",
      "cloudwatch:ListMetrics",
    ]
    resources = ["*"]
  }
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "host_permissions" {
  name   = "mesh-host"
  role   = aws_iam_role.host_instance.id
  policy = data.aws_iam_policy_document.host_permissions.json
}

# --- the App Runner ECR-access role (pull the host image) ----------------------------------------
data "aws_iam_policy_document" "apprunner_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["build.apprunner.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "apprunner_ecr" {
  name               = "${var.name_prefix}-apprunner-ecr"
  assume_role_policy = data.aws_iam_policy_document.apprunner_assume.json
}

resource "aws_iam_role_policy_attachment" "apprunner_ecr" {
  role       = aws_iam_role.apprunner_ecr.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}
