# The Mesh Host on AWS App Runner.
#
# Why App Runner over Fargate: the host is exactly one long-lived stateless web service (collector +
# aggregator poll loop + UI) that needs a public HTTPS URL and a health check — App Runner gives that from
# an ECR image with a built-in load balancer, managed TLS, and autoscaling, and NO cluster / task
# definition / ALB / target group / listener / security groups / subnets to own. Fargate would add ~10
# resources for the same single container. The one constraint App Runner must honour here is the host's
# in-process collector catalog: run exactly ONE instance, so the scaling config below pins min = max = 1.

resource "aws_apprunner_auto_scaling_configuration_version" "host" {
  auto_scaling_configuration_name = "${var.name_prefix}-host"

  # One instance only — the collector state lives in this single process (a multi-replica host would need
  # a shared collector store, which is out of scope for this MVP).
  min_size = 1
  max_size = 1

  tags = {
    Name = "${var.name_prefix}-host"
  }
}

resource "aws_apprunner_service" "host" {
  service_name = "${var.name_prefix}-host"

  source_configuration {
    # Deploy an image tag explicitly; no auto-deploy so a rebuild only ships on the next terraform apply.
    auto_deployments_enabled = false

    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_ecr.arn # pull the image from ECR
    }

    image_repository {
      image_identifier      = local.host_image
      image_repository_type = "ECR"

      image_configuration {
        port = "8080" # uvicorn host_app:app --port 8080

        runtime_environment_variables = merge(
          {
            BENZENE_MESH_DISCOVERY     = var.discovery_mode
            BENZENE_MESH_ENRICH        = var.enrich ? "1" : "0"
            BENZENE_MESH_POLL_INTERVAL = tostring(var.poll_interval_seconds)
            BENZENE_MESH_TAG_KEY       = "benzene"
            # Static-mode registry: the three service API base URLs, known at plan time. Ignored in
            # lambda (discovery) mode, but always correct, so switching discovery_mode needs no rebuild.
            BENZENE_MESH_REGISTRY = jsonencode([
              for name, _ in local.services : {
                name    = name
                baseUrl = aws_apigatewayv2_api.this[name].api_endpoint
              }
            ])
          },
          local.mesh_key_env, # BENZENE_MESH_KEY, only when a key is configured
        )
      }
    }
  }

  instance_configuration {
    cpu               = var.host_cpu
    memory            = var.host_memory
    instance_role_arn = aws_iam_role.host_instance.arn # discovery + X-Ray + CloudWatch reads
  }

  health_check_configuration {
    protocol = "HTTP"
    path     = "/" # the mesh UI (served once the first aggregation pass has emitted the artifacts)
    interval = 10
    timeout  = 5
  }

  auto_scaling_configuration_arn = aws_apprunner_auto_scaling_configuration_version.host.arn
}
