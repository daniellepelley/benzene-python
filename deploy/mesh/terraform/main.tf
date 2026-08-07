# The Benzene mesh collector (Mesh Host) on AWS Fargate, behind a public Application Load Balancer.
# Uses the account's default VPC + subnets to keep a demo stack small; point the data sources at a
# dedicated VPC for anything real.

locals {
  name = var.name_prefix
  tags = merge({ Project = "benzene-mesh", ManagedBy = "terraform" }, var.tags)
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# --- Container registry for the Mesh Host image -------------------------------------------------
resource "aws_ecr_repository" "collector" {
  name                 = "${local.name}-collector"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
  tags = local.tags
}

# --- Logs --------------------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "collector" {
  name              = "/ecs/${local.name}-collector"
  retention_in_days = 14
  tags              = local.tags
}

# --- IAM: task execution (pull image, write logs) and the (minimal) task role -------------------
data "aws_iam_policy_document" "assume_ecs" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-exec"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# The task role the collector runs as. The HTTP poller needs no AWS permissions; grant lambda:InvokeFunction
# here when you add a LambdaServiceSource that invokes Lambda-hosted services directly.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.assume_ecs.json
  tags               = local.tags
}

# --- Networking: an ALB open on :80, forwarding to the collector on its container port ----------
resource "aws_security_group" "alb" {
  name        = "${local.name}-alb"
  description = "Ingress to the mesh ALB"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.ingress_cidrs
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

resource "aws_security_group" "service" {
  name        = "${local.name}-svc"
  description = "The collector task; only the ALB may reach it"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "From the ALB"
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"] # the poller reaches the fleet over the internet
  }
  tags = local.tags
}

resource "aws_lb" "mesh" {
  name               = "${local.name}-alb"
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = data.aws_subnets.default.ids
  tags               = local.tags
}

resource "aws_lb_target_group" "collector" {
  name        = "${local.name}-tg"
  port        = var.container_port
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/benzene/health" # the Mesh Host is itself a profile-conformant service
    matcher             = "200"
    interval            = 30
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
  tags = local.tags
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.mesh.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.collector.arn
  }
}

# --- ECS: cluster, task definition, service ----------------------------------------------------
resource "aws_ecs_cluster" "mesh" {
  name = local.name
  tags = local.tags
}

resource "aws_ecs_task_definition" "collector" {
  family                   = "${local.name}-collector"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "collector"
      image     = var.collector_image
      essential = true
      portMappings = [
        { containerPort = var.container_port, protocol = "tcp" }
      ]
      environment = [
        { name = "PORT", value = tostring(var.container_port) },
        { name = "MESH_SERVICES", value = local.effective_mesh_services }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.collector.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "collector"
        }
      }
    }
  ])
  tags = local.tags
}

resource "aws_ecs_service" "collector" {
  name            = "${local.name}-collector"
  cluster         = aws_ecs_cluster.mesh.id
  task_definition = aws_ecs_task_definition.collector.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = true # default subnets are public; the task needs egress to poll the fleet
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.collector.arn
    container_name   = "collector"
    container_port   = var.container_port
  }

  depends_on = [aws_lb_listener.http]
  tags       = local.tags
}
