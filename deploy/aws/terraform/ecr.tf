# ECR repositories for the two images this stack runs: the shared service-Lambda image and the Mesh
# Host image. Create these FIRST (a targeted apply), build + push the images, then apply the rest — a
# Lambda/App Runner service can only reference an image tag that already exists in the repo.
#
# force_delete = true so `terraform destroy` removes the repo even with images still in it (demo-grade;
# drop it for a long-lived environment).

resource "aws_ecr_repository" "lambda" {
  name                 = "${var.name_prefix}-lambda"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "host" {
  name                 = "${var.name_prefix}-host"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Expire untagged images so the repos don't grow unbounded across rebuilds.
resource "aws_ecr_lifecycle_policy" "lambda" {
  repository = aws_ecr_repository.lambda.name
  policy     = local.ecr_expire_untagged
}

resource "aws_ecr_lifecycle_policy" "host" {
  repository = aws_ecr_repository.host.name
  policy     = local.ecr_expire_untagged
}

locals {
  ecr_expire_untagged = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Expire untagged images after 14 days"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 14
      }
      action = { type = "expire" }
    }]
  })
}
