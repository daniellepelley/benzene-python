# Provider + version pins for the Benzene mesh AWS stack.
#
# One AWS provider, one region. `default_tags` stamps every resource with project/ownership tags; the
# mesh-discovery tags (`benzene`, `benzene:mesh-url`, `benzene:mesh-path`) are set explicitly on the
# service Lambdas in lambda.tf, because those are the tags the Mesh Host's AwsLambdaDiscoveryProvider
# reads (list_functions + list_tags) to find the fleet.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "benzene-mesh"
      ManagedBy = "terraform"
    }
  }
}
