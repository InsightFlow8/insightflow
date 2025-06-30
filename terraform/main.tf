# provider.tf
# 配置 AWS Terraform 提供商信息

provider "aws" {
  region = var.aws_region
}


terraform {
  required_version = "1.7.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
