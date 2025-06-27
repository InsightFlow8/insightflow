# provider.tf
# 配置 AWS Terraform 提供商信息

provider "aws" {
  region = var.aws_region
}
