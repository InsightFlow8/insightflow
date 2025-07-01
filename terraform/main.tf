# provider.tf
# 配置 AWS Terraform 提供商信息

provider "aws" {
  region = var.aws_region
}


# 获取当前 AWS 账号信息（account_id, user_id, arn）
data "aws_caller_identity" "current" {}