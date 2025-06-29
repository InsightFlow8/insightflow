# 配置 AWS Provider，定义 AWS 区域 和 访问凭证
provider "aws" {
  region     = "ap-southeast-2"
  access_key = var.aws_access_key  # 从变量中读取 Access Key
  secret_key = var.aws_secret_key  # 从变量中读取 Secret Key
}

# 定义一个 S3 bucket 资源
resource "aws_s3_bucket" "test_bucket" {
  bucket = "insightflow-test-bucket-tobby"  # S3 bucket 名称（全局唯一）
  acl    = "private"  # 访问权限，私有
}

# 定义变量：aws_access_key
variable "aws_access_key" {
  description = "AWS Access Key"
}

# 定义变量：aws_secret_key
variable "aws_secret_key" {
  description = "AWS Secret Key"
}
