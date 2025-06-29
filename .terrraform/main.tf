# 配置 AWS Provider，定义 AWS 区域 和 访问凭证
provider "aws" {
  region     = "ap-southeast-2"
  access_key = var.AWS_ACCESS_KEY_ID  # 从变量中读取 Access Key
  secret_key = var.SECRET_ACCESS_KEY  # 从变量中读取 Secret Key
}

# 定义一个 S3 bucket 资源
resource "aws_s3_bucket" "test_bucket" {
  bucket = "insightflow-bucket-test-tobby"  # S3 bucket 名称（全局唯一）
  acl    = "private"  # 访问权限，私有
}

# 定义变量：AWS_ACCESS_KEY_ID
variable "AWS_ACCESS_KEY_ID" {
  description = "AWS_ACCESS_KEY_ID"
}

# 定义变量：AWS_SECRET_ACCESS_KEY
variable "AWS_SECRET_ACCESS_KEY" {
  description = "AWS_SECRET_ACCESS_KEY"
} 
