# =============================
# 简化版IAM策略配置
# 专注于增量爬取的核心权限需求
# =============================

# Get current AWS account ID and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Glue Crawler IAM Role
resource "aws_iam_role" "glue_crawler_role" {
  name = "${var.env}-glue-crawler-role-simple"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.tags, {
    Name        = "${var.env}-glue-crawler-role-simple"
    Environment = var.env
    Purpose     = "SimpleIncrementalCrawling"
  })
}

# AWS管理的Glue服务策略（基础权限）
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# 增强版S3访问策略 - 解决 InvalidAccessKeyId 问题
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.env}-glue-s3-access-enhanced"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:ListBucketVersions",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning",
          "s3:GetBucketAcl",
          "s3:GetObjectAcl"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      },
      {
        # 允许列出所有存储桶（Glue验证需要）
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets",
          "s3:GetBucketLocation"
        ]
        Resource = "*"
      }
    ]
  })
}

# 简化版Glue Catalog访问策略
resource "aws_iam_role_policy" "glue_catalog_access" {
  name = "${var.env}-glue-catalog-access-simple"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:UpdateDatabase",
          "glue:UpdateTable",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.database_name}/*"
        ]
      }
    ]
  })
}

# 简化版CloudWatch日志权限
resource "aws_iam_role_policy" "glue_cloudwatch_logs" {
  name = "${var.env}-glue-logs-simple"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# 🔧 额外权限策略 - 解决 Glue Crawler 验证问题
resource "aws_iam_role_policy" "glue_additional_permissions" {
  name = "${var.env}-glue-additional-permissions"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # 允许 Glue 服务验证 S3 路径存在
        Effect = "Allow"
        Action = [
          "s3:HeadBucket",
          "s3:HeadObject",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      },
      {
        # 允许 Glue 获取当前区域信息
        Effect = "Allow"
        Action = [
          "sts:GetCallerIdentity",
          "iam:GetRole",
          "iam:PassRole"
        ]
        Resource = "*"
      }
    ]
  })
}
