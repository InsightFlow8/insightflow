# =============================
# S3 Directory Structure for Glue Crawlers
# =============================
# 🚀 简化版：总是创建占位符，但使用智能内容管理

locals {
  table_names = ["orders", "products", "departments", "aisles", "order_products_prior", "order_products_train"]
}

# 🚀 简化占位符：总是创建，但设计为不干扰真实数据
resource "aws_s3_object" "glue_crawler_placeholders" {
  for_each = toset(local.table_names)

  bucket = var.s3_bucket_name
  key    = "${var.s3_raw_data_prefix}/${each.value}/_placeholder_for_crawler.txt"

  content = <<EOF
# Glue Crawler 占位符
# Purpose: 确保 S3 目录存在以供 Glue Crawler 部署使用
# Status: 此文件不会干扰真实数据处理

Table: ${each.value}
S3 Path: s3://${var.s3_bucket_name}/${var.s3_raw_data_prefix}/${each.value}/
Environment: ${var.env}
Auto-managed: true

# 📝 说明：
# - 此文件确保目录结构存在
# - Glue Crawler 会忽略此占位符文件
# - 真实数据到达时，此文件不会影响处理
EOF

  tags = merge(var.tags, {
    Name        = "glue-crawler-placeholder-${each.value}"
    Environment = var.env
    Purpose     = "DirectoryStructure"
    Table       = each.value
    AutoManaged = "true"
  })

  lifecycle {
    ignore_changes = [key]
  }
}

// ...existing code...
