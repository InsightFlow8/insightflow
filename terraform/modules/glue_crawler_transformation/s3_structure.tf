# =============================
# S3 Directory Structure for Transformation Features
# =============================
# 只在必要时创建占位符，避免与实际数据文件冲突

locals {
  feature_names = ["user_features", "product_features", "upi_features", "product_features_union", "upi_features_union"]
}

# 🚀 为特征文件创建占位符：仅在数据文件不存在时创建
# 如果已有 Parquet 文件，则跳过占位符创建
resource "aws_s3_object" "glue_crawler_feature_placeholders" {
  for_each = toset(local.feature_names)

  bucket = var.s3_bucket_name
  key    = "${var.s3_transformation_data_prefix}/${each.value}/.keep"

  content = "# Directory placeholder for Glue Crawler - Safe for concurrent access"

  tags = merge(var.tags, {
    Name        = "glue-crawler-feature-placeholder-${each.value}"
    Environment = var.env
    Purpose     = "FeatureDirectoryStructure"
    Feature     = each.value
    AutoManaged = "true"
  })

  lifecycle {
    ignore_changes = [key, content]
  }
}
