# =============================
# Glue Crawler for Transformation Features
# 爬取 transformation 后的特征数据，生成 Glue 表
# =============================

# -----------------------------
# User Features Crawler
# -----------------------------
resource "aws_glue_crawler" "user_features" {
  name          = "${var.env}_crawler_user_features"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = var.database_name
  table_prefix  = var.table_prefix
  description   = "User Features表爬虫 - 爬取用户特征数据"

  s3_target {
    path = "s3://${var.s3_bucket_name}/${var.s3_transformation_data_prefix}/user_features/"

    # 只包含 .parquet 文件，排除临时文件和占位符
    exclusions = [
      "**/_temporary/**",
      "**/.*",
      "**/_SUCCESS",
      "**/.keep",
      "*.txt"
    ]
  }

  # Schema变更策略 - 全量爬取，允许更新
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  # 全量爬取配置
  recrawl_policy {
    recrawl_behavior = var.recrawl_behavior
  }

  schedule = var.crawler_schedule

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy.glue_catalog_access,
    aws_iam_role_policy.glue_cloudwatch_logs,
    aws_iam_role_policy.glue_additional_permissions,
    aws_s3_object.glue_crawler_feature_placeholders
  ]

  tags = merge(var.tags, {
    Name        = "${var.env}_crawler_user_features"
    Environment = var.env
    DataType    = "user-features"
  })
}

# -----------------------------
# Product Features Crawler
# -----------------------------
resource "aws_glue_crawler" "product_features" {
  name          = "${var.env}_crawler_product_features"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = var.database_name
  table_prefix  = var.table_prefix
  description   = "Product Features表爬虫 - 爬取产品特征数据"

  s3_target {
    path = "s3://${var.s3_bucket_name}/${var.s3_transformation_data_prefix}/product_features/"

    exclusions = [
      "**/_temporary/**",
      "**/.*",
      "**/_SUCCESS",
      "**/.keep",
      "*.txt"
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = var.recrawl_behavior
  }

  schedule = var.crawler_schedule

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy.glue_catalog_access,
    aws_iam_role_policy.glue_cloudwatch_logs,
    aws_iam_role_policy.glue_additional_permissions,
    aws_s3_object.glue_crawler_feature_placeholders
  ]

  tags = merge(var.tags, {
    Name        = "${var.env}_crawler_product_features"
    Environment = var.env
    DataType    = "product-features"
  })
}

# -----------------------------
# UPI Features Crawler
# -----------------------------
resource "aws_glue_crawler" "upi_features" {
  name          = "${var.env}_crawler_upi_features"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = var.database_name
  table_prefix  = var.table_prefix
  description   = "UPI Features表爬虫 - 爬取用户-产品交互特征数据"

  s3_target {
    path = "s3://${var.s3_bucket_name}/${var.s3_transformation_data_prefix}/upi_features/"

    exclusions = [
      "**/_temporary/**",
      "**/.*",
      "**/_SUCCESS",
      "**/.keep",
      "*.txt"
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = var.recrawl_behavior
  }

  schedule = var.crawler_schedule

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy.glue_catalog_access,
    aws_iam_role_policy.glue_cloudwatch_logs,
    aws_iam_role_policy.glue_additional_permissions,
    aws_s3_object.glue_crawler_feature_placeholders
  ]

  tags = merge(var.tags, {
    Name        = "${var.env}_crawler_upi_features"
    Environment = var.env
    DataType    = "upi-features"
  })
}

# -----------------------------
# Product Features Union Crawler
# -----------------------------
resource "aws_glue_crawler" "product_features_union" {
  name          = "${var.env}_crawler_product_features_union"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = var.database_name
  table_prefix  = var.table_prefix
  description   = "Product Features Union表爬虫 - 爬取产品特征联合数据"

  s3_target {
    path = "s3://${var.s3_bucket_name}/${var.s3_transformation_data_prefix}/product_features_union/"

    exclusions = [
      "**/_temporary/**",
      "**/.*",
      "**/_SUCCESS",
      "**/.keep",
      "*.txt"
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = var.recrawl_behavior
  }

  schedule = var.crawler_schedule

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy.glue_catalog_access,
    aws_iam_role_policy.glue_cloudwatch_logs,
    aws_iam_role_policy.glue_additional_permissions,
    aws_s3_object.glue_crawler_feature_placeholders
  ]

  tags = merge(var.tags, {
    Name        = "${var.env}_crawler_product_features_union"
    Environment = var.env
    DataType    = "product-features-union"
  })
}

# -----------------------------
# UPI Features Union Crawler
# -----------------------------
resource "aws_glue_crawler" "upi_features_union" {
  name          = "${var.env}_crawler_upi_features_union"
  role          = aws_iam_role.glue_crawler_role.arn
  database_name = var.database_name
  table_prefix  = var.table_prefix
  description   = "UPI Features Union表爬虫 - 爬取用户-产品交互特征联合数据"

  s3_target {
    path = "s3://${var.s3_bucket_name}/${var.s3_transformation_data_prefix}/upi_features_union/"

    exclusions = [
      "**/_temporary/**",
      "**/.*",
      "**/_SUCCESS",
      "**/.keep",
      "*.txt"
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = var.recrawl_behavior
  }

  schedule = var.crawler_schedule

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_access,
    aws_iam_role_policy.glue_catalog_access,
    aws_iam_role_policy.glue_cloudwatch_logs,
    aws_iam_role_policy.glue_additional_permissions,
    aws_s3_object.glue_crawler_feature_placeholders
  ]

  tags = merge(var.tags, {
    Name        = "${var.env}_crawler_upi_features_union"
    Environment = var.env
    DataType    = "upi-features-union"
  })
}
