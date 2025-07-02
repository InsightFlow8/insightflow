# =============================
# Terraform for AWS Glue Crawlers (One Crawler per Table)
# Updated to match new S3 layout:
# s3://imba-test-aaron-landing/data/batch/{table_name}/year=YYYY/month=MM/day=DD/hhmm=HHMM/{table}_part{x}.csv
# =============================

resource "aws_glue_catalog_database" "landing_db" {
  name        = "imba_landing_db"
  description = "Glue database for raw data catalog"
}

resource "aws_iam_role" "glue_role" {
  name = "glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "glue-crawler-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::imba-test-aaron-landing",
          "arn:aws:s3:::imba-test-aaron-landing/*"
        ]
      }
    ]
  })
}

# -----------------------------
# Orders Crawler
# -----------------------------
resource "aws_glue_crawler" "raw_orders" {
  name          = "crawler_raw_orders"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.landing_db.name
  table_prefix  = "raw_batch_"

  s3_target {
    path = "s3://${var.bucket_name}/${var.landing_prefix_base}/orders/"
  }

  configuration = jsonencode({
    Version = 1.0,
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# -----------------------------
# Products Crawler
# -----------------------------
resource "aws_glue_crawler" "raw_products" {
  name          = "crawler_raw_products"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.landing_db.name
  table_prefix  = "raw_batch_"

  s3_target {
    path = "s3://${var.bucket_name}/${var.landing_prefix_base}/products/"
  }

  configuration = jsonencode({
    Version = 1.0,
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# -----------------------------
# Departments Crawler
# -----------------------------
resource "aws_glue_crawler" "raw_departments" {
  name          = "crawler_raw_departments"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.landing_db.name
  table_prefix  = "raw_batch_"

  s3_target {
    path = "s3://${var.bucket_name}/${var.landing_prefix_base}/departments/"
  }

  configuration = jsonencode({
    Version = 1.0,
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

# -----------------------------
# Aisles Crawler
# -----------------------------
resource "aws_glue_crawler" "raw_aisles" {
  name          = "crawler_raw_aisles"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.landing_db.name
  table_prefix  = "raw_batch_"

  s3_target {
    path = "s3://${var.bucket_name}/${var.landing_prefix_base}/aisles/"
  }

  configuration = jsonencode({
    Version = 1.0,
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "DEPRECATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}