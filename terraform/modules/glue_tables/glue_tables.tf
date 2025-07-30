# =============================
# 简化版增量爬取 AWS Glue Crawlers 
# 专注于核心增量功能，去除复杂配置
# =============================

# Glue Catalog Database
resource "aws_glue_catalog_database" "raw_data_catalog" {
  name        = var.raw_database_name
  description = "简化版原始数据目录 - 支持增量爬取"

  tags = merge(var.tags, {
    Name        = var.raw_database_name
    Environment = var.env
    Purpose     = "SimpleIncrementalCrawling"
  })
}

locals {
  raw_data_path = "s3://${var.s3_raw_bucket_name}/${var.s3_raw_data_prefix}"
}

module "departments_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}departments"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/departments/"
  columns = [
    { name = "department_id", type = "int" },
    { name = "department", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}

module "aisles_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}aisles"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/aisles/"
  columns = [
    { name = "aisle_id", type = "int" },
    { name = "aisle", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}

module "products_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}products"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/products/"
  columns = [
    { name = "product_id", type = "int" },
    { name = "product_name", type = "string" },
    { name = "aisle_id", type = "int" },
    { name = "department_id", type = "int" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}

module "orders_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}orders"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/orders/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "user_id", type = "int" },
    { name = "eval_set", type = "string" },
    { name = "order_number", type = "int" },
    { name = "order_dow", type = "int" },
    { name = "order_hour_of_day", type = "int" },
    { name = "days_since_prior", type = "int" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}

module "order_products__prior_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}order_products_prior"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/order_products_prior/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "product_id", type = "int" },
    { name = "add_to_cart_order", type = "int" },
    { name = "reordered", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}

module "order_products__train_table" {
  source        = "./table"
  name          = "${var.raw_table_prefix}order_products_train"
  database_name = var.raw_database_name
  location      = "${local.raw_data_path}/order_products_train/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "product_id", type = "int" },
    { name = "add_to_cart_order", type = "int" },
    { name = "reordered", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.raw_data_catalog]
}
