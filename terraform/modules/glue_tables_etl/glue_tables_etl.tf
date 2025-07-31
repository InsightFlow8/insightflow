# =============================
# Glue Tables for ETL Clean Data
# 针对 after-clean 数据的 Glue table 配置
# =============================

# Glue Catalog Database for Clean Data
resource "aws_glue_catalog_database" "clean_data_catalog" {
  name        = var.clean_database_name
  description = "ETL清洗后数据目录 - 存储 after-clean 和后续 transformation 数据"

  tags = merge(var.tags, {
    Name        = var.clean_database_name
    Environment = var.env
    Purpose     = "ETLCleanDataCatalog"
  })
}

locals {
  clean_data_path = "s3://${var.s3_clean_bucket_name}/after-clean"
}

module "departments_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}departments"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/departments/"
  columns = [
    { name = "department_id", type = "int" },
    { name = "department", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}

module "aisles_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}aisles"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/aisles/"
  columns = [
    { name = "aisle_id", type = "int" },
    { name = "aisle", type = "string" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}

module "products_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}products"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/products/"
  columns = [
    { name = "product_id", type = "int" },
    { name = "product_name", type = "string" },
    { name = "aisle_id", type = "int" },
    { name = "department_id", type = "int" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}

module "orders_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}orders"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/orders/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "user_id", type = "int" },
    { name = "eval_set", type = "string" },
    { name = "order_number", type = "int" },
    { name = "order_dow", type = "int" },
    { name = "order_hour_of_day", type = "int" },
    { name = "days_since_prior_order", type = "double" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}

module "order_products_prior_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}order_products_prior"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/order_products_prior/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "product_id", type = "int" },
    { name = "add_to_cart_order", type = "int" },
    { name = "reordered", type = "int" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}

module "order_products_train_clean_table" {
  source        = "./table"
  name          = "${var.after_clean_table_prefix}order_products_train"
  database_name = var.clean_database_name
  location      = "${local.clean_data_path}/order_products_train/"
  columns = [
    { name = "order_id", type = "int" },
    { name = "product_id", type = "int" },
    { name = "add_to_cart_order", type = "int" },
    { name = "reordered", type = "int" }
  ]
  depends_on = [aws_glue_catalog_database.clean_data_catalog]
}
