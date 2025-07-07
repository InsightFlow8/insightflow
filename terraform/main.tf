# main.tf
# 顶层 Terraform 入口，包含 provider 配置与 module 加载

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# ========================
# 模块 1：Batch Ingestion 阶段
# ========================
module "batch_ingestion" {
  source = "./module/batch_ingestion"

  # 传入变量
  lambda_function_name            = var.lambda_function_name
  lambda_handler                  = var.lambda_handler
  lambda_runtime                  = var.lambda_runtime
  lambda_zip_path                 = var.lambda_zip_path
  bucket_name                     = var.bucket_name
  eventbridge_schedule_expression = var.eventbridge_schedule_expression
  lambda_role_name                = var.lambda_role_name

  snowflake_user      = var.snowflake_user
  snowflake_password  = var.snowflake_password
  snowflake_account   = var.snowflake_account
  snowflake_role      = var.snowflake_role
  snowflake_warehouse = var.snowflake_warehouse

  raw_crawler_names   = var.raw_crawler_names
  landing_prefix_base = var.landing_prefix_base
  aws_region          = var.aws_region
}

# ========================
# 模块 2：Glue ETL 阶段
# ========================
module "glue_etl" {
  source = "./module/glue_ETL"

  orders_job_name        = var.orders_job_name
  orders_script_path     = var.orders_script_path
  orders_output_path     = var.orders_output_path
  orders_input_table     = var.orders_input_table
  orders_temp_dir        = var.orders_temp_dir
  orders_eval_set_filter = var.orders_eval_set_filter
  orders_role_arn        = module.iam_glue_etl_orders.glue_role_orders_arn

  departments_job_name        = var.departments_job_name
  departments_script_path     = var.departments_script_path
  departments_output_path     = var.departments_output_path
  departments_input_table     = var.departments_input_table
  departments_temp_dir        = var.departments_temp_dir
  departments_historical_path = var.departments_historical_path
  departments_role_arn        = module.iam_glue_etl_departments.glue_role_departments_arn

  aisles_job_name        = var.aisles_job_name
  aisles_script_path     = var.aisles_script_path
  aisles_output_path     = var.aisles_output_path
  aisles_input_table     = var.aisles_input_table
  aisles_temp_dir        = var.aisles_temp_dir
  aisles_historical_path = var.aisles_historical_path
  aisles_role_arn        = module.iam_glue_etl_aisles.glue_role_aisles_arn

  products_job_name    = var.products_job_name
  products_script_path = var.products_script_path
  products_output_path = var.products_output_path
  products_input_table = var.products_input_table
  products_temp_dir    = var.products_temp_dir
  products_role_arn    = module.iam_glue_etl_products.glue_role_products_arn


  bucket_name         = var.bucket_name
  landing_prefix_base = var.landing_prefix_base
  raw_crawler_names   = var.raw_crawler_names
  aws_region          = var.aws_region
}

# ========================
# 模块 3：IAM glue_ETL
# ========================
module "iam_glue_etl_orders" {
  source    = "./iam/glue_ETL/glue_ETL_orders"
  role_name = "glue-role-orders"
  bucket_names = [
    "imba-test-aaron-landing",
    "imba-test-glue-etl-aaron"
  ]
}

module "iam_glue_etl_departments" {
  source    = "./iam/glue_ETL/glue_ETL_departments"
  role_name = "glue-role-departments"
  bucket_names = [
    "imba-test-aaron-landing",
    "imba-test-glue-etl-aaron"
  ]
}

module "iam_glue_etl_aisles" {
  source    = "./iam/glue_ETL/glue_ETL_aisles"
  role_name = "glue-role-aisles"
  bucket_names = [
    "imba-test-aaron-landing",
    "imba-test-glue-etl-aaron"
  ]
}

module "iam_glue_etl_products" {
  source    = "./iam/glue_ETL/glue_ETL_products"
  role_name = "glue-role-products"
  bucket_names = [
    "imba-test-aaron-landing",
    "imba-test-glue-etl-aaron"
  ]
}