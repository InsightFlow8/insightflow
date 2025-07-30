# main.tf
# 顶层 Terraform 入口，包含 provider 配置与 module 加载.

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

module "s3_buckets" {
  source         = "../modules/s3_buckets"
  bucket_names   = [var.raw_bucket, var.clean_bucket, var.curated_bucket, var.scripts_bucket]
  raw_bucket     = var.raw_bucket
  clean_bucket   = var.clean_bucket
  curated_bucket = var.curated_bucket
  scripts_bucket = var.scripts_bucket
}

module "vpc" {
  source               = "../modules/vpc"
  env                  = "insightflow_dev"
  vpc_cidr_block       = "10.0.0.0/16"
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs = ["10.0.3.0/24", "10.0.4.0/24"]
  region               = var.aws_region
}

# Glue Tables
module "glue_tables" {
  source = "../modules/glue_tables"

  env                = "insightflow_dev"
  s3_raw_bucket_name = var.raw_bucket
  s3_raw_data_prefix = var.raw_prefix
  raw_database_name  = "insightflow_imba_raw_data_catalog"
  depends_on         = [module.s3_buckets]
}

module "batch_ingestion" {
  source = "../modules/data_ingestion/batch"

  lambda_zip_path      = "../assets/batch_ingestion_lambda.zip"
  lambda_function_name = "batch_ingestion"
  lambda_handler       = "lambda_function.lambda_handler"
  lambda_runtime       = "python3.13"
  lambda_timeout       = 900
  lambda_memory_size   = 1024

  eventbridge_rule_name        = "batch_ingestion_trigger"
  eventbridge_rule_description = "Trigger batch ingestion Lambda on 30th of every month at 00:00 Sydney time"
  # 悉尼时间0点，冬令时UTC+10=14点，夏令时UTC+11=13点。此表达式为每月30号悉尼时间0点（UTC 14点），如需兼容31号可调整为29-31
  eventbridge_schedule_expression = "cron(0 14 30 * ? *)"
  # 根据最后的项目需求调整


  snowflake_secret_name = var.snowflake_secret_name

  aws_region        = var.aws_region
  raw_bucket        = var.raw_bucket
  clean_bucket      = var.clean_bucket
  raw_database_arn  = module.glue_tables.raw_database_arn
  raw_database_name = module.glue_tables.raw_database_name

  # 确保 S3 bucket 相关资源先于数据采集模块创建
  depends_on = [module.s3_buckets, module.glue_tables]
}

module "streaming_ingestion" {
  source      = "../modules/data_ingestion/streaming"
  stream_name = "insightflow-dummy-streaming"

  publisher_function_name = "streaming_data_publisher"
  publisher_zip_path      = "../assets/streaming_data_publisher.zip"
  publisher_handler       = "streaming_data_publisher.lambda_handler"
  publisher_runtime       = "python3.13"
  publisher_memory_size   = 128
  publisher_timeout       = 60

  firehose_name             = "insightflow-dummy-firehose"
  raw_bucket                = "insightflow-raw-bucket"
  transformer_function_name = "streaming_data_transformer"
  transformer_zip_path      = "../assets/streaming_data_transformer.zip"
  transformer_handler       = "streaming_data_transformer.lambda_handler"
  transformer_runtime       = "python3.13"
  transformer_memory_size   = 128
  transformer_timeout       = 60

  # 确保 S3 bucket 相关资源先于数据采集模块创建
  depends_on = [module.s3_buckets]
}

module "ec2" {
  source = "../modules/ec2"
  env    = "insightflow_dev"

  ami_id                     = var.ami_id
  instance_type              = var.instance_type
  key_name                   = var.key_name
  public_subnet_id           = module.vpc.public_subnet_ids[0]
  bastion_security_group_ids = [module.vpc.bastion_security_group_id]
  region                     = var.aws_region

  rds_endpoint = module.rds_postgresql.rds_endpoint
  rds_host     = module.rds_postgresql.rds_host
  rds_port     = module.rds_postgresql.rds_port
  db_name      = var.db_name
  db_username  = var.db_username
  db_password  = var.db_password
  sql_s3_path  = "s3://insightflow-imba-scripts-upload/rds-postgresql/create_tables.sql"

  depends_on = [module.vpc, module.rds_postgresql]
}

module "rds_postgresql" {
  source = "../modules/rds_postgresql"
  env    = "insightflow-dev"

  db_name                     = var.db_name
  db_username                 = var.db_username
  db_password                 = var.db_password
  private_subnet_ids          = module.vpc.private_subnet_ids
  postgres_security_group_ids = [module.vpc.postgres_security_group_id]
  depends_on                  = [module.vpc]
}

# =============================
# Glue Crawler for Raw Data
# =============================
# module "glue_crawler_raw" {
#   source = "../modules/glue_crawler_raw"

#   env                = "insightflow_dev"
#   s3_bucket_name     = var.raw_bucket
#   s3_raw_data_prefix = var.raw_prefix
#   database_name      = "insightflow_imba_raw_data_catalog"
#   table_prefix       = "raw_"
#   recrawl_behavior   = var.recrawl_behavior

#   # 使用变量设置爬虫调度，而不是硬编码
#   crawler_schedule = var.crawler_schedule

#   tags = {
#     Project     = "InsightFlow"
#     Environment = "insightflow_dev"
#     Owner       = "IMBADataTeam"
#     Purpose     = "RawDataCrawling"
#   }

#   depends_on = [module.s3_buckets]
# }



module "data_sync_raw" {
  source = "../modules/data_sync/raw"

  lambda_zip_path = "../assets/lambda_s3_to_rds_raw.zip"
  s3_bucket_name  = var.raw_bucket
  s3_bucket_arn   = module.s3_buckets.raw_bucket_arn

  rds_host     = module.rds_postgresql.rds_host
  rds_port     = module.rds_postgresql.rds_port
  rds_db       = var.db_name
  rds_user     = var.db_username
  rds_password = var.db_password

  table_name  = var.table_name
  schema_name = "insightflow_raw"

  # NOTE: batch_size cannot be too large otherwise it will exceed the limit of RDS connection.
  batch_size = "10000"

  s3_key_prefix = var.s3_key_prefix
  start_ts      = var.start_ts
  end_ts        = var.end_ts


  eventbridge_schedule = var.eventbridge_schedule

  private_subnet_ids       = module.vpc.private_subnet_ids
  lambda_security_group_id = module.vpc.lambda_sync_raw_security_group_id

  depends_on = [module.vpc, module.rds_postgresql, module.s3_buckets]
}

# =============================
# DMS Module (Currently Disabled)
# =============================
# NOTE: DMS module is temporarily disabled while being redesigned 
# for the new architecture: Glue Crawler → Glue Table → DMS → RDS
# The new DMS will use Glue Data Catalog as source instead of direct S3

# module "dms" {
#   source = "../modules/dms"
#   
#   # Will be updated with new parameters for Glue Data Catalog integration
# }


# =============================
# ETL Data Clean Module
# =============================
module "etl_data_clean" {
  source = "../modules/ETL/data_clean"

  # Job Configuration
  job_name        = var.etl_job_name
  iam_role_name   = var.etl_iam_role_name
  script_location = var.etl_script_location
  temp_dir        = var.etl_temp_dir
  scripts_bucket  = var.scripts_bucket

  # Input S3 Paths
  aisles_path               = var.aisles_input_path
  departments_path          = var.departments_input_path
  products_path             = var.products_input_path
  orders_path               = var.orders_input_path
  order_products_prior_path = var.order_products_prior_input_path
  order_products_train_path = var.order_products_train_input_path

  # Output S3 Paths
  aisles_out               = var.aisles_output_path
  departments_out          = var.departments_output_path
  products_out             = var.products_output_path
  orders_out               = var.orders_output_path
  order_products_prior_out = var.order_products_prior_output_path
  order_products_train_out = var.order_products_train_output_path

  # Glue Configuration
  glue_version      = var.etl_glue_version
  number_of_workers = var.etl_number_of_workers
  worker_type       = var.etl_worker_type

  tags = {
    Project     = "InsightFlow"
    Environment = "dev"
    Component   = "etl-data-clean"
  }

  depends_on = [module.s3_buckets]
}

# =============================
# ETL Table Combine Module
# =============================
module "etl_table_combine" {
  source = "../modules/ETL/table_combine"

  # Job Configuration
  job_name       = var.table_combine_job_name
  iam_role_name  = var.table_combine_iam_role_name
  temp_dir       = var.etl_temp_dir
  scripts_bucket = var.scripts_bucket

  # Input S3 Paths (from data_clean stage)
  aisles_path               = var.aisles_output_path
  departments_path          = var.departments_output_path
  products_path             = var.products_output_path
  orders_path               = var.orders_output_path
  order_products_prior_path = var.order_products_prior_output_path
  order_products_train_path = var.order_products_train_output_path

  # Output S3 Path
  output_path = var.table_combine_output_path

  # Glue Configuration
  glue_version      = var.etl_glue_version
  number_of_workers = var.etl_number_of_workers
  worker_type       = var.etl_worker_type

  tags = {
    Project     = "InsightFlow"
    Environment = "dev"
    Component   = "etl-table-combine"
  }

  depends_on = [module.s3_buckets, module.etl_data_clean]
}