variable "snowflake_user" {
  description = "Snowflake user for Lambda environment"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password for Lambda environment"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

# S3
variable "raw_bucket" {
  description = "S3 bucket for raw data"
  type        = string
}

variable "clean_bucket" {
  description = "S3 bucket for clean data"
  type        = string
}

variable "curated_bucket" {
  description = "S3 bucket for curated data"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket for scripts and assets"
  type        = string
  default     = "insightflow-dev-scripts"
}

# EC2
variable "ami_id" {}
variable "instance_type" {}
variable "key_name" {}

# rds_postgresql
variable "db_name" {
  description = "Database name for RDS PostgreSQL"
  type        = string
}

variable "db_username" {
  description = "Master username for RDS PostgreSQL"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Master password for RDS PostgreSQL"
  type        = string
  sensitive   = true
}

# S3, glue_crawler_raw, Data Processing
variable "raw_prefix" {
  description = "S3 prefix/folder for raw data (e.g., 'data/batch/')"
  type        = string
}

variable "aws_az" {
  description = "Availability zone for DMS instance"
  type        = string
}

# =============================
# Glue Crawler Variables (Temporarily Disabled)
# =============================
# NOTE: These variables are temporarily disabled as glue_crawler_raw module is commented out
# To re-enable: uncomment these variable definitions

# variable "recrawl_behavior" {
#   description = "爬取行为: CRAWL_EVERYTHING(全量) 或 CRAWL_NEW_FOLDERS_ONLY(增量)"
#   type        = string
#   validation {
#     condition = contains([
#       "CRAWL_EVERYTHING",
#       "CRAWL_NEW_FOLDERS_ONLY"
#     ], var.recrawl_behavior)
#     error_message = "Recrawl behavior must be either CRAWL_EVERYTHING or CRAWL_NEW_FOLDERS_ONLY."
#   }
# }

# variable "crawler_schedule" {
#   description = "Schedule for running crawlers (cron expression)"
#   type        = string
#   default     = "cron(0 15 30 * ? *)"
# }

# variable "lambda_zip_path" {
#   description = "Path to Lambda deployment package zip file"
#   type        = string
# }

# variable "s3_bucket_arn" {
#   description = "ARN of the S3 bucket"
#   type        = string
# }

# variable "rds_host" {
#   description = "RDS PostgreSQL host"
#   type        = string
# }

# variable "rds_port" {
#   description = "RDS PostgreSQL port"
#   type        = string
#   default     = "5432"
# }

variable "eventbridge_schedule" {
  description = "EventBridge schedule expression for Lambda trigger"
  type        = string
  default     = "cron(0 16 30 * ? *)"
}

variable "table_name" {
  description = "List of database table names to sync data to"
  type        = list(string)
  default     = ["orders"]
}

variable "s3_key_prefix" {
  description = "List of S3 key prefixes to filter objects for processing"
  type        = list(string)
  default     = ["data/batch/orders/"]
}

variable "start_ts" {
  description = "Start timestamp for data sync (optional)"
  type        = string
  default     = "1900-01-01-0000"
}

variable "end_ts" {
  description = "End timestamp for data sync (optional)"
  type        = string
  default     = "2099-12-31-2359"
}

# variable "schema_name" {
#   description = "Target schema name in RDS"
#   type        = string
#   default     = "insightflow_raw"
# }

# variable "batch_size" {
#   description = "Batch size for RDS insert"
#   type        = string
#   default     = "100000"
# }

# =============================
# ETL Data Clean Variables
# =============================
variable "etl_job_name" {
  description = "Name of the ETL Data Clean Glue Job"
  type        = string
  default     = "insightflow-data-clean-job"
}

variable "etl_iam_role_name" {
  description = "Name of the IAM Role for ETL Data Clean Glue Job"
  type        = string
  default     = "insightflow-glue-data-clean-role"
}

variable "etl_script_location" {
  description = "S3 path to the ETL PySpark script"
  type        = string
  default     = "s3://insightflow-dev-scripts/ETL/data_clean/ETL_data_clean.py"
}

variable "etl_temp_dir" {
  description = "S3 temp directory for Glue Job"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/glue-temp/"
}

# Input S3 Paths
variable "aisles_input_path" {
  description = "S3 input path for aisles table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/aisles/"
}

variable "departments_input_path" {
  description = "S3 input path for departments table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/departments/"
}

variable "products_input_path" {
  description = "S3 input path for products table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/products/"
}

variable "orders_input_path" {
  description = "S3 input path for orders table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/orders/"
}

variable "order_products_prior_input_path" {
  description = "S3 input path for order_products_prior table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/order_products_prior/"
}

variable "order_products_train_input_path" {
  description = "S3 input path for order_products_train table"
  type        = string
  default     = "s3://insightflow-dev-raw-bucket/data/batch/order_products_train/"
}

# Output S3 Paths
variable "aisles_output_path" {
  description = "S3 output path for cleaned aisles table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/aisles/"
}

variable "departments_output_path" {
  description = "S3 output path for cleaned departments table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/departments/"
}

variable "products_output_path" {
  description = "S3 output path for cleaned products table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/products/"
}

variable "orders_output_path" {
  description = "S3 output path for cleaned orders table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/orders/"
}

variable "order_products_prior_output_path" {
  description = "S3 output path for cleaned order_products_prior table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/order_products_prior/"
}

variable "order_products_train_output_path" {
  description = "S3 output path for cleaned order_products_train table"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/after-clean/order_products_train/"
}

# Glue Configuration
variable "etl_glue_version" {
  description = "Glue version for ETL job"
  type        = string
  default     = "4.0"
}

variable "etl_number_of_workers" {
  description = "Number of workers for ETL Glue Job"
  type        = number
  default     = 5
}

variable "etl_worker_type" {
  description = "Worker type for ETL Glue Job"
  type        = string
  default     = "G.1X"
}

# =============================
# ETL Table Combine Variables (Temporarily Disabled)
# =============================
# NOTE: Table Combine variables are temporarily disabled per team discussion
# To re-enable: uncomment these variable definitions

# variable "table_combine_job_name" {
#   description = "Name for the table combine Glue Job"
#   type        = string
#   default     = "insightflow-table-combine-job"
# }

# variable "table_combine_iam_role_name" {
#   description = "IAM role name for table combine Glue Job"
#   type        = string
#   default     = "insightflow-glue-table-combine-role"
# }

# variable "table_combine_output_path" {
#   description = "S3 path for combined table output"
#   type        = string
#   default     = "s3://insightflow-dev-clean-bucket/combined/"
# }

variable "snowflake_secret_name" {
  description = "Snowflake secret name"
  type        = string
}

# =============================
# ETL Data Transformation Variables
# =============================
variable "data_transformation_job_name" {
  description = "Name for the data transformation Glue Job"
  type        = string
  default     = "insightflow-data-transformation-job"
}

variable "data_transformation_iam_role_name" {
  description = "IAM role name for data transformation Glue Job"
  type        = string
  default     = "insightflow-glue-data-transformation-role"
}

# =============================
# Glue Crawler Transformation Variables
# =============================
variable "transformation_crawler_schedule" {
  description = "Schedule for running transformation feature crawlers (cron expression)"
  type        = string
  default     = "cron(0 17 30 * ? *)" # UTC时间每月30日下午5点运行，在 transformation job 完成后
}
