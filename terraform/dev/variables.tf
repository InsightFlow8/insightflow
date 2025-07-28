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

variable "recrawl_behavior" {
  description = "爬取行为: CRAWL_EVERYTHING(全量) 或 CRAWL_NEW_FOLDERS_ONLY(增量)"
  type        = string
  validation {
    condition = contains([
      "CRAWL_EVERYTHING",
      "CRAWL_NEW_FOLDERS_ONLY"
    ], var.recrawl_behavior)
    error_message = "Recrawl behavior must be either CRAWL_EVERYTHING or CRAWL_NEW_FOLDERS_ONLY."
  }
}

variable "crawler_schedule" {
  description = "Schedule for running crawlers (cron expression)"
  type        = string
  default     = "cron(0 15 30 * ? *)"
}

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
}

variable "etl_temp_dir" {
  description = "S3 temp directory for Glue Job"
  type        = string
}

# Input S3 Paths
variable "aisles_input_path" {
  description = "S3 input path for aisles table"
  type        = string
}

variable "departments_input_path" {
  description = "S3 input path for departments table"
  type        = string
}

variable "products_input_path" {
  description = "S3 input path for products table"
  type        = string
}

variable "orders_input_path" {
  description = "S3 input path for orders table"
  type        = string
}

variable "order_products_prior_input_path" {
  description = "S3 input path for order_products_prior table"
  type        = string
}

variable "order_products_train_input_path" {
  description = "S3 input path for order_products_train table"
  type        = string
}

# Output S3 Paths
variable "aisles_output_path" {
  description = "S3 output path for cleaned aisles table"
  type        = string
}

variable "departments_output_path" {
  description = "S3 output path for cleaned departments table"
  type        = string
}

variable "products_output_path" {
  description = "S3 output path for cleaned products table"
  type        = string
}

variable "orders_output_path" {
  description = "S3 output path for cleaned orders table"
  type        = string
}

variable "order_products_prior_output_path" {
  description = "S3 output path for cleaned order_products_prior table"
  type        = string
}

variable "order_products_train_output_path" {
  description = "S3 output path for cleaned order_products_train table"
  type        = string
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
# ETL Table Combine Variables
# =============================
variable "table_combine_job_name" {
  description = "Name for the table combine Glue Job"
  type        = string
  default     = "insightflow-table-combine-job"
}

variable "table_combine_iam_role_name" {
  description = "IAM role name for table combine Glue Job"
  type        = string
  default     = "insightflow-glue-table-combine-role"
}

variable "table_combine_output_path" {
  description = "S3 path for combined table output"
  type        = string
  default     = "s3://insightflow-dev-clean-bucket/combined/"
}
