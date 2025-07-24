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

# EC2
variable "ami_id" {}
variable "instance_type" {}
variable "key_name" {}

# rds_postgresql
variable "db_name" {
  description = "Database name for RDS PostgreSQL"
  type        = string
}

variable "rds_db_username" {
  description = "Master username for RDS PostgreSQL"
  type        = string
  sensitive   = true
}

variable "rds_db_password" {
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

variable "github_repository" {
  description = "The GitHub repository name"
  type        = string
}

variable "bucket_name" {
  description = "The name of the S3 bucket"
  type        = string
}
