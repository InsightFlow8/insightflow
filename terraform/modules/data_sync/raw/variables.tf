variable "private_subnet_ids" {
  description = "List of private subnet IDs for Lambda VPC config"
  type        = list(string)
}

variable "lambda_security_group_id" {
  description = "Security group ID for Lambda function"
  type        = string
}
variable "lambda_zip_path" {
  description = "Path to Lambda deployment package zip file"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  type        = string
}

variable "rds_host" {
  description = "RDS PostgreSQL host"
  type        = string
}

variable "rds_port" {
  description = "RDS PostgreSQL port"
  type        = string
  default     = "5432"
}

variable "rds_db" {
  description = "RDS PostgreSQL database name"
  type        = string
}

variable "rds_user" {
  description = "RDS PostgreSQL username"
  type        = string
}

variable "rds_password" {
  description = "RDS PostgreSQL password"
  type        = string
}

variable "table_name" {
  description = "Target table name(s) in RDS, e.g. [\"orders\",\"aisles\"] or [\"orders\"]"
  type        = list(string)
}

variable "schema_name" {
  description = "Target schema name in RDS"
  type        = string
  default     = "insightflow_raw"
}

variable "batch_size" {
  description = "Batch size for RDS insert"
  type        = string
  default     = "1000000"
}

variable "eventbridge_schedule" {
  description = "EventBridge schedule expression for Lambda trigger"
  type        = string
  default     = "cron(0 16 30 * ? *)"    # 16:00 UTC on the 30th of every month - can be adjusted as needed
}

variable "s3_key_prefix" {
  description = "S3 key prefix for automatic data sync (e.g., ['data/batch/orders', 'data/batch/aisles'])"
  type        = list(string)
}

variable "start_ts" {
  description = "Start timestamp for manual data sync (format: YYYY-MM-DD HH:MM:SS)"
  type        = string
  default     = "1900-01-01-0000"  # Default to a very early date to include all data
}

variable "end_ts" {
  description = "End timestamp for manual data sync (format: YYYY-MM-DD HH:MM:SS)"
  type        = string
  default     = "9999-12-31-2359"  # Default to a very late date to include all data
}
