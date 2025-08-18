# modules/data_ingestion/batch/variables.tf
# 本模块接收的变量，用于配置 Lambda + EventBridge + Snowflake + Crawler 等资源

variable "lambda_zip_path" {
  description = "Path to the Lambda deployment package zip file"
  type        = string
}

variable "lambda_function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "lambda_handler" {
  description = "Handler for the Lambda function"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "lambda_runtime" {
  description = "Runtime for the Lambda function"
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout" {
  description = "Timeout for the Lambda function (seconds)"
  type        = number
  default     = 900
}


variable "lambda_memory_size" {
  description = "Memory size (MB) for the Lambda function"
  type        = number
  default     = 128
}

variable "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  type        = string
}

variable "eventbridge_rule_description" {
  description = "Description for the EventBridge rule"
  type        = string
  default     = "Trigger batch ingestion Lambda on schedule"
}

variable "eventbridge_schedule_expression" {
  description = "Schedule expression (cron) for EventBridge rule"
  type        = string
}

variable "snowflake_secret_name" {
  description = "Snowflake secret name"
  type        = string
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "raw_bucket" {
  description = "S3 bucket for raw data"
  type        = string
}

variable "clean_bucket" {
  description = "S3 bucket for clean data"
  type        = string
}

variable "raw_database_arn" {
  description = "ARN of the Glue catalog database for raw data"
  type        = string
}

variable "raw_database_name" {
  description = "Name of the Glue catalog database for raw data"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "lambda_sync_raw_security_group_id" {
  description = "Security group ID for the application"
  type        = string
}
