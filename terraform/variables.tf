# variables.tf
# 声明必要的 Terraform 参数变量

variable "aws_region" {
  description = "AWS 区域"
  type        = string
}

variable "lambda_function_name" {
  description = "Lambda 函数名称"
  type        = string
}

variable "lambda_handler" {
  description = "Lambda 入口函数 handler，例如 main.lambda_handler"
  type        = string
}

variable "lambda_runtime" {
  description = "Lambda 使用的运行环境，例如 python3.11"
  type        = string
}

variable "lambda_zip_path" {
  description = "本地打包好的 zip 文件路径"
  type        = string
}

variable "bucket_name" {
  description = "S3 Bucket 名称"
  type        = string
}

variable "eventbridge_schedule_expression" {
  description = "EventBridge 调度 Cron 表达式"
  type        = string
}

variable "lambda_role_name" {
  description = "IAM Role 名称"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake 用户名"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake 密码"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake 账户 ID"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake 执行角色"
  type        = string
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse 名称"
  type        = string
}

variable "raw_crawler_names" {
  default = "crawler_raw_orders,crawler_raw_products,crawler_raw_departments,crawler_raw_aisles"
}
