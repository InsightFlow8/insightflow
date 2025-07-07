# module/batch_ingestion/variables.tf
# 本模块接收的变量，用于配置 Lambda + EventBridge + Snowflake + Crawler

variable "aws_region" {
  description = "AWS 区域"
  type        = string
}

variable "lambda_function_name" {
  description = "Lambda 函数名称"
  type        = string
}

variable "lambda_handler" {
  description = "Lambda 函数入口 handler"
  type        = string
}

variable "lambda_runtime" {
  description = "Lambda 运行环境"
  type        = string
}

variable "lambda_zip_path" {
  description = "Lambda zip 文件路径"
  type        = string
}

variable "bucket_name" {
  description = "Landing 区域的 S3 Bucket"
  type        = string
}

variable "eventbridge_schedule_expression" {
  description = "Lambda 定时触发表达式"
  type        = string
}

variable "lambda_role_name" {
  description = "Lambda 执行角色名称"
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
  description = "要批量触发的 Glue Crawler 名称集合"
  type        = string
}

variable "landing_prefix_base" {
  description = "Landing Bucket 中的数据路径前缀"
  type        = string
}
