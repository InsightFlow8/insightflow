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

variable "landing_prefix_base" {
  description = "基础数据前缀路径（用于 Glue Crawler）"
  type        = string
  default     = "data/batch"
}

# ===== orders glue job =====
variable "orders_job_name" {
  description = "Glue Job 名称"
  type        = string
}

variable "orders_script_path" {
  description = "Glue Job 脚本在 S3 上的路径"
  type        = string
}

variable "orders_output_path" {
  description = "ETL 输出路径（clean zone）"
  type        = string
}

variable "orders_input_table" {
  description = "输入数据对应的 Glue Catalog 表名"
  type        = string
}

variable "orders_temp_dir" {
  description = "Glue 临时目录路径"
  type        = string
}

variable "orders_eval_set_filter" {
  description = "筛选 eval_set 类型（如 prior, train）"
  type        = string
}

# ===== departments glue job =====
variable "departments_job_name" {
  description = "Glue Job 名称 - departments"
  type        = string
}

variable "departments_script_path" {
  description = "Glue 脚本路径 - departments"
  type        = string
}

variable "departments_output_path" {
  description = "清洗后输出路径 - departments"
  type        = string
}

variable "departments_historical_path" {
  description = "清洗后历史路径 - departments"
  type        = string
}

variable "departments_input_table" {
  description = "Glue Catalog 表名 - departments"
  type        = string
}

variable "departments_temp_dir" {
  description = "Glue 临时目录路径 - departments"
  type        = string
}

# ===== aisles glue job =====
variable "aisles_job_name" {
  description = "Glue Job 名称 - aisles"
  type        = string
}

variable "aisles_script_path" {
  description = "Glue 脚本路径 - aisles"
  type        = string
}

variable "aisles_output_path" {
  description = "清洗后输出路径 - aisles"
  type        = string
}

variable "aisles_historical_path" {
  description = "清洗后历史路径 - aisles"
  type        = string
}


variable "aisles_input_table" {
  description = "Glue Catalog 表名 - aisles"
  type        = string
}

variable "aisles_temp_dir" {
  description = "Glue 临时目录路径 - aisles"
  type        = string
}

# ===== products glue job =====
variable "products_job_name" {
  description = "Glue Job 名称 - products"
  type        = string
}

variable "products_script_path" {
  description = "Glue 脚本路径 - products"
  type        = string
}

variable "products_output_path" {
  description = "清洗后输出路径 - products"
  type        = string
}

variable "products_input_table" {
  description = "Glue Catalog 表名 - products"
  type        = string
}

variable "products_temp_dir" {
  description = "Glue 临时目录路径 - products"
  type        = string
}