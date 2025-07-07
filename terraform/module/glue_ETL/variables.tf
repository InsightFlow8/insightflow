# module/glue_ETL/variables.tf
# 参数化 orders Glue Job（生产级：无 default，全由 main.tf 显式传入）
# ========== Orders Job ==========
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

variable "orders_role_arn" {
  description = "Glue Job 的 IAM Role ARN"
  type        = string
}

# ========== Departments Job ==========
variable "departments_job_name" {
  description = "Glue Job 名称（departments）"
  type        = string
}

variable "departments_script_path" {
  description = "Glue Job 脚本在 S3 上的路径（departments）"
  type        = string
}

variable "departments_output_path" {
  description = "ETL 输出路径（departments clean zone）"
  type        = string
}

variable "departments_input_table" {
  description = "输入数据对应的 Glue Catalog 表名（departments）"
  type        = string
}

variable "departments_role_arn" {
  description = "IAM Role ARN for departments Glue Job"
  type        = string
}

variable "departments_temp_dir" {
  description = "Glue 临时目录路径"
  type        = string
}

variable "departments_historical_path" {
  description = "ETL 历史路径（departments clean zone）"
  type        = string
}

# ========== Aisles Job ==========
variable "aisles_job_name" {
  description = "Glue Job 名称（aisles）"
  type        = string
}

variable "aisles_script_path" {
  description = "Glue Job 脚本在 S3 上的路径（aisles）"
  type        = string
}

variable "aisles_output_path" {
  description = "ETL 输出路径（aisles clean zone）"
  type        = string
}

variable "aisles_input_table" {
  description = "输入数据对应的 Glue Catalog 表名（aisles）"
  type        = string
}

variable "aisles_role_arn" {
  description = "IAM Role ARN for aisles Glue Job"
  type        = string
}

variable "aisles_temp_dir" {
  description = "Glue 临时目录路径"
  type        = string
}

variable "aisles_historical_path" {
  description = "ETL 历史路径（aisles clean zone）"
  type        = string
}

# ========== Products Job ==========
variable "products_job_name" {
  description = "Glue Job 名称（products）"
  type        = string
}

variable "products_script_path" {
  description = "Glue Job 脚本在 S3 上的路径（products）"
  type        = string
}

variable "products_output_path" {
  description = "ETL 输出路径（products clean zone）"
  type        = string
}

variable "products_input_table" {
  description = "输入数据对应的 Glue Catalog 表名（products）"
  type        = string
}

variable "products_role_arn" {
  description = "IAM Role ARN for products Glue Job"
  type        = string
}

variable "products_temp_dir" {
  description = "Glue 临时目录路径"
  type        = string
}

# ========== 公共共享变量 ==========
variable "bucket_name" {
  description = "S3 Bucket 名称"
  type        = string
}

variable "landing_prefix_base" {
  description = "Raw 层路径前缀（用于 Glue Crawler）"
  type        = string
}

variable "raw_crawler_names" {
  description = "多个 crawler 名称（逗号分隔）"
  type        = string
}

variable "aws_region" {
  description = "AWS 区域"
  type        = string
}

