# =============================
# Variables for Glue Tables ETL Module
# =============================

variable "clean_database_name" {
  description = "ETL 清洗后数据的 Glue Catalog 数据库名称"
  type        = string
  default     = "insightflow_imba_clean_data_catalog"
}

variable "s3_clean_bucket_name" {
  description = "存储清洗后数据的 S3 bucket 名称"
  type        = string
}

variable "after_clean_table_prefix" {
  description = "After-clean 表的前缀"
  type        = string
  default     = "after_clean_"
}

variable "env" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "tags" {
  description = "Common tags to be applied to resources"
  type        = map(string)
  default     = {}
}
