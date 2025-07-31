# =============================
# Variables for Glue Table Submodule (ETL Clean Data)
# =============================

variable "name" {
  description = "表名称"
  type        = string
}

variable "database_name" {
  description = "Glue 数据库名称"
  type        = string
}

variable "location" {
  description = "S3 表数据位置路径"
  type        = string
}

variable "columns" {
  description = "表列定义"
  type = list(object({
    name = string
    type = string
  }))
}
