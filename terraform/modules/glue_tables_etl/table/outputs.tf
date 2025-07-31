# =============================
# Outputs for Glue Table Submodule (ETL Clean Data)
# =============================

output "table_name" {
  description = "Glue 表名称"
  value       = aws_glue_catalog_table.table.name
}

output "table_arn" {
  description = "Glue 表 ARN"
  value       = aws_glue_catalog_table.table.arn
}

output "table_location" {
  description = "表数据的 S3 位置"
  value       = aws_glue_catalog_table.table.storage_descriptor[0].location
}
