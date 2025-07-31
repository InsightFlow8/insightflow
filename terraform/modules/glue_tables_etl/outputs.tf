# =============================
# Outputs for Glue Tables ETL Module
# =============================

output "clean_database_name" {
  description = "ETL 清洗后数据的 Glue 数据库名称"
  value       = aws_glue_catalog_database.clean_data_catalog.name
}

output "clean_database_arn" {
  description = "ETL 清洗后数据的 Glue 数据库 ARN"
  value       = aws_glue_catalog_database.clean_data_catalog.arn
}

output "departments_clean_table_name" {
  description = "Departments clean table name"
  value       = module.departments_clean_table.table_name
}

output "aisles_clean_table_name" {
  description = "Aisles clean table name"
  value       = module.aisles_clean_table.table_name
}

output "products_clean_table_name" {
  description = "Products clean table name"
  value       = module.products_clean_table.table_name
}

output "orders_clean_table_name" {
  description = "Orders clean table name"
  value       = module.orders_clean_table.table_name
}

output "order_products_prior_clean_table_name" {
  description = "Order products prior clean table name"
  value       = module.order_products_prior_clean_table.table_name
}

output "order_products_train_clean_table_name" {
  description = "Order products train clean table name"
  value       = module.order_products_train_clean_table.table_name
}
