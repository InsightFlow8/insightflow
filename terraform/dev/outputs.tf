output "rds_endpoint" {
  value = module.rds_postgresql.rds_endpoint
}

output "rds_host" {
  value = module.rds_postgresql.rds_host
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = module.rds_postgresql.rds_port
}

# =============================
# Glue Crawler Outputs
# =============================
output "glue_database_name" {
  description = "Name of the Glue catalog database for raw data"
  value       = module.glue_crawler_raw.glue_database_name
}

output "glue_database_arn" {
  description = "ARN of the Glue catalog database"
  value       = module.glue_crawler_raw.glue_database_arn
}

output "glue_crawler_role_arn" {
  description = "ARN of the IAM role used by Glue crawlers"
  value       = module.glue_crawler_raw.glue_role_arn
}

output "glue_crawler_names" {
  description = "Names of all created Glue crawlers"
  value       = module.glue_crawler_raw.crawler_names
}

output "glue_table_names" {
  description = "Expected table names in the Glue catalog"
  value       = module.glue_crawler_raw.table_names
}