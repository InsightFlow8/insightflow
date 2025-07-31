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
# output "glue_database_name" {
#   description = "Name of the Glue catalog database for raw data"
#   value       = module.glue_crawler_raw.glue_database_name
# }

# output "glue_database_arn" {
#   description = "ARN of the Glue catalog database"
#   value       = module.glue_crawler_raw.glue_database_arn
# }

# output "glue_crawler_role_arn" {
#   description = "ARN of the IAM role used by Glue crawlers"
#   value       = module.glue_crawler_raw.glue_role_arn
# }

# output "glue_crawler_names" {
#   description = "Names of all created Glue crawlers"
#   value       = module.glue_crawler_raw.crawler_names
# }

# output "glue_table_names" {
#   description = "Expected table names in the Glue catalog"
#   value       = module.glue_crawler_raw.table_names
# }


# =============================
# ETL Data Clean Outputs
# =============================
output "etl_glue_job_name" {
  description = "Name of the ETL Data Clean Glue Job"
  value       = module.etl_data_clean.glue_job_name
}

output "etl_glue_job_arn" {
  description = "ARN of the ETL Data Clean Glue Job"
  value       = module.etl_data_clean.glue_job_arn
}

output "etl_iam_role_arn" {
  description = "ARN of the IAM Role for ETL Data Clean Glue Job"
  value       = module.etl_data_clean.iam_role_arn
}

output "etl_iam_role_name" {
  description = "Name of the IAM Role for ETL Data Clean Glue Job"
  value       = module.etl_data_clean.iam_role_name
}

# =============================
# ETL Table Combine Outputs (Temporarily Disabled)
# =============================
# NOTE: Table Combine outputs are temporarily disabled per team discussion
# To re-enable: uncomment these output blocks
# output "table_combine_glue_job_name" {
#   description = "Name of the ETL Table Combine Glue Job"
#   value       = module.etl_table_combine.glue_job_name
# }
#
# output "table_combine_glue_job_arn" {
#   description = "ARN of the ETL Table Combine Glue Job"
#   value       = module.etl_table_combine.glue_job_arn
# }
#
# output "table_combine_iam_role_arn" {
#   description = "ARN of the IAM Role for ETL Table Combine Glue Job"
#   value       = module.etl_table_combine.iam_role_arn
# }
#
# output "table_combine_iam_role_name" {
#   description = "Name of the IAM Role for ETL Table Combine Glue Job"
#   value       = module.etl_table_combine.iam_role_name
# }
#
# output "table_combine_output_path" {
#   description = "S3 path for the combined table output"
#   value       = var.table_combine_output_path
# }