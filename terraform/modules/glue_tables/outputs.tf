output "raw_database_arn" {
  value = aws_glue_catalog_database.raw_data_catalog.arn
}

output "raw_database_name" {
  value = aws_glue_catalog_database.raw_data_catalog.name
}
