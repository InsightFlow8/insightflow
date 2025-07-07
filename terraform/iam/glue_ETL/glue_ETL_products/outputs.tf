# terraform/iam/glue_ETL/glue_ETL_products/outputs.tf

output "glue_role_products_arn" {
  description = "ARN of the IAM Role for Glue ETL products job"
  value       = aws_iam_role.glue_role_products.arn
}
