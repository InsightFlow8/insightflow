# terraform/iam/glue_ETL_departments/outputs.tf

output "glue_role_departments_arn" {
  description = "ARN of the IAM Role for Glue ETL departments job"
  value       = aws_iam_role.glue_role_departments.arn
}
