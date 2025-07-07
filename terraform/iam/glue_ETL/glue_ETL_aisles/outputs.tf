# terraform/iam/glue_ETL_aisles/outputs.tf

output "glue_role_aisles_arn" {
  description = "ARN of the IAM Role for Glue ETL aisles job"
  value       = aws_iam_role.glue_role_aisles.arn
}
