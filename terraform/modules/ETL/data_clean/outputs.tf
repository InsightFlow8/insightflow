output "data_clean_glue_job_name" {
  description = "The name of the Glue Job for data cleaning."
  value       = aws_glue_job.data_clean.name
}

output "data_clean_glue_job_arn" {
  description = "The ARN of the Glue Job for data cleaning."
  value       = aws_glue_job.data_clean.arn
}

output "iam_role_arn" {
  description = "The ARN of the IAM Role for Glue Job."
  value       = aws_iam_role.glue_data_clean.arn
}

output "iam_role_name" {
  description = "The name of the IAM Role for Glue Job."
  value       = aws_iam_role.glue_data_clean.name
}
