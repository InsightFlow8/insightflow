output "job_name" {
  description = "Name of the ETL Data Transformation Glue Job"
  value       = aws_glue_job.data_transformation.name
}

output "job_arn" {
  description = "ARN of the ETL Data Transformation Glue Job"
  value       = aws_glue_job.data_transformation.arn
}

output "iam_role_arn" {
  description = "ARN of the IAM role for the Glue Job"
  value       = aws_iam_role.glue_data_transformation.arn
}

output "script_s3_key" {
  description = "S3 key of the uploaded ETL script"
  value       = aws_s3_object.etl_data_transformation_script.key
}

output "log_group_name" {
  description = "CloudWatch log group name for the Glue Job"
  value       = aws_cloudwatch_log_group.glue_job_logs.name
}
