output "glue_job_name" {
  description = "Name of the Glue Job"
  value       = aws_glue_job.table_combine.name
}

output "glue_job_arn" {
  description = "ARN of the Glue Job"
  value       = aws_glue_job.table_combine.arn
}

output "iam_role_arn" {
  description = "ARN of the IAM Role"
  value       = aws_iam_role.glue_table_combine.arn
}

output "iam_role_name" {
  description = "Name of the IAM Role"
  value       = aws_iam_role.glue_table_combine.name
}

output "script_s3_key" {
  description = "S3 key of the uploaded script"
  value       = aws_s3_object.etl_table_combine_script.key
}

output "script_s3_path" {
  description = "Full S3 path of the uploaded script"
  value       = "s3://${var.scripts_bucket}/${aws_s3_object.etl_table_combine_script.key}"
}
