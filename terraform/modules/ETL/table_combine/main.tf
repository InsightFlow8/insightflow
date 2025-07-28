# ETL Table Combine Glue Job Terraform Module

# CloudWatch Log Group for Glue Job
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/table-combine-logs-v2"
  retention_in_days = 14
  
  tags = {
    Name        = "Glue ETL Table Combine Logs"
    Environment = "dev"
    Component   = "etl-table-combine-logs"
  }
}

resource "aws_glue_job" "table_combine" {
  name              = var.job_name
  role_arn          = aws_iam_role.glue_table_combine.arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/${aws_s3_object.etl_table_combine_script.key}"
    python_version  = "3"
  }
  default_arguments = merge({
    "--TempDir" = var.temp_dir
    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--aisles_path" = var.aisles_path
    "--departments_path" = var.departments_path
    "--products_path" = var.products_path
    "--orders_path" = var.orders_path
    "--order_products_prior_path" = var.order_products_prior_path
    "--order_products_train_path" = var.order_products_train_path
    "--output_path" = var.output_path
  }, var.extra_arguments)
  max_retries       = 1
  glue_version      = var.glue_version
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  tags              = var.tags
}
