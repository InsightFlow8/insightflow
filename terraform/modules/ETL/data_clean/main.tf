# ETL Data Clean Glue Job Terraform Module

# CloudWatch Log Group for Glue Job
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/logs-v2"
  retention_in_days = 14

  tags = {
    Name        = "Glue ETL Data Clean Logs"
    Environment = "dev"
    Component   = "etl-logs"
  }
}

data "aws_subnet" "selected_subnet" {
  id = var.private_subnet_ids[0]
}

resource "aws_glue_connection" "vpc" {
  name                  = "${var.job_name}-vpc-connection"
  connection_type       = "NETWORK"
  connection_properties = {}
  physical_connection_requirements {
    subnet_id              = var.private_subnet_ids[0]
    availability_zone      = data.aws_subnet.selected_subnet.availability_zone
    security_group_id_list = [var.glue_security_group_id]
  }
}

resource "aws_glue_job" "data_clean" {
  name     = var.job_name
  role_arn = aws_iam_role.glue_data_clean.arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/${aws_s3_object.etl_data_clean_script.key}"
    python_version  = "3"
  }
  default_arguments = merge({
    "--TempDir"                          = var.temp_dir
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--aisles_path"                      = var.aisles_path
    "--departments_path"                 = var.departments_path
    "--products_path"                    = var.products_path
    "--orders_path"                      = var.orders_path
    "--order_products_prior_path"        = var.order_products_prior_path
    "--order_products_train_path"        = var.order_products_train_path
    "--aisles_out"                       = var.aisles_out
    "--departments_out"                  = var.departments_out
    "--products_out"                     = var.products_out
    "--orders_out"                       = var.orders_out
    "--order_products_prior_out"         = var.order_products_prior_out
    "--order_products_train_out"         = var.order_products_train_out
  }, var.extra_arguments)
  max_retries       = 1
  glue_version      = var.glue_version
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  tags              = var.tags

  connections = [aws_glue_connection.vpc.name]

}
