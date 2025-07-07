# 2.2.2_batch_ETL_departments.tf
# Terraform 资源定义：ETL 清洗 departments 表

resource "aws_glue_job" "departments_etl" {
  name     = var.departments_job_name
  role_arn = var.departments_role_arn

  command {
    name            = "glueetl"
    script_location = var.departments_script_path
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"           = var.departments_temp_dir
    "--job-language"      = "python"
    "--input_table"       = var.departments_input_table
    "--output_path"       = var.departments_output_path
    "--historical_path"   = var.departments_output_path
  }

  max_retries       = 0
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10
}