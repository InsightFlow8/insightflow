# 2.2.3_batch_ETL_aisles.tf
# Terraform 资源定义：ETL 清洗 aisles 表

resource "aws_glue_job" "aisles_etl" {
  name     = var.aisles_job_name
  role_arn = var.aisles_role_arn

  command {
    name            = "glueetl"
    script_location = var.aisles_script_path
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"           = var.aisles_temp_dir
    "--job-language"      = "python"
    "--input_table"       = var.aisles_input_table
    "--output_path"       = var.aisles_output_path
    "--historical_path"   = var.aisles_output_path
  }

  max_retries       = 0
  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 10
}
