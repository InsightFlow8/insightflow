# terraform/module/glue_ETL/glue_job_products.tf
resource "aws_glue_job" "products_etl" {
  name     = var.products_job_name
  role_arn = var.products_role_arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = var.products_script_path
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = var.products_temp_dir
    "--input_table"  = var.products_input_table
    "--output_path"  = var.products_output_path
  }

  max_retries       = 0
  timeout           = 10
  number_of_workers = 2
  worker_type       = "G.1X"
  glue_version      = "4.0"
}
