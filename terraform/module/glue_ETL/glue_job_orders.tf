resource "aws_glue_job" "orders_etl" {
  name     = var.orders_job_name
  role_arn = var.orders_role_arn

  command {
    name            = "glueetl"
    script_location = var.orders_script_path
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  max_retries       = 0
  timeout           = 10

  default_arguments = {
    "--job-language"    = "python"
    "--TempDir"         = var.orders_temp_dir
    "--input_table"     = var.orders_input_table
    "--output_path"     = var.orders_output_path
    "--eval_set_filter" = var.orders_eval_set_filter
  }
}
