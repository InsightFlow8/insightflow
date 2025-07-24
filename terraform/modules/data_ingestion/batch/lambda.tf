# resource of lambda function of batch ingestion module.

resource "aws_lambda_function" "batch_ingestion" {
  function_name    = var.lambda_function_name
  filename         = var.lambda_zip_path
  handler          = var.lambda_handler
  source_code_hash = filebase64sha256(var.lambda_zip_path)
  runtime          = var.lambda_runtime
  role             = aws_iam_role.batch_ingestion_lambda_role.arn
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_size

  environment {
    variables = {
      SNOWFLAKE_USER      = var.snowflake_user
      SNOWFLAKE_PASSWORD  = var.snowflake_password
      SNOWFLAKE_ACCOUNT   = var.snowflake_account
      SNOWFLAKE_WAREHOUSE = var.snowflake_warehouse
      SNOWFLAKE_ROLE      = var.snowflake_role
      RAW_BUCKET          = var.raw_bucket
      CLEAN_BUCKET        = var.clean_bucket
    }
  }
}