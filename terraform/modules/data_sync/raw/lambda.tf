resource "aws_lambda_function" "s3_to_rds_raw" {
  function_name = "s3-to-rds-sync-raw"
  filename      = var.lambda_zip_path
  handler       = "lambda_s3_to_rds_raw.lambda_handler"
  runtime       = "python3.13"
  role          = aws_iam_role.lambda_exec.arn

  environment {
    variables = {
      S3_BUCKET       = var.s3_bucket_name
      RDS_HOST        = var.rds_host
      RDS_PORT        = var.rds_port
      RDS_DB          = var.rds_db
      RDS_USER        = var.rds_user
      RDS_PASSWORD    = var.rds_password
      TABLE_NAME      = jsonencode(var.table_name)
      SCHEMA_NAME     = var.schema_name
      BATCH_SIZE      = var.batch_size
      S3_KEY_PREFIX   = jsonencode(var.s3_key_prefix)
      START_TIMESTAMP = var.start_ts
      END_TIMESTAMP   = var.end_ts
    }
  }

  timeout = 900
  memory_size = 2048

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [var.lambda_security_group_id]
  }
}
### S3 bucket trigger is commented out for now, can be enabled later
# resource "aws_lambda_permission" "allow_s3" {
#   statement_id  = "AllowExecutionFromS3"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.s3_to_rds.function_name
#   principal     = "s3.amazonaws.com"
#   source_arn    = var.s3_bucket_arn
# }

# resource "aws_lambda_permission" "allow_eventbridge" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.s3_to_rds_raw.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.scheduled_sync.arn
# }

# resource "aws_cloudwatch_event_rule" "scheduled_sync" {
#   name        = "lambda-s3-to-rds-schedule"
#   description = "Scheduled sync from S3 to RDS"
#   schedule_expression = var.eventbridge_schedule
# }

# resource "aws_cloudwatch_event_target" "lambda_target" {
#   rule      = aws_cloudwatch_event_rule.scheduled_sync.name
#   target_id = "lambda-s3-to-rds"
#   arn       = aws_lambda_function.s3_to_rds.arn
# }

// ...existing code...
