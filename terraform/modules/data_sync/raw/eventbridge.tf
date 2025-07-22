# resource of EventBridge Trigger to batch ingestion Lambda function

resource "aws_cloudwatch_event_rule" "scheduled_sync_raw" {
  name                = "lambda-s3-to-rds-schedule"
  description         = "Scheduled sync from S3 to RDS"
  schedule_expression = var.eventbridge_schedule
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.scheduled_sync_raw.name
  target_id = "lambda-s3-to-rds_raw"
  arn       = aws_lambda_function.s3_to_rds_raw.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_to_rds_raw.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduled_sync_raw.arn
}