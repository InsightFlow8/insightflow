# resource of EventBridge Trigger to batch ingestion Lambda function.

resource "aws_cloudwatch_event_rule" "batch_ingestion_trigger" {
  name                = var.eventbridge_rule_name
  description         = var.eventbridge_rule_description
  schedule_expression = var.eventbridge_schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.batch_ingestion_trigger.name
  target_id = "batch_ingestion_lambda"
  arn       = aws_lambda_function.batch_ingestion.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.batch_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.batch_ingestion_trigger.arn
}