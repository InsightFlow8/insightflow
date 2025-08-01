# EventBridge configuration to trigger Step Functions state machine

# EventBridge Rule
resource "aws_cloudwatch_event_rule" "step_functions_trigger" {
  name                = var.sf_eventbridge_rule_name
  description         = var.sf_eventbridge_rule_description
  schedule_expression = var.sf_eventbridge_schedule_expression

  tags = {
    Name        = var.sf_eventbridge_rule_name
    Environment = var.env
    Project     = "InsightFlow"
  }
}

# IAM Role for EventBridge to invoke Step Functions
resource "aws_iam_role" "eventbridge_step_functions_role" {
  name = "EventBridgeStepFunctionsRole-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "EventBridgeStepFunctionsRole-${var.env}"
    Environment = var.env
    Project     = "InsightFlow"
  }
}

# IAM Policy for EventBridge to invoke Step Functions
resource "aws_iam_policy" "eventbridge_step_functions_policy" {
  name        = "EventBridgeStepFunctionsPolicy-${var.env}"
  description = "Policy for EventBridge to invoke Step Functions state machine"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = [
          aws_sfn_state_machine.data_pipeline.arn
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "eventbridge_step_functions_policy_attach" {
  role       = aws_iam_role.eventbridge_step_functions_role.name
  policy_arn = aws_iam_policy.eventbridge_step_functions_policy.arn
}

# EventBridge Target to invoke Step Functions
resource "aws_cloudwatch_event_target" "step_functions_target" {
  rule      = aws_cloudwatch_event_rule.step_functions_trigger.name
  target_id = "StepFunctionsTarget"
  arn       = aws_sfn_state_machine.data_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_step_functions_role.arn

  input_transformer {
    input_paths = {}
    input_template = jsonencode({
      "executionName" = "scheduled-execution-$${aws:events:rule-name}-$${aws:events:event-id}"
      "timestamp"     = "$${aws:events:event-time}"
    })
  }
}

