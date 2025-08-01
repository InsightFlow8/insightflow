output "step_functions_state_machine_arn" {
  value = aws_sfn_state_machine.data_pipeline.arn
}

output "step_functions_state_machine_name" {
  value = aws_sfn_state_machine.data_pipeline.name
}

# EventBridge Outputs
output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.step_functions_trigger.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.step_functions_trigger.name
}

output "eventbridge_role_arn" {
  description = "ARN of the EventBridge IAM role"
  value       = aws_iam_role.eventbridge_step_functions_role.arn
}
