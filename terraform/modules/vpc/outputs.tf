output "github_action_role_arn" {
  description = "The ARN of the GitHub Action IAM role"
  value       = aws_iam_role.github_action_role.arn
}
