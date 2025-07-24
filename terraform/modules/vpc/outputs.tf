output "github_action_role_arn" {
  description = "The ARN of the GitHub Action IAM role"
  value       = aws_iam_role.github_action_role.arn
}

output "private_subnet_ids" {
  description = "List of private subnet IDs"
  value       = [aws_subnet.private.id]
}

output "postgres_security_group_id" {
  description = "ID of the PostgreSQL security group"
  value       = aws_security_group.postgres.id
}

output "lambda_sync_raw_security_group_id" {
  description = "ID of the Lambda security group for data sync raw"
  value       = aws_security_group.lambda.id
}

output "public_subnet_ids" {
  description = "List of public subnet IDs"
  value       = [aws_subnet.public.id]
}

output "bastion_security_group_id" {
  description = "ID of the Bastion security group"
  value       = aws_security_group.bastion.id
}
