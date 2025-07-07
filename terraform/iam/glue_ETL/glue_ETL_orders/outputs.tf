# terraform/iam/glue_ETL_orders/outputs.tf

output "glue_role_orders_arn" {
  description = "ARN of the IAM Role for Glue ETL orders job"
  value       = aws_iam_role.glue_role_orders.arn
}
