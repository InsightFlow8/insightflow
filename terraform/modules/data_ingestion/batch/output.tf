output "batch_ingestion_lambda_function_name" {
  value = aws_lambda_function.batch_ingestion.function_name
}

output "batch_ingestion_lambda_function_arn" {
  value = aws_lambda_function.batch_ingestion.arn
}
