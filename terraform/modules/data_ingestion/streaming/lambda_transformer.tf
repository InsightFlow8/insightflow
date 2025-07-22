resource "aws_lambda_function" "streaming_data_transformer" {
  function_name = var.transformer_function_name
  filename      = var.transformer_zip_path
  handler       = var.transformer_handler
  runtime       = var.transformer_runtime
  memory_size   = var.transformer_memory_size
  timeout       = var.transformer_timeout
  role          = aws_iam_role.streaming_transformer_lambda_role.arn
}