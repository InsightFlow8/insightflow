resource "aws_lambda_function" "streaming_data_publisher" {
  function_name = var.publisher_function_name
  filename      = var.publisher_zip_path
  handler       = var.publisher_handler
  runtime       = var.publisher_runtime
  memory_size   = var.publisher_memory_size
  timeout       = var.publisher_timeout
  role          = aws_iam_role.streaming_data_publisher_lambda_role.arn

  environment {
    variables = {
      KINESIS_STREAM_NAME = aws_kinesis_stream.dummy_streaming.name
    }
  }
}