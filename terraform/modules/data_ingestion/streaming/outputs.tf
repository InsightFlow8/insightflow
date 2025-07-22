output "kinesis_stream_name" {
  value = aws_kinesis_stream.dummy_streaming.name
}
output "lambda_function_name" {
  value = aws_lambda_function.streaming_data_publisher.function_name
}