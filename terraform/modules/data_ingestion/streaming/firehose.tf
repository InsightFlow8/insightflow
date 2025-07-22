resource "aws_kinesis_firehose_delivery_stream" "streaming_firehose" {
  name        = var.firehose_name
  # 必须是extended_s3，Gemini查询得到，GPT无法给到正确回答
  destination = "extended_s3"
  
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.dummy_streaming.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  # 必须是extended_s3，Gemini查询得到，GPT无法给到正确回答
  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = "arn:aws:s3:::${var.raw_bucket}"
    prefix             = "data/streaming/orders/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hhmm=!{timestamp:HHmm}/orders_"
    error_output_prefix = "data/streaming/orders_error/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    compression_format = "UNCOMPRESSED"
    file_extension      = ".csv"

    processing_configuration {
      enabled = true
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = aws_lambda_function.streaming_data_transformer.arn
        }
      }
    }
  }
}