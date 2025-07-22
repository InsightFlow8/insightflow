resource "aws_kinesis_stream" "dummy_streaming" {
  name             = var.stream_name
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
  retention_period = 24
}