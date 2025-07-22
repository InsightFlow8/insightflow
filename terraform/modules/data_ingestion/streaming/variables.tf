# Variables for streaming_data_ingestion module

variable "stream_name" {
  description = "Kinesis Data Stream name"
  type        = string
}

variable "publisher_function_name" {
  description = "Publisher Lambda function name"
  type        = string
}

variable "publisher_zip_path" {
  description = "Path to publisher Lambda deployment package"
  type        = string
}

variable "publisher_handler" {
  description = "Publisher Lambda handler"
  type        = string
}

variable "publisher_runtime" {
  description = "Publisher Lambda runtime"
  type        = string
  default     = "python3.13"
}

variable "publisher_memory_size" {
  description = "Publisher Lambda memory size"
  type        = number
  default     = 128
}

variable "publisher_timeout" {
  description = "Publisher Lambda timeout"
  type        = number
  default     = 60
}

variable "transformer_function_name" {
  description = "Transformer Lambda function name"
  type        = string
}

variable "transformer_zip_path" {
  description = "Path to transformer Lambda deployment package"
  type        = string
}

variable "transformer_handler" {
  description = "Transformer Lambda handler"
  type        = string
}

variable "transformer_runtime" {
  description = "Transformer Lambda runtime"
  type        = string
  default     = "python3.13"
}

variable "transformer_memory_size" {
  description = "Transformer Lambda memory size"
  type        = number
  default     = 128
}

variable "transformer_timeout" {
  description = "Transformer Lambda timeout"
  type        = number
  default     = 60
}

variable "firehose_name" {
  description = "Kinesis Firehose name"
  type        = string
}
variable "raw_bucket" {
  description = "Target S3 bucket for Firehose"
  type        = string
}