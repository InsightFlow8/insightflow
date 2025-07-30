variable "env" {
  description = "Environment"
  type        = string
}

variable "tags" {
  description = "Tags"
  type        = map(string)
  default     = {}
}

variable "s3_raw_bucket_name" {
  type = string
}

variable "s3_raw_data_prefix" {
  type    = string
  default = "data/batch"
}

variable "raw_database_name" {
  type = string
}

variable "raw_table_prefix" {
  description = "Prefix for all tables created by crawlers"
  type        = string
  default     = "raw_"
}
