variable "env" {
  description = "Environment name (e.g., dev, prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket containing transformation feature data"
  type        = string
}

variable "s3_transformation_data_prefix" {
  description = "S3 prefix for transformation feature data (e.g., after-transformation/features)"
  type        = string
  default     = "after-transformation/features"
}

variable "database_name" {
  description = "Name of the Glue catalog database (same as clean data catalog)"
  type        = string
}

variable "table_prefix" {
  description = "Prefix for all tables created by crawlers"
  type        = string
  default     = "features_"
}

variable "crawler_schedule" {
  description = "Schedule for running crawlers (cron expression)"
  type        = string
  default     = "cron(0 16 * * ? *)" # 每天下午4点运行，在 transformation job 完成后
}

variable "recrawl_behavior" {
  description = "爬取行为：暂时固定为 CRAWL_EVERYTHING（全量爬取）"
  type        = string
  default     = "CRAWL_EVERYTHING"
  validation {
    condition = contains([
      "CRAWL_EVERYTHING",
      "CRAWL_NEW_FOLDERS_ONLY"
    ], var.recrawl_behavior)
    error_message = "Recrawl behavior must be either CRAWL_EVERYTHING or CRAWL_NEW_FOLDERS_ONLY."
  }
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}
