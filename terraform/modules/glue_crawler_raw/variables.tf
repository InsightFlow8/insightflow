variable "env" {
  description = "Environment name (e.g., dev, prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket containing raw data"
  type        = string
}

variable "s3_raw_data_prefix" {
  description = "S3 prefix for raw data (e.g., data/batch)"
  type        = string
  default     = "data/batch"
}

variable "database_name" {
  description = "Name of the Glue catalog database"
  type        = string
  default     = "imba_raw_data_catalog"
}

variable "table_prefix" {
  description = "Prefix for all tables created by crawlers"
  type        = string
  default     = "raw_"
}

variable "crawler_schedule" {
  description = "Schedule for running crawlers (cron expression)"
  type        = string
  default     = "cron(0 15 30 * ? *)" # 每天凌晨2点运行
}

variable "recrawl_behavior" {
  description = "爬取行为：CRAWL_EVERYTHING(全量) 或 CRAWL_NEW_FOLDERS_ONLY(增量)"
  type        = string
  default     = "CRAWL_NEW_FOLDERS_ONLY"
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
