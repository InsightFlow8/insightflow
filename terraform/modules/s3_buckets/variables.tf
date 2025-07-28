variable "raw_bucket" {
  description = "Name of the raw S3 bucket"
  type        = string
}

variable "clean_bucket" {
  description = "Name of the clean S3 bucket"
  type        = string
}

variable "curated_bucket" {
  description = "Name of the curated S3 bucket"
  type        = string
}

variable "scripts_bucket" {
  description = "Name of the S3 bucket for scripts and assets"
  type        = string
  default     = "insightflow-dev-scripts"
}

variable "bucket_names" {
  description = "List of four S3 bucket names"
  type        = list(string)
  validation {
    condition     = length(var.bucket_names) == 4
    error_message = "You must provide exactly four bucket names."
  }
}
