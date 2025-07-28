variable "job_name" {
  description = "Name of the Glue Job"
  type        = string
}

variable "iam_role_name" {
  description = "Name of the IAM Role for Glue Job"
  type        = string
}

variable "temp_dir" {
  description = "S3 temp directory for Glue Job"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket name for storing ETL scripts"
  type        = string
}

# Input S3 Paths
variable "aisles_path" {
  description = "S3 path to cleaned aisles parquet files"
  type        = string
}

variable "departments_path" {
  description = "S3 path to cleaned departments parquet files"
  type        = string
}

variable "products_path" {
  description = "S3 path to cleaned products parquet files"
  type        = string
}

variable "orders_path" {
  description = "S3 path to cleaned orders parquet files"
  type        = string
}

variable "order_products_prior_path" {
  description = "S3 path to cleaned order_products_prior parquet files"
  type        = string
}

variable "order_products_train_path" {
  description = "S3 path to cleaned order_products_train parquet files"
  type        = string
}

# Output S3 Path
variable "output_path" {
  description = "S3 path for combined table output"
  type        = string
}

# Glue Job Configuration
variable "glue_version" {
  description = "Glue version to use"
  type        = string
  default     = "4.0"
}

variable "number_of_workers" {
  description = "Number of workers for Glue Job"
  type        = number
  default     = 5
}

variable "worker_type" {
  description = "Worker type for Glue Job"
  type        = string
  default     = "G.1X"
}

variable "extra_arguments" {
  description = "Extra arguments for Glue Job"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
