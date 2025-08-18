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

# Input paths (from after-clean data)
variable "aisles_path" {
  description = "S3 input path for aisles after-clean table"
  type        = string
}

variable "departments_path" {
  description = "S3 input path for departments after-clean table"
  type        = string
}

variable "products_path" {
  description = "S3 input path for products after-clean table"
  type        = string
}

variable "orders_path" {
  description = "S3 input path for orders after-clean table"
  type        = string
}

variable "order_products_prior_path" {
  description = "S3 input path for order_products_prior after-clean table"
  type        = string
}

variable "order_products_train_path" {
  description = "S3 input path for order_products_train after-clean table"
  type        = string
}

# Output paths (for feature files)
variable "user_features_output" {
  description = "S3 output path for user_features"
  type        = string
}

variable "product_features_output" {
  description = "S3 output path for product_features"
  type        = string
}

variable "upi_features_output" {
  description = "S3 output path for upi_features"
  type        = string
}

variable "product_features_union_output" {
  description = "S3 output path for product_features_union"
  type        = string
}

variable "upi_features_union_output" {
  description = "S3 output path for upi_features_union"
  type        = string
}

variable "extra_arguments" {
  description = "Extra arguments for the Glue Job"
  type        = map(string)
  default     = {}
}

variable "glue_version" {
  description = "Glue version for the job"
  type        = string
  default     = "4.0"
}

variable "number_of_workers" {
  description = "Number of workers for the Glue Job"
  type        = number
  default     = 10
}

variable "worker_type" {
  description = "Worker type for the Glue Job"
  type        = string
  default     = "G.1X"
}

variable "tags" {
  description = "Tags for the resources"
  type        = map(string)
  default     = {}
}

variable "s3_clean_bucket" {
  description = "S3 clean bucket name for input and output data"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "glue_security_group_id" {
  description = "Glue security group ID"
  type        = string
}
