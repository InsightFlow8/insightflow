variable "job_name" {
  description = "Name of the Glue Job"
  type        = string
}

variable "iam_role_name" {
  description = "Name of the IAM Role for Glue Job"
  type        = string
}

variable "script_location" {
  description = "S3 path to the ETL PySpark script"
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

variable "aisles_path" {
  description = "S3 input path for aisles table"
  type        = string
}

variable "departments_path" {
  description = "S3 input path for departments table"
  type        = string
}

variable "products_path" {
  description = "S3 input path for products table"
  type        = string
}

variable "orders_path" {
  description = "S3 input path for orders table"
  type        = string
}

variable "order_products_prior_path" {
  description = "S3 input path for order_products_prior table"
  type        = string
}

variable "order_products_train_path" {
  description = "S3 input path for order_products_train table"
  type        = string
}

variable "aisles_out" {
  description = "S3 output path for cleaned aisles table"
  type        = string
}

variable "departments_out" {
  description = "S3 output path for cleaned departments table"
  type        = string
}

variable "products_out" {
  description = "S3 output path for cleaned products table"
  type        = string
}

variable "orders_out" {
  description = "S3 output path for cleaned orders table"
  type        = string
}

variable "order_products_prior_out" {
  description = "S3 output path for cleaned order_products_prior table"
  type        = string
}

variable "order_products_train_out" {
  description = "S3 output path for cleaned order_products_train table"
  type        = string
}

variable "extra_arguments" {
  description = "Extra default arguments for Glue Job"
  type        = map(string)
  default     = {}
}

variable "glue_version" {
  description = "Glue version (e.g., 3.0, 4.0)"
  type        = string
  default     = "4.0"
}

variable "number_of_workers" {
  description = "Number of workers for Glue Job"
  type        = number
  default     = 2
}

variable "worker_type" {
  description = "Worker type (Standard, G.1X, G.2X, G.025X)"
  type        = string
  default     = "G.1X"
}

variable "tags" {
  description = "Tags for Glue Job"
  type        = map(string)
  default     = {}
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Glue job VPC config"
  type        = list(string)
}

variable "glue_security_group_id" {
  description = "Security group ID for Glue job VPC config"
  type        = string
}
