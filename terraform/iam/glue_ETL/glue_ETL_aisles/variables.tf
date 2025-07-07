# terraform/iam/glue_ETL_orders/variables.tf

variable "role_name" {
  description = "Name of the IAM Role to be created for Glue ETL orders job"
  type        = string
}

variable "bucket_names" {
  description = "List of S3 bucket names the Glue job should access"
  type        = list(string)
}