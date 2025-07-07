# terraform/iam/glue_ETL/glue_ETL_departments/variables.tf

variable "role_name" {
  description = "Name of the IAM Role to be created for Glue ETL departments job"
  type        = string
}

variable "bucket_names" {
  description = "List of S3 bucket names the Glue job should access"
  type        = list(string)
}