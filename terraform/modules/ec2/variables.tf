variable "env" {
  description = "Environment name (e.g. dev, prod)"
  type        = string
}

variable "ami_id" {}
variable "instance_type" {}
variable "key_name" {}
# variable "vpc_id" {}
variable "public_subnet_id" {}
# variable "private_subnet_id" {}

variable "s3_bucket" {
  type    = string
  default = "source-bucket-chien-as"
}

variable "region" {
  type = string
}

# ================================
# RDS PostgreSQL
# ================================

# variable "rds_endpoint" {
#   description = "PostgreSQL RDS endpoint(with port number)"
#   type = string
# }

# variable "rds_host" {
#   description = "PostgreSQL RDS endpoint(without port number)"
#   type = string
# }

# variable "db_name" {
#   description = "Database name"
#   type        = string
# }

# variable "db_username" {
#   description = "Master username for PostgreSQL"
#   type        = string
# }

# variable "db_password" {
#   description = "Master password for PostgreSQL"
#   type        = string
#   sensitive   = true
# }

# variable "rds_port" {
#   description = "RDS port for PostgreSQL"
#   type        = number
#   sensitive   = true
# }

# variable "sql_s3_path" {
#   description = "S3 path for create_tables.sql"
#   type        = string
# }


variable "bastion_security_group_ids" {}

# variable "postgres_security_group_ids" {}
