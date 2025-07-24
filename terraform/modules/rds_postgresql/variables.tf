variable "env" {
  description = "Environment name (e.g. dev, prod)"
  type        = string
}

variable "db_name" {
  description = "Database name"
  type        = string
}

variable "rds_db_username" {
  description = "Master username for PostgreSQL"
  type        = string
}

variable "rds_db_password" {
  description = "Master password for PostgreSQL"
  type        = string
  sensitive   = true
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for RDS subnet group"
  type        = list(string)
}

variable "postgres_security_group_ids" {
  description = "List of security group IDs for RDS PostgreSQL"
  type        = list(string)
}