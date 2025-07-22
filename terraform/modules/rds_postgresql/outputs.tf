output "rds_instance_id" {
  description = "ID of the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres.id
}

# AWS RDS 的 endpoint 属性在较新版本的 AWS Provider 中的行为变化，现在会返回带端口号的完整连接字符串（主机:端口），这是 Terraform AWS Provider 的默认行为。但会导致.sh初始化登录数据库时报错。因此需要拆分！
output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint (with port)"
  value       = aws_db_instance.postgres.endpoint
}

# 拆分，仅保留出端口号的部分。
output "rds_host" {
  description = "RDS PostgreSQL host (without port)"
  value       = split(":", aws_db_instance.postgres.endpoint)[0]
}

output "rds_port" {
  description = "RDS PostgreSQL port"
  value       = aws_db_instance.postgres.port
}

output "rds_db_name" {
  description = "RDS PostgreSQL database name"
  value       = aws_db_instance.postgres.db_name
}