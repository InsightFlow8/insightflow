# PostgreSQL RDS (Free Tier) in Private Subnet
# 仅创建数据库实例，安全组需允许bastion访问5432端口

resource "aws_db_subnet_group" "postgres" {
  name       = "${var.env}-postgres-subnet-group"
  subnet_ids = var.private_subnet_ids
  tags = {
    Name = "${var.env}-postgres-subnet-group"
  }
}

resource "aws_db_instance" "postgres" {
  identifier              = "${var.env}-postgres"
  engine                  = "postgres"
  engine_version          = "17.5"
  instance_class          = "db.t3.micro" # Free tier eligible
  allocated_storage       = 20
  max_allocated_storage   = 100
  storage_type            = "gp2"
  db_name                 = var.db_name
  username                = var.db_username
  password                = var.db_password
  db_subnet_group_name    = aws_db_subnet_group.postgres.name
  vpc_security_group_ids  = var.postgres_security_group_ids
  multi_az                = false
  publicly_accessible     = false
  skip_final_snapshot     = true
  deletion_protection     = false
  backup_retention_period = 0
  apply_immediately       = true
  tags = {
    Name = "${var.env}-postgres"
  }
}
